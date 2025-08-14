//! Acropolis DRep State module for Caryatid
//! Accepts certificate events and derives the DRep State in memory

use acropolis_common::{
    messages::{CardanoMessage, DRepStateMessage, Message, StateQuery, StateQueryResponse},
    queries::governance::{
        DRepDelegatorAddresses, DRepInfo, DRepInfoWithDelegators, DRepMetadata, DRepUpdates,
        DRepVotes, DRepsList, GovernanceStateQuery, GovernanceStateQueryResponse,
    },
    state_history::StateHistory,
    BlockInfo, BlockStatus,
};
use anyhow::anyhow;
use anyhow::Result;
use caryatid_sdk::{module, Context, Module, Subscription};
use config::Config;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Mutex},
    time::timeout,
};
use tracing::{debug, error, info, info_span, Instrument};
mod state;
use crate::state::DRepStorageConfig;
use state::State;

const DEFAULT_CERTIFICATES_SUBSCRIBE_TOPIC: (&str, &str) =
    ("certificates-subscribe-topic", "cardano.certificates");
const DEFAULT_GOVERNANCE_SUBSCRIBE_TOPIC: (&str, &str) =
    ("governance-subscribe-topic", "cardano.governance");
const DEFAULT_PARAMETERS_SUBSCRIBE_TOPIC: (&str, &str) =
    ("parameters-subscribe-topic", "cardano.protocol.parameters");
const DEFAULT_DREP_STATE_TOPIC: (&str, &str) = ("publish-drep-state-topic", "cardano.drep.state");

const DEFAULT_STORE_INFO: (&str, bool) = ("store-info", false);
const DEFAULT_STORE_DELEGATORS: (&str, bool) = ("store-delegators", false);
const DEFAULT_STORE_METADATA: (&str, bool) = ("store-metadata", false);
const DEFAULT_STORE_UPDATES: (&str, bool) = ("store-updates", false);
const DEFAULT_STORE_VOTES: (&str, bool) = ("store-votes", false);

const DEFAULT_DREPS_QUERY_TOPIC: (&str, &str) = ("dreps-state-query-topic", "cardano.query.dreps");

/// DRep State module
#[module(
    message_type(Message),
    name = "drep-state",
    description = "In-memory DRep State from certificate events"
)]

pub struct DRepState;

struct Pair {
    bi: BlockInfo,
    certs: Option<acropolis_common::messages::TxCertificatesMessage>,
    gov: Option<acropolis_common::messages::GovernanceProceduresMessage>,
}

impl DRepState {
    pub async fn init(&self, context: Arc<Context<Message>>, config: Arc<Config>) -> Result<()> {
        fn get_flag(config: &Config, key: (&str, bool)) -> bool {
            config.get_bool(key.0).unwrap_or(key.1)
        }

        fn get_string(config: &Config, key: (&str, &str)) -> String {
            config.get_string(key.0).unwrap_or_else(|_| key.1.to_string())
        }

        // Get configuration
        let certificates_subscribe_topic =
            get_string(&config, DEFAULT_CERTIFICATES_SUBSCRIBE_TOPIC);
        info!("Creating subscriber on '{certificates_subscribe_topic}'");

        let drep_state_topic = get_string(&config, DEFAULT_DREP_STATE_TOPIC);
        info!("Creating DRep state publisher on '{drep_state_topic}'");

        let drep_query_topic = get_string(&config, DEFAULT_DREPS_QUERY_TOPIC);
        info!("Creating DRep query publisher on '{drep_query_topic}'");

        let storage_config = DRepStorageConfig {
            store_info: get_flag(&config, DEFAULT_STORE_INFO),
            store_delegators: get_flag(&config, DEFAULT_STORE_DELEGATORS),
            store_metadata: get_flag(&config, DEFAULT_STORE_METADATA),
            store_updates: get_flag(&config, DEFAULT_STORE_UPDATES),
            store_votes: get_flag(&config, DEFAULT_STORE_VOTES),
        };

        // Optional subscribe topics
        let mut governance_subscribe_topic = String::new();
        if storage_config.store_votes {
            governance_subscribe_topic = get_string(&config, DEFAULT_GOVERNANCE_SUBSCRIBE_TOPIC);
            info!("Creating subscriber on '{governance_subscribe_topic}'");
        }
        let mut parameters_subscribe_topic = String::new();
        if storage_config.store_info {
            parameters_subscribe_topic = get_string(&config, DEFAULT_PARAMETERS_SUBSCRIBE_TOPIC);
            info!("Creating subscriber on '{parameters_subscribe_topic}'");
        }

        let history = Arc::new(Mutex::new(StateHistory::<State>::new("DRepState")));

        // Subscriptions
        let certs_subscription: Box<dyn Subscription<Message> + Send> =
            context.subscribe(&certificates_subscribe_topic).await?;

        let mut gov_subscription: Option<Box<dyn Subscription<Message> + Send>> =
            if storage_config.store_votes {
                Some(context.subscribe(&governance_subscribe_topic).await?)
            } else {
                None
            };

        let mut parameters_subscription: Option<Box<dyn Subscription<Message> + Send>> =
            if storage_config.store_info || storage_config.store_votes {
                Some(context.subscribe(&parameters_subscribe_topic).await?)
            } else {
                None
            };

        // Main loop of synchronised messages
        let context_subscribe = context.clone();
        let context_handler = context.clone();
        let history_handler = history.clone();
        let drep_state_topic = drep_state_topic.clone();
        let init_cfg = storage_config;

        // Subscribe to certificates and governance messages in separate tasks
        // This keeps bus reads running at full speed and prevents one stream
        // from blocking the other. Messages are forwarded into tx_certs and tx_gov channels.
        let (mut rx_certs, mut rx_gov) =
            spawn_subscription_forwarders(certs_subscription, gov_subscription.take());

        context.run(async move {
            // Conway epoch start, set when Conway params are seen.
            // Used to determine when to start processing votes.
            let mut conway_epoch_start: Option<u64> = None;

            // Pending blocks to process. Once conway_epoch_start is set,
            // we wait for matching cert and gov messages before processing a block.
            let mut pending: BTreeMap<u64, Pair> = BTreeMap::new();

            // Last committed block number used to avoid reprocessing.
            let mut last_committed: u64 = 0;
            loop {
                // Drain certificate and governance messages from forwarder to pending
                // If no new messages, we block until new messages arrive.
                let new_messages = drain_cert_messages(
                    &mut rx_certs,
                    &mut pending,
                    &history_handler,
                    &mut last_committed,
                    &mut conway_epoch_start,
                )
                .await
                    || drain_gov_messages(
                        &mut rx_gov,
                        &mut pending,
                        &history_handler,
                        &mut last_committed,
                        &mut conway_epoch_start,
                    )
                    .await;

                // If no messages were drained, wait for new messages
                if !new_messages {
                    wait_for_new_message(
                        &mut pending,
                        &mut rx_certs,
                        &mut rx_gov,
                        &init_cfg,
                        &conway_epoch_start,
                    )
                    .await;
                    continue;
                }

                // Process blocks that have both certs and gov messages ready sequentially by block number
                process_ready_blocks(
                    &mut pending,
                    &init_cfg,
                    &mut conway_epoch_start,
                    &mut last_committed,
                    &history_handler,
                    &mut parameters_subscription,
                    &context_handler,
                    &context_subscribe,
                    &drep_state_topic,
                )
                .await;
            }
        });

        let query_state = history.clone();
        context.handle(&drep_query_topic, move |message| {
            let state_handle = query_state.clone();
            async move {
                let Message::StateQuery(StateQuery::Governance(query)) = message.as_ref() else {
                    return Arc::new(Message::StateQueryResponse(StateQueryResponse::Governance(
                        GovernanceStateQueryResponse::Error(
                            "Invalid message for governance-state".into(),
                        ),
                    )));
                };

                let locked = state_handle.lock().await;

                let response = match query {
                    GovernanceStateQuery::GetDRepsList => match locked.current() {
                        Some(state) => {
                            let dreps = state.list();
                            GovernanceStateQueryResponse::DRepsList(DRepsList { dreps })
                        }
                        None => GovernanceStateQueryResponse::Error("No current DRep state".into()),
                    },
                    GovernanceStateQuery::GetDRepInfoWithDelegators { drep_credential } => {
                        match locked.current() {
                            Some(state) => match state.get_drep_info(&drep_credential) {
                                Ok(Some(info)) => match state.get_drep_delegators(&drep_credential)
                                {
                                    Ok(Some(delegators)) => {
                                        let response = DRepInfoWithDelegators {
                                            info: DRepInfo {
                                                deposit: info.deposit,
                                                retired: info.retired,
                                                expired: info.expired,
                                                active_epoch: info.active_epoch,
                                                last_active_epoch: info.last_active_epoch,
                                            },
                                            delegators: delegators.to_vec(),
                                        };

                                        GovernanceStateQueryResponse::DRepInfoWithDelegators(
                                            response,
                                        )
                                    }

                                    Ok(None) => GovernanceStateQueryResponse::NotFound,
                                    Err(msg) => {
                                        GovernanceStateQueryResponse::Error(msg.to_string())
                                    }
                                },

                                Ok(None) => GovernanceStateQueryResponse::NotFound,
                                Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                            },
                            None => {
                                GovernanceStateQueryResponse::Error("No current state".to_string())
                            }
                        }
                    }
                    GovernanceStateQuery::GetDRepDelegators { drep_credential } => {
                        match locked.current() {
                            Some(state) => match state.get_drep_delegators(&drep_credential) {
                                Ok(Some(delegators)) => {
                                    GovernanceStateQueryResponse::DRepDelegators(
                                        DRepDelegatorAddresses {
                                            addresses: delegators.clone(),
                                        },
                                    )
                                }
                                Ok(None) => GovernanceStateQueryResponse::NotFound,
                                Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                            },
                            None => {
                                GovernanceStateQueryResponse::Error("No current state".to_string())
                            }
                        }
                    }
                    GovernanceStateQuery::GetDRepMetadata { drep_credential } => {
                        match locked.current() {
                            Some(state) => match state.get_drep_anchor(&drep_credential) {
                                Ok(Some(anchor)) => {
                                    GovernanceStateQueryResponse::DRepMetadata(DRepMetadata {
                                        anchor: Some(anchor.clone()),
                                    })
                                }
                                Ok(None) => GovernanceStateQueryResponse::NotFound,
                                Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                            },
                            None => {
                                GovernanceStateQueryResponse::Error("No current state".to_string())
                            }
                        }
                    }
                    GovernanceStateQuery::GetDRepUpdates { drep_credential } => {
                        match locked.current() {
                            Some(state) => match state.get_drep_updates(&drep_credential) {
                                Ok(Some(updates)) => {
                                    GovernanceStateQueryResponse::DRepUpdates(DRepUpdates {
                                        updates: updates.to_vec(),
                                    })
                                }
                                Ok(None) => GovernanceStateQueryResponse::NotFound,
                                Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                            },
                            None => {
                                GovernanceStateQueryResponse::Error("No current state".to_string())
                            }
                        }
                    }
                    GovernanceStateQuery::GetDRepVotes { drep_credential } => {
                        match locked.current() {
                            Some(state) => match state.get_drep_votes(&drep_credential) {
                                Ok(Some(votes)) => {
                                    GovernanceStateQueryResponse::DRepVotes(DRepVotes {
                                        votes: votes.to_vec(),
                                    })
                                }
                                Ok(None) => GovernanceStateQueryResponse::NotFound,
                                Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                            },
                            None => {
                                GovernanceStateQueryResponse::Error("No current state".to_string())
                            }
                        }
                    }
                    _ => GovernanceStateQueryResponse::Error(format!(
                        "Unimplemented governance query: {query:?}"
                    )),
                };
                Arc::new(Message::StateQueryResponse(StateQueryResponse::Governance(
                    response,
                )))
            }
        });

        // Ticker to log stats
        let mut subscription = context.subscribe("clock.tick").await?;
        let history_ticker = history.clone();
        context.run(async move {
            loop {
                let Ok((_, message)) = subscription.read().await else {
                    return;
                };
                if let Message::Clock(message) = message.as_ref() {
                    if (message.number % 60) == 0 {
                        let span = info_span!("drep_state.tick", number = message.number);
                        async {
                            let snapshot = {
                                let mut h = history_ticker.lock().await;
                                h.get_current_state()
                            };

                            snapshot.tick().inspect_err(|e| error!("Tick error: {e}")).ok();
                        }
                        .instrument(span)
                        .await;
                    }
                }
            }
        });

        Ok(())
    }
}

fn spawn_subscription_forwarders(
    mut certs_subscription: Box<dyn Subscription<Message> + Send>,
    gov_subscription: Option<Box<dyn Subscription<Message> + Send>>,
) -> (
    mpsc::UnboundedReceiver<Arc<Message>>,
    mpsc::UnboundedReceiver<Arc<Message>>,
) {
    let (tx_certs, rx_certs) = mpsc::unbounded_channel();
    let (tx_gov, rx_gov) = mpsc::unbounded_channel();

    // Certificates forwarder
    tokio::spawn(async move {
        while let Ok((_, msg)) = certs_subscription.read().await {
            if tx_certs.send(msg).is_err() {
                break;
            }
        }
    });

    // Governance forwarder
    if let Some(mut sub) = gov_subscription {
        tokio::spawn(async move {
            while let Ok((_, msg)) = sub.read().await {
                let _ = tx_gov.send(msg);
            }
        });
    }

    (rx_certs, rx_gov)
}

async fn drain_cert_messages(
    rx_certs: &mut mpsc::UnboundedReceiver<Arc<Message>>,
    pending: &mut BTreeMap<u64, Pair>,
    history_handler: &Arc<Mutex<StateHistory<State>>>,
    last_committed: &mut u64,
    conway_epoch_start: &mut Option<u64>,
) -> bool {
    let mut progressed = false;

    while let Ok(msg) = rx_certs.try_recv() {
        progressed = true;
        if let Message::Cardano((bi, CardanoMessage::TxCertificates(certs))) = msg.as_ref() {
            // Ensure we don't reprocess blocks
            if bi.number <= *last_committed {
                continue;
            }

            // Check for roll back
            if bi.status == BlockStatus::RolledBack {
                // Get rolled back state
                let new_state = {
                    let mut h = history_handler.lock().await;
                    h.get_rolled_back_state(bi)
                };

                // Clear pending >= this block
                pending.split_off(&bi.number);

                // Commit rolled back state
                {
                    let mut h = history_handler.lock().await;
                    h.commit(bi, new_state);
                }

                // Check if rollback occured over conway epoch start, rollback flag to None.
                if let Some(start) = *conway_epoch_start {
                    if bi.epoch <= start {
                        *conway_epoch_start = None;
                    }
                }
            } else {
                // No rollback, store certificate message in pending
                pending
                    .entry(bi.number)
                    .or_insert_with(|| Pair {
                        bi: bi.clone(),
                        certs: None,
                        gov: None,
                    })
                    .certs = Some(certs.clone());
            }
        }
    }

    progressed
}

async fn drain_gov_messages(
    rx_gov: &mut mpsc::UnboundedReceiver<Arc<Message>>,
    pending: &mut BTreeMap<u64, Pair>,
    history_handler: &Arc<Mutex<StateHistory<State>>>,
    last_committed: &mut u64,
    conway_epoch_start: &mut Option<u64>,
) -> bool {
    let mut progressed = false;

    while let Ok(msg) = rx_gov.try_recv() {
        progressed = true;
        if let Message::Cardano((bi, CardanoMessage::GovernanceProcedures(gp))) = msg.as_ref() {
            // Ensure we don't reprocess blocks
            if bi.number <= *last_committed {
                continue;
            }

            // Check for roll back
            if bi.status == BlockStatus::RolledBack {
                // Get rolled back state
                let new_state = {
                    let mut h = history_handler.lock().await;
                    h.get_rolled_back_state(bi)
                };

                // Clear pending >= this block
                pending.split_off(&bi.number);

                // Commit rolled back state
                {
                    let mut h = history_handler.lock().await;
                    h.commit(bi, new_state);
                }

                // Rollback last committed block
                *last_committed = bi.number;

                // Check if rollback occured over conway epoch start, rollback flag to None.
                if let Some(start) = *conway_epoch_start {
                    if bi.epoch <= start {
                        *conway_epoch_start = None;
                    }
                }
            } else {
                // No rollback, store governance message in pending
                pending
                    .entry(bi.number)
                    .or_insert_with(|| Pair {
                        bi: bi.clone(),
                        certs: None,
                        gov: None,
                    })
                    .gov = Some(gp.clone());
            }
        }
    }

    progressed
}

async fn wait_for_new_message(
    pending: &mut BTreeMap<u64, Pair>,
    rx_certs: &mut mpsc::UnboundedReceiver<Arc<Message>>,
    rx_gov: &mut mpsc::UnboundedReceiver<Arc<Message>>,
    init_cfg: &DRepStorageConfig,
    conway_epoch_start: &Option<u64>,
) {
    // Only listen for gov messages if store-votes is enabled, Conway era has started,
    // and there are pending cert messages awaiting their gov partner.
    let wait_for_gov = init_cfg.store_votes
        && pending.values().any(|p| {
            p.certs.is_some()
                && p.gov.is_none()
                && conway_epoch_start.map_or(false, |start| p.bi.epoch >= start)
        });

    // Wait for new messages to avoid busy waiting
    tokio::select! {
        // Always receive and store certificate messages
        Some(msg) = rx_certs.recv() => {
            if let Message::Cardano((bi, CardanoMessage::TxCertificates(certs))) = msg.as_ref() {
                pending.entry(bi.number)
                    .or_insert_with(|| Pair { bi: bi.clone(), certs: None, gov: None })
                    .certs = Some(certs.clone());
            }
        }

        // Only poll gov when a block is waiting for its gov partner.
        Some(msg) = rx_gov.recv(), if wait_for_gov => {
            if let Message::Cardano((bi, CardanoMessage::GovernanceProcedures(gp))) = msg.as_ref() {
                pending.entry(bi.number)
                    .or_insert_with(|| Pair { bi: bi.clone(), certs: None, gov: None })
                    .gov = Some(gp.clone());
            }
        }
    }
}

async fn process_ready_blocks(
    pending: &mut BTreeMap<u64, Pair>,
    init_cfg: &DRepStorageConfig,
    conway_epoch_start: &mut Option<u64>,
    last_committed: &mut u64,
    history_handler: &Arc<Mutex<StateHistory<State>>>,
    parameters_subscription: &mut Option<Box<dyn Subscription<Message> + Send>>,
    context_handler: &Arc<Context<Message>>,
    context_subscribe: &Arc<Context<Message>>,
    drep_state_topic: &str,
) {
    // Process blocks that have both certs and gov messages ready sequentially by block number
    while let Some((&k, p)) = pending.iter().next() {
        // Check if governance certs are needed based on store-votes config and if conway has started.
        let need_gov =
            init_cfg.store_votes && conway_epoch_start.map_or(false, |start| p.bi.epoch >= start);

        // Check if both cert and gov messages are ready for this block
        let ready = p.certs.is_some() && (!need_gov || p.gov.is_some());

        // If the oldest block is not ready, exit loop
        if !ready {
            break;
        }

        // Remove the block from pending for processing
        let pair = pending.remove(&k).expect("exists");
        let block_info = pair.bi.clone();

        // Get the current state or initialize with config
        let mut state = {
            let mut h = history_handler.lock().await;
            h.get_or_init_with(|| State::new(init_cfg.clone()))
        };

        // Check if we are at an epoch boundary
        let new_epoch = block_info.new_epoch && block_info.epoch > 0;
        if new_epoch {
            // At epoch boundary, read parameters to check for Conway activation and retrieve
            // DRep expiration parameter.
            if let Some(sub) = parameters_subscription.as_mut() {
                if let Ok(Ok((pblk, d_rep_activity))) =
                    timeout(Duration::from_millis(50), read_parameters(sub)).await
                {
                    // Set conway_block_start if epoch transition to Conway is detected
                    if conway_epoch_start.is_none() {
                        *conway_epoch_start = Some(block_info.epoch);
                        info!("Conway activated at epoch {}", block_info.epoch);
                    }

                    // Ensure parameters are for the correct block
                    if pblk.number != block_info.number {
                        error!(
                            "Params out of sync: certs {} vs params {}",
                            block_info.number, pblk.number
                        );
                    }

                    // Update DRep expirations based on DRep expiration parameter
                    if let Err(err) =
                        state.update_drep_expirations(block_info.epoch, d_rep_activity)
                    {
                        error!("Failed to update DRep expirations: {err}");
                    }
                } else {
                    debug!(
                        "No params at epoch boundary {}, proceeding with previous era",
                        block_info.epoch
                    );
                }
            }

            // Publish DRep state at epoch boundary
            let dreps = {
                let mut h = history_handler.lock().await;
                h.get_current_state().active_drep_list()
            };
            let out = Message::Cardano((
                block_info.clone(),
                CardanoMessage::DRepState(DRepStateMessage {
                    epoch: block_info.epoch,
                    dreps,
                }),
            ));
            if let Err(e) = context_subscribe.publish(&drep_state_topic, Arc::new(out)).await {
                error!("Failed to publish DRep state: {e}");
            }
        }

        // Process governance procedures if store-votes is enabled
        if let Some(gp) = pair.gov.as_ref() {
            if let Err(e) = state.process_votes(gp).await {
                error!("Failed to handle governance procedures: {e}");
            }
        }

        // Process certificates
        if let Some(ref certs_msg) = pair.certs {
            if let Err(e) = state.process_certificates(context_handler.clone(), certs_msg).await {
                error!("Certificates handling error: {e}");
            }
        }

        // Commit the updated state
        {
            let mut h = history_handler.lock().await;
            h.commit(&block_info, state);
        }

        // Update the last committed block number
        *last_committed = block_info.number;
    }
}

pub async fn read_parameters(
    sub: &mut Box<dyn Subscription<Message> + Send>,
) -> Result<(BlockInfo, u32)> {
    match sub.read().await?.1.as_ref() {
        Message::Cardano((blk, CardanoMessage::ProtocolParams(params))) => {
            if let Some(conway) = &params.params.conway {
                Ok((blk.clone(), conway.d_rep_activity))
            } else {
                Err(anyhow!("ProtocolParams without Conway section"))
            }
        }
        other => Err(anyhow!("Unexpected message on parameters topic: {other:?}")),
    }
}
