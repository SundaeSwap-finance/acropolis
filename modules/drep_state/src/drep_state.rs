//! Acropolis DRep State module for Caryatid
//! Accepts certificate events and derives the DRep State in memory

use acropolis_common::{
    messages::{CardanoMessage, DRepStateMessage, Message, StateQuery, StateQueryResponse},
    queries::governance::{
        DRepDelegatorAddresses, DRepInfo, DRepInfoWithDelegators, DRepMetadata, DRepUpdates,
        DRepVotes, DRepsList, GovernanceStateQuery, GovernanceStateQueryResponse,
    },
    state_history::StateHistory,
    BlockStatus,
};
use anyhow::Result;
use caryatid_sdk::{module, Context, Module};
use config::Config;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, info_span, Instrument};

mod state;
use state::State;

use crate::state::DRepStorageConfig;

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
        let mut certs_subscription = context.subscribe(&certificates_subscribe_topic).await?;
        let mut votes_subscription = if storage_config.store_votes {
            Some(context.subscribe(&governance_subscribe_topic).await?)
        } else {
            None
        };
        let mut parameters_subscription = if storage_config.store_info {
            Some(context.subscribe(&parameters_subscribe_topic).await?)
        } else {
            None
        };

        let (vote_tx, mut vote_rx) = mpsc::channel::<Arc<Message>>(256);
        if let Some(mut sub) = votes_subscription.take() {
            let tx = vote_tx.clone();
            let ctx = context.clone();
            ctx.run(async move {
                loop {
                    match sub.read().await {
                        Ok((_, msg)) => {
                            let _ = tx.send(msg).await;
                        }
                        Err(e) => {
                            tracing::warn!("votes subscription ended: {e}");
                            break;
                        }
                    }
                }
            });
        }

        let (param_tx, mut param_rx) = mpsc::channel::<Arc<Message>>(64);
        if let Some(mut sub) = parameters_subscription.take() {
            let tx = param_tx.clone();
            let ctx = context.clone();
            ctx.run(async move {
                loop {
                    match sub.read().await {
                        Ok((_, msg)) => {
                            let _ = tx.send(msg).await;
                        }
                        Err(e) => {
                            tracing::warn!("params subscription ended: {e}");
                            break;
                        }
                    }
                }
            });
        }

        // Main loop of synchronised messages
        let context_subscribe = context.clone();
        let context_handler = context.clone();
        let history_handler = history.clone();
        let drep_state_topic = drep_state_topic.clone();
        let init_cfg = storage_config;

        context.run(async move {
            use std::cmp::Ordering;
            use std::collections::BTreeMap;

            let mut vote_buf: BTreeMap<u64, Vec<Arc<Message>>> = BTreeMap::new();
            let mut params_buf: BTreeMap<u64, (u64, u32)> = BTreeMap::new();

            loop {
                // state snapshot (cloned) to work on
                let mut state = {
                    let mut h = history_handler.lock().await;
                    h.get_or_init_with(|| State::new(init_cfg))
                };

                // Anchor on certificates (blocking read)
                let Ok((_, certs_msg)) = certs_subscription.read().await else {
                    return;
                };
                let (block_info, tx_certs) = match certs_msg.as_ref() {
                    Message::Cardano((bi, CardanoMessage::TxCertificates(txcs))) => {
                        (bi.clone(), txcs)
                    }
                    _ => {
                        error!("Unexpected message on certificates: {certs_msg:?}");
                        continue;
                    }
                };

                if block_info.status == BlockStatus::RolledBack {
                    state = history_handler.lock().await.get_rolled_back_state(&block_info);
                    vote_buf.retain(|num, _| *num < block_info.number);
                    params_buf.retain(|_, (param_block, _)| *param_block < block_info.number);
                }
                let new_epoch = block_info.new_epoch && block_info.epoch > 0;

                // ---- Drain votes channel (non-blocking) and handle/buffer by block ----
                while let Ok(msg) = vote_rx.try_recv() {
                    if let Message::Cardano((bi, CardanoMessage::GovernanceProcedures(gp))) =
                        msg.as_ref()
                    {
                        match bi.number.cmp(&block_info.number) {
                            Ordering::Equal => {
                                if let Err(e) = state.handle_votes(gp).await {
                                    error!("Failed to handle governance procedures: {e}");
                                }
                            }
                            Ordering::Greater => {
                                vote_buf.entry(bi.number).or_default().push(msg.clone());
                            }
                            Ordering::Less => {
                                tracing::debug!("Stale governance msg for block {}", bi.number);
                            }
                        }
                    } else {
                        error!("Unexpected governance message: {msg:?}");
                    }
                }
                // Also apply any votes we buffered for this block (if they arrived earlier)
                if let Some(pending) = vote_buf.remove(&block_info.number) {
                    for msg in pending {
                        if let Message::Cardano((_bi, CardanoMessage::GovernanceProcedures(gp))) =
                            msg.as_ref()
                        {
                            if let Err(e) = state.handle_votes(gp).await {
                                error!("Failed to handle buffered governance procedures: {e}");
                            }
                        }
                    }
                }

                // Always drain params channel; store the epoch and the block they apply from
                while let Ok(msg) = param_rx.try_recv() {
                    if let Message::Cardano((bi, CardanoMessage::ProtocolParams(pp))) = msg.as_ref()
                    {
                        if let Some(conway) = &pp.params.conway {
                            params_buf.insert(bi.epoch, (bi.number, conway.d_rep_activity));
                        } else {
                            tracing::debug!(
                                "ProtocolParams without Conway (epoch {}, block {})",
                                bi.epoch,
                                bi.number
                            );
                        }
                    } else {
                        error!("Unexpected parameters message: {msg:?}");
                    }
                }

                // Apply once we reach or pass the block that emitted the params for this epoch
                if let Some(&(param_block, activity)) = params_buf.get(&block_info.epoch) {
                    if block_info.number >= param_block {
                        if let Err(err) = state.update_drep_expirations(block_info.epoch, activity)
                        {
                            error!("Failed to update DRep expirations: {err}");
                        }
                        params_buf.remove(&block_info.epoch);
                    }
                }

                // ---- Certificates last (same as before) ----
                if let Err(e) = state.handle_certificates(context_handler.clone(), tx_certs).await {
                    error!("Certificates handling error: {e}");
                }

                // Commit the new state snapshot
                {
                    let mut h = history_handler.lock().await;
                    h.commit(&block_info, state);
                }

                // Publish epoch snapshot
                if new_epoch && block_info.epoch > 0 {
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
                    if let Err(e) =
                        context_subscribe.publish(&drep_state_topic, Arc::new(out)).await
                    {
                        error!("Failed to publish DRep state: {e}");
                    }
                }
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

                            snapshot.tick().await.inspect_err(|e| error!("Tick error: {e}")).ok();
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
