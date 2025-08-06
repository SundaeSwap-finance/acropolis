//! Acropolis DRep State module for Caryatid
//! Accepts certificate events and derives the DRep State in memory

use acropolis_common::{
    messages::{CardanoMessage, DRepStateMessage, Message, StateQuery, StateQueryResponse},
    queries::governance::{
        DRepDelegatorAddresses, DRepInfo, DRepInfoWithDelegators, DRepMetadata, DRepUpdates,
        DRepVotes, DRepsList, GovernanceStateQuery, GovernanceStateQueryResponse,
    },
};
use anyhow::Result;
use caryatid_sdk::{module, Context, Module};
use config::Config;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, info_span, Instrument};

mod state;
use state::State;

use crate::state::DRepStorageConfig;

const DEFAULT_CERTIFICATES_SUBSCRIBE_TOPIC: (&str, &str) =
    ("certificates-subscribe-topic", "cardano.certificates");
const DEFAULT_GOVERNANCE_SUBSCRIBE_TOPIC: (&str, &str) =
    ("governance-subscribe-topic", "cardano.governance");
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

        let state = Arc::new(Mutex::new(State::new(storage_config.clone())));

        // Subscribe for certificate messages
        let state1 = state.clone();
        let mut subscription = context.subscribe(&certificates_subscribe_topic).await?;
        let context_subscribe = context.clone();
        let context_cert_handler = context.clone();
        context.run(async move {
            loop {
                let Ok((_, message)) = subscription.read().await else {
                    return;
                };
                match message.as_ref() {
                    Message::Cardano((block_info, CardanoMessage::TxCertificates(tx_cert_msg))) => {
                        let context_handle = context_cert_handler.clone();
                        let span = info_span!("drep_state.handle", block = block_info.number);
                        async {
                            let mut state = state1.lock().await;
                            state
                                .handle_certificates(context_handle, &tx_cert_msg)
                                .await
                                .inspect_err(|e| error!("Messaging handling error: {e}"))
                                .ok();

                            if block_info.new_epoch && block_info.epoch > 0 {
                                // publish DRep state at end of epoch
                                let dreps = state.active_drep_list();
                                let message = Message::Cardano((
                                    block_info.clone(),
                                    CardanoMessage::DRepState(DRepStateMessage {
                                        epoch: block_info.epoch,
                                        dreps,
                                    }),
                                ));
                                context_subscribe
                                    .publish(&drep_state_topic, Arc::new(message))
                                    .await
                                    .unwrap_or_else(|e| error!("Failed to publish: {e}"));
                            }
                        }
                        .instrument(span)
                        .await;
                    }

                    _ => error!("Unexpected message type: {message:?}"),
                }
            }
        });

        // Optionally subscribe to governance messages to process and store DRep votes
        if storage_config.store_votes {
            let governance_subscribe_topic =
                get_string(&config, DEFAULT_GOVERNANCE_SUBSCRIBE_TOPIC);
            info!("Creating subscriber on '{governance_subscribe_topic}'");

            let state_votes = state.clone();
            let mut procedures_subscription =
                context.subscribe(&governance_subscribe_topic).await?;

            context.run(async move {
                loop {
                    let Ok((_, message)) = procedures_subscription.read().await else {
                        return;
                    };

                    if let Message::Cardano((
                        block_info,
                        CardanoMessage::GovernanceProcedures(proc_msg),
                    )) = message.as_ref()
                    {
                        let span = info_span!("drep_state.handle_votes", block = block_info.number);
                        async {
                            state_votes
                                .lock()
                                .await
                                .handle_votes(&proc_msg)
                                .await
                                .inspect_err(|e| {
                                    error!("Failed to handle governance procedures: {e}")
                                })
                                .ok();
                        }
                        .instrument(span)
                        .await;
                    }
                }
            });
        }

        let query_state = state.clone();
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
                    GovernanceStateQuery::GetDRepsList => {
                        let dreps = locked.list();
                        GovernanceStateQueryResponse::DRepsList(DRepsList { dreps })
                    }
                    GovernanceStateQuery::GetDRepInfoWithDelegators { drep_credential } => {
                        match locked.get_drep_info(&drep_credential) {
                            Ok(Some(info)) => match locked.get_drep_delegators(&drep_credential) {
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

                                    GovernanceStateQueryResponse::DRepInfoWithDelegators(response)
                                }

                                Ok(None) => GovernanceStateQueryResponse::NotFound,

                                Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                            },

                            Ok(None) => GovernanceStateQueryResponse::NotFound,

                            Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                        }
                    }

                    GovernanceStateQuery::GetDRepDelegators { drep_credential } => {
                        match locked.get_drep_delegators(&drep_credential) {
                            Ok(Some(delegators)) => GovernanceStateQueryResponse::DRepDelegators(
                                DRepDelegatorAddresses {
                                    addresses: delegators.clone(),
                                },
                            ),
                            Ok(None) => GovernanceStateQueryResponse::NotFound,
                            Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                        }
                    }
                    GovernanceStateQuery::GetDRepMetadata { drep_credential } => {
                        match locked.get_drep_anchor(&drep_credential) {
                            Ok(Some(anchor)) => {
                                GovernanceStateQueryResponse::DRepMetadata(DRepMetadata {
                                    anchor: Some(anchor.clone()),
                                })
                            }
                            Ok(None) => GovernanceStateQueryResponse::NotFound,
                            Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                        }
                    }
                    GovernanceStateQuery::GetDRepUpdates { drep_credential } => {
                        match locked.get_drep_updates(&drep_credential) {
                            Ok(Some(updates)) => {
                                GovernanceStateQueryResponse::DRepUpdates(DRepUpdates {
                                    updates: updates.to_vec(),
                                })
                            }
                            Ok(None) => GovernanceStateQueryResponse::NotFound,
                            Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
                        }
                    }
                    GovernanceStateQuery::GetDRepVotes { drep_credential } => {
                        match locked.get_drep_votes(drep_credential) {
                            Ok(Some(votes)) => GovernanceStateQueryResponse::DRepVotes(DRepVotes {
                                votes: votes.to_vec(),
                            }),
                            Ok(None) => GovernanceStateQueryResponse::NotFound,
                            Err(msg) => GovernanceStateQueryResponse::Error(msg.to_string()),
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
        let mut subscription = context.subscribe(&certificates_subscribe_topic).await?;
        let state2 = state.clone();
        context.run(async move {
            loop {
                let Ok((_, message)) = subscription.read().await else {
                    return;
                };
                if let Message::Clock(message) = message.as_ref() {
                    if (message.number % 60) == 0 {
                        let span = info_span!("drep_state.tick", number = message.number);
                        async {
                            state2
                                .lock()
                                .await
                                .tick()
                                .await
                                .inspect_err(|e| error!("Tick error: {e}"))
                                .ok();
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
