//! REST handlers for Acropolis Blockfrost /governance endpoints
use acropolis_common::{
    messages::{Message, RESTResponse, StateQuery, StateQueryResponse},
    queries::governance::{GovernanceStateQuery, GovernanceStateQueryResponse},
    Credential, GovActionId, Voter,
};
use anyhow::Result;
use caryatid_sdk::Context;
use std::sync::Arc;

use crate::types::{DRepInfoREST, DRepUpdateREST, DRepsListREST, ProposalVoteREST, VoterRoleREST};

pub async fn handle_dreps_list_blockfrost(
    context: Arc<Context<Message>>,
    _params: Vec<String>,
) -> Result<RESTResponse> {
    let msg = Arc::new(Message::StateQuery(StateQuery::Governance(
        GovernanceStateQuery::GetDRepsList,
    )));
    let raw = context.message_bus.request("drep-state", msg).await?;
    let message = Arc::try_unwrap(raw).unwrap_or_else(|arc| (*arc).clone());
    match message {
        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::DRepsList(list),
        )) => {
            let response: Vec<DRepsListREST> = list
                .dreps
                .iter()
                .map(|cred| {
                    Ok(DRepsListREST {
                        drep_id: cred.to_drep_bech32()?,
                        hex: hex::encode(cred.get_hash()),
                    })
                })
                .collect::<Result<_, anyhow::Error>>()?;

            Ok(RESTResponse::with_json(
                200,
                &serde_json::to_string_pretty(&response)?,
            ))
        }

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::Error(e),
        )) => Ok(RESTResponse::with_text(500, &format!("Query error: {e}"))),

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::NotFound,
        )) => Ok(RESTResponse::with_text(404, "No DReps found")),

        _ => Ok(RESTResponse::with_text(500, "Unexpected message type")),
    }
}

pub async fn handle_single_drep_blockfrost(
    context: Arc<Context<Message>>,
    params: Vec<String>,
) -> Result<RESTResponse> {
    let Some(drep_id) = params.get(0) else {
        return Ok(RESTResponse::with_text(400, "Missing DRep ID parameter"));
    };

    let credential = match Credential::from_drep_bech32(drep_id) {
        Ok(c) => c,
        Err(e) => {
            return Ok(RESTResponse::with_text(
                400,
                &format!("Invalid Bech32 DRep ID: {drep_id}. Error: {e}"),
            ));
        }
    };

    let msg = Arc::new(Message::StateQuery(StateQuery::Governance(
        GovernanceStateQuery::GetDRepInfo {
            drep_credential: credential.clone(),
        },
    )));

    let raw = context.message_bus.request("drep-state", msg).await?;
    let message = Arc::try_unwrap(raw).unwrap_or_else(|arc| (*arc).clone());

    match message {
        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::DRepInfo(info),
        )) => {
            let active = !info.retired && !info.expired;

            let rest_info = DRepInfoREST {
                drep_id: credential.to_drep_bech32().unwrap_or_else(|_| "<invalid>".to_string()),
                hex: hex::encode(credential.get_hash()),
                amount: info.deposit.to_string(),
                active,
                active_epoch: info.active_epoch, // <- allow null
                has_script: matches!(credential, Credential::ScriptHash(_)),
                last_active_epoch: info.last_active_epoch,
                retired: info.retired,
                expired: info.expired,
            };

            match serde_json::to_string_pretty(&rest_info) {
                Ok(json) => Ok(RESTResponse::with_json(200, &json)),
                Err(e) => Ok(RESTResponse::with_text(
                    500,
                    &format!("Failed to serialize DRep info: {e}"),
                )),
            }
        }

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::NotFound,
        )) => Ok(RESTResponse::with_text(404, "DRep not found")),

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::Error(_),
        )) => Ok(RESTResponse::with_text(
            500,
            &format!("DRep info storage is disabled in config"),
        )),

        _ => Ok(RESTResponse::with_text(500, "Unexpected message type")),
    }
}

pub async fn handle_drep_delegators_blockfrost(
    _context: Arc<Context<Message>>,
    _params: Vec<String>,
) -> Result<RESTResponse> {
    Ok(RESTResponse::with_text(501, "Not implemented"))
}

pub async fn handle_drep_metadata_blockfrost(
    _context: Arc<Context<Message>>,
    _params: Vec<String>,
) -> Result<RESTResponse> {
    Ok(RESTResponse::with_text(501, "Not implemented"))
}

pub async fn handle_drep_updates_blockfrost(
    context: Arc<Context<Message>>,
    params: Vec<String>,
) -> Result<RESTResponse> {
    let Some(drep_id) = params.get(0) else {
        return Ok(RESTResponse::with_text(400, "Missing DRep ID parameter"));
    };

    let credential = match Credential::from_drep_bech32(drep_id) {
        Ok(c) => c,
        Err(e) => {
            return Ok(RESTResponse::with_text(
                400,
                &format!("Invalid Bech32 DRep ID: {drep_id}. Error: {e}"),
            ));
        }
    };

    let msg = Arc::new(Message::StateQuery(StateQuery::Governance(
        GovernanceStateQuery::GetDRepUpdates {
            drep_credential: credential.clone(),
        },
    )));

    let raw = context.message_bus.request("drep-state", msg).await?;
    let message = Arc::try_unwrap(raw).unwrap_or_else(|arc| (*arc).clone());

    match message {
        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::DRepUpdates(list),
        )) => {
            let response: Vec<DRepUpdateREST> = list
                .updates
                .iter()
                .map(|event| DRepUpdateREST {
                    tx_hash: hex::encode(event.tx_hash),
                    cert_index: event.cert_index,
                    action: event.action.clone(),
                })
                .collect();

            Ok(RESTResponse::with_json(
                200,
                &serde_json::to_string(&response)?,
            ))
        }

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::Error(_),
        )) => Ok(RESTResponse::with_text(
            503,
            &format!("DRep updates storage is disabled in config"),
        )),

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::NotFound,
        )) => Ok(RESTResponse::with_text(404, "DRep not found")),

        _ => Ok(RESTResponse::with_text(500, "Unexpected message type")),
    }
}

pub async fn handle_drep_votes_blockfrost(
    _context: Arc<Context<Message>>,
    _params: Vec<String>,
) -> Result<RESTResponse> {
    Ok(RESTResponse::with_text(501, "Not implemented"))
}

pub async fn handle_proposals_list_blockfrost(
    context: Arc<Context<Message>>,
    _params: Vec<String>,
) -> Result<RESTResponse> {
    let msg = Arc::new(Message::StateQuery(StateQuery::Governance(
        GovernanceStateQuery::GetProposalsList,
    )));

    let raw = context.message_bus.request("governance-state", msg).await?;
    let message = Arc::try_unwrap(raw).unwrap_or_else(|arc| (*arc).clone());

    match message {
        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::ProposalsList(list),
        )) => {
            if list.proposals.is_empty() {
                return Ok(RESTResponse::with_json(200, "[]"));
            }

            let props_bech32: Result<Vec<String>, _> =
                list.proposals.iter().map(|id| id.to_bech32()).collect();

            match props_bech32 {
                Ok(vec) => match serde_json::to_string(&vec) {
                    Ok(json) => Ok(RESTResponse::with_json(200, &json)),
                    Err(e) => Ok(RESTResponse::with_text(
                        500,
                        &format!("Failed to serialize proposals list: {e}"),
                    )),
                },
                Err(e) => Ok(RESTResponse::with_text(
                    500,
                    &format!("Failed to convert proposal IDs to Bech32: {e}"),
                )),
            }
        }

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::Error(e),
        )) => Ok(RESTResponse::with_text(500, &format!("Query error: {e}"))),

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::NotFound,
        )) => Ok(RESTResponse::with_text(404, "No proposals found")),

        _ => Ok(RESTResponse::with_text(500, "Unexpected message type")),
    }
}

pub async fn handle_single_proposal_blockfrost(
    context: Arc<Context<Message>>,
    params: Vec<String>,
) -> Result<RESTResponse> {
    let proposal = match parse_gov_action_id(&params)? {
        Ok(id) => id,
        Err(resp) => return Ok(resp),
    };

    let msg = Arc::new(Message::StateQuery(StateQuery::Governance(
        GovernanceStateQuery::GetProposalInfo { proposal },
    )));
    let raw = context.message_bus.request("governance-state", msg).await?;
    let message = Arc::try_unwrap(raw).unwrap_or_else(|arc| (*arc).clone());

    match message {
        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::ProposalInfo(info),
        )) => match serde_json::to_string(&info) {
            Ok(json) => Ok(RESTResponse::with_json(200, &json)),
            Err(e) => Ok(RESTResponse::with_text(
                500,
                &format!("Failed to serialize proposal info: {e}"),
            )),
        },

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::NotFound,
        )) => Ok(RESTResponse::with_text(404, "Proposal not found")),

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::Error(e),
        )) => Ok(RESTResponse::with_text(500, &format!("Query error: {e}"))),

        _ => Ok(RESTResponse::with_text(500, "Unexpected message type")),
    }
}

pub async fn handle_proposal_parameters_blockfrost(
    _context: Arc<Context<Message>>,
    _params: Vec<String>,
) -> Result<RESTResponse> {
    Ok(RESTResponse::with_text(501, "Not implemented"))
}

pub async fn handle_proposal_withdrawals_blockfrost(
    _context: Arc<Context<Message>>,
    _params: Vec<String>,
) -> Result<RESTResponse> {
    Ok(RESTResponse::with_text(501, "Not implemented"))
}

pub async fn handle_proposal_votes_blockfrost(
    context: Arc<Context<Message>>,
    params: Vec<String>,
) -> Result<RESTResponse> {
    let proposal = match parse_gov_action_id(&params)? {
        Ok(id) => id,
        Err(resp) => return Ok(resp),
    };

    let tx_hash = hex::encode(&proposal.transaction_id);
    let cert_index = proposal.action_index;

    let msg = Arc::new(Message::StateQuery(StateQuery::Governance(
        GovernanceStateQuery::GetProposalVotes { proposal },
    )));

    let raw = context.message_bus.request("governance-state", msg).await?;
    let message = Arc::try_unwrap(raw).unwrap_or_else(|arc| (*arc).clone());

    match message {
        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::ProposalVotes(votes),
        )) => {
            let mut votes_list = Vec::new();

            for (voter, (_, voting_proc)) in votes.votes {
                let voter_role = match voter {
                    Voter::ConstitutionalCommitteeKey(_)
                    | Voter::ConstitutionalCommitteeScript(_) => {
                        VoterRoleREST::ConstitutionalCommittee
                    }
                    Voter::DRepKey(_) | Voter::DRepScript(_) => VoterRoleREST::Drep,
                    Voter::StakePoolKey(_) => VoterRoleREST::Spo,
                };

                let voter_str = voter.to_string();

                votes_list.push(ProposalVoteREST {
                    tx_hash: tx_hash.clone(),
                    cert_index,
                    voter_role,
                    voter: voter_str,
                    vote: voting_proc.vote,
                });
            }

            match serde_json::to_string(&votes_list) {
                Ok(json) => Ok(RESTResponse::with_json(200, &json)),
                Err(e) => Ok(RESTResponse::with_text(
                    500,
                    &format!("Internal server error while retrieving proposal votes: {e}"),
                )),
            }
        }

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::NotFound,
        )) => Ok(RESTResponse::with_text(404, "Proposal not found")),

        Message::StateQueryResponse(StateQueryResponse::Governance(
            GovernanceStateQueryResponse::Error(e),
        )) => Ok(RESTResponse::with_text(500, &format!("Query error: {e}"))),

        _ => Ok(RESTResponse::with_text(500, "Unexpected message type")),
    }
}

pub async fn handle_proposal_metadata_blockfrost(
    _context: Arc<Context<Message>>,
    _params: Vec<String>,
) -> Result<RESTResponse> {
    Ok(RESTResponse::with_text(501, "Not implemented"))
}

pub fn parse_gov_action_id(params: &[String]) -> Result<Result<GovActionId, RESTResponse>> {
    if params.len() != 2 {
        return Ok(Err(RESTResponse::with_text(
            400,
            "Expected two parameters: tx_hash/cert_index",
        )));
    }

    let tx_hash_hex = &params[0];
    let cert_index_str = &params[1];

    let transaction_id = match hex::decode(tx_hash_hex) {
        Ok(bytes) => bytes,
        Err(e) => {
            return Ok(Err(RESTResponse::with_text(
                400,
                &format!("Invalid hex tx_hash: {e}"),
            )));
        }
    };

    let action_index = match cert_index_str.parse::<u8>() {
        Ok(i) => i,
        Err(e) => {
            return Ok(Err(RESTResponse::with_text(
                400,
                &format!("Invalid cert_index, expected u8: {e}"),
            )));
        }
    };

    Ok(Ok(GovActionId {
        transaction_id,
        action_index,
    }))
}
