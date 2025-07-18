//! REST handlers for Acropolis Accounts State module
use std::collections::HashMap;
use std::sync::Arc;

use acropolis_common::serialization::Bech32WithHrp;
use acropolis_common::DRepChoice;
use anyhow::{anyhow, Result};
use tokio::sync::Mutex;

use crate::distribution_history::DistributionHistory;
use crate::state::State;
use acropolis_common::state_history::StateHistory;
use acropolis_common::{
    messages::RESTResponse, Address, Lovelace, StakeAddress, StakeAddressPayload,
};

/// REST response structure for /accounts/{stake_address}
#[derive(serde::Serialize)]
pub struct APIStakeAccount {
    pub utxo_value: u64,
    pub rewards: u64,
    pub delegated_spo: Option<String>,
    pub delegated_drep: Option<APIDRepChoice>,
}
#[derive(serde::Serialize)]
pub struct APIDRepChoice {
    pub drep_type: String,
    pub value: Option<String>,
}

/// Handles /drdd
#[derive(serde::Serialize, serde::Deserialize)]
struct APIDRepDelegationDistribution {
    pub abstain: Lovelace,
    pub no_confidence: Lovelace,
    pub dreps: Vec<(String, u64)>,
}

/// Handles /accounts/info/{stake_address}
pub async fn handle_single_account(
    history: Arc<Mutex<StateHistory<State>>>,
    param: String,
) -> Result<RESTResponse> {
    let stake_address = match Address::from_string(&param) {
        Ok(Address::Stake(StakeAddress {
            payload: StakeAddressPayload::StakeKeyHash(hash),
            ..
        })) => hash,
        _ => {
            return Ok(RESTResponse::with_text(
                400,
                &format!("Not a valid stake address: {param}"),
            ));
        }
    };

    let locked = history.lock().await;
    let state = match locked.current() {
        Some(state) => state,
        None => return Ok(RESTResponse::with_text(500, "No current state available")),
    };

    let stake = match state.get_stake_state(&stake_address) {
        Some(stake) => stake,
        None => return Ok(RESTResponse::with_text(404, "Stake address not found")),
    };

    let delegated_spo = match &stake.delegated_spo {
        Some(spo) => match spo.to_bech32_with_hrp("pool") {
            Ok(val) => Some(val),
            Err(e) => {
                return Ok(RESTResponse::with_text(
                    500,
                    &format!("Internal server error while retrieving stake address: {e}"),
                ));
            }
        },
        None => None,
    };

    let delegated_drep = match &stake.delegated_drep {
        Some(drep) => match map_drep_choice(drep) {
            Ok(val) => Some(val),
            Err(e) => {
                return Ok(RESTResponse::with_text(
                    500,
                    &format!("Internal server error while retrieving stake address: {e}"),
                ))
            }
        },
        None => None,
    };

    let response = APIStakeAccount {
        utxo_value: stake.utxo_value,
        rewards: stake.rewards,
        delegated_spo: delegated_spo,
        delegated_drep: delegated_drep,
    };

    match serde_json::to_string(&response) {
        Ok(json) => Ok(RESTResponse::with_json(200, &json)),
        Err(e) => Ok(RESTResponse::with_text(
            500,
            &format!("Internal server error while retrieving stake address: {e}"),
        )),
    }
}

/// Handles /spdd
pub async fn handle_spdd(
    history: Arc<Mutex<StateHistory<State>>>,
    distribution_history: Option<Arc<Mutex<DistributionHistory>>>,
    query: Option<HashMap<String, String>>,
) -> Result<RESTResponse> {
    tracing::info!("Query parameters: {:?}", query);
    // Handle historical if query parameter provided
    if let Some(query) = query {
        let epoch = match extract_epoch_from_query(&query) {
            Ok(epoch) => epoch,
            Err(e) => {
                return Ok(RESTResponse::with_text(
                    400,
                    &format!("Invalid query parameter: {e}"),
                ))
            }
        };

        if let Some(dist_arc) = distribution_history {
            let dist = dist_arc.lock().await;
            match dist.get_spdd(epoch) {
                Some(spdd) => Ok(RESTResponse::with_json(200, &serde_json::to_string(&spdd)?)),
                None => Ok(RESTResponse::with_text(
                    404,
                    &format!("No SPDD found for epoch {epoch}"),
                )),
            }
        } else {
            Ok(RESTResponse::with_text(
                400,
                "Distribution history is not enabled.",
            ))
        }
    } else {
        // No query parameter, provide current spdd
        let locked = history.lock().await;
        let state = match locked.current() {
            Some(state) => state,
            None => return Ok(RESTResponse::with_json(200, "{}")),
        };

        let spdd: HashMap<String, u64> = state
            .generate_spdd()
            .iter()
            .map(|(k, v)| {
                let bech32 = k.to_bech32_with_hrp("pool").unwrap_or_else(|_| hex::encode(k));
                (bech32, *v)
            })
            .collect();

        Ok(RESTResponse::with_json(200, &serde_json::to_string(&spdd)?))
    }
}

/// Handles /pots
pub async fn handle_pots(history: Arc<Mutex<StateHistory<State>>>) -> Result<RESTResponse> {
    let locked = history.lock().await;
    let state = match locked.current() {
        Some(state) => state,
        None => return Ok(RESTResponse::with_json(200, "{}")),
    };

    let pots = state.get_pots();

    match serde_json::to_string(&pots) {
        Ok(body) => Ok(RESTResponse::with_json(200, &body)),
        Err(e) => Ok(RESTResponse::with_text(
            500,
            &format!("Internal server error while retrieving pots: {e}"),
        )),
    }
}

/// Handles /drdd
pub async fn handle_drdd(
    history: Arc<Mutex<StateHistory<State>>>,
    distribution_history: Option<Arc<Mutex<DistributionHistory>>>,
    query: Option<HashMap<String, String>>,
) -> Result<RESTResponse> {
    tracing::info!("Received DRDD request with query: {:?}", query);
    // Handle historical if query parameter provided
    if let Some(query) = query {
        let epoch = match extract_epoch_from_query(&query) {
            Ok(epoch) => epoch,
            Err(e) => {
                return Ok(RESTResponse::with_text(
                    400,
                    &format!("Invalid query parameter: {e}"),
                ))
            }
        };

        if let Some(distribution_arc) = distribution_history {
            let distribution = distribution_arc.lock().await;
            match distribution.get_drdd(epoch) {
                Some(drdd) => {
                    let dreps = {
                        let mut dreps = Vec::with_capacity(drdd.dreps.len());
                        for (cred, amount) in drdd.dreps.iter() {
                            let bech32 = match cred.to_drep_bech32() {
                                Ok(val) => val,
                                Err(e) => {
                                    return Ok(RESTResponse::with_text(
                                        500,
                                        &format!("Internal server error while retrieving DRep delegation distribution: {e}"),
                ));
                                }
                            };
                            dreps.push((bech32, *amount));
                        }
                        dreps
                    };

                    let response = APIDRepDelegationDistribution {
                        abstain: drdd.abstain,
                        no_confidence: drdd.no_confidence,
                        dreps,
                    };

                    return match serde_json::to_string(&response) {
                        Ok(json) => Ok(RESTResponse::with_json(200, &json)),
                        Err(e) => Ok(RESTResponse::with_text(
                            500,
                            &format!("Internal server error while serializing response: {e}"),
                        )),
                    };
                }
                None => {
                    return Ok(RESTResponse::with_text(
                        404,
                        &format!("No DRDD found for epoch {epoch}"),
                    ));
                }
            }
        } else {
            return Ok(RESTResponse::with_text(
                400,
                "Distribution history is not enabled.",
            ));
        }
    }

    // No query parameter, provide current drdd
    let locked = history.lock().await;
    let state = match locked.current() {
        Some(state) => state,
        None => return Ok(RESTResponse::with_json(200, "{}")),
    };

    let drdd = state.generate_drdd();

    let dreps = {
        let mut dreps = Vec::with_capacity(drdd.dreps.len());
        for (cred, amount) in drdd.dreps {
            let bech32 = match cred.to_drep_bech32() {
                Ok(val) => val,
                Err(e) => {
                    return Ok(RESTResponse::with_text(
                        500,
                        &format!("Internal server error while retrieving DRep delegation distribution: {e}"),
                    ));
                }
            };
            dreps.push((bech32, amount));
        }
        dreps
    };

    let response = APIDRepDelegationDistribution {
        abstain: drdd.abstain,
        no_confidence: drdd.no_confidence,
        dreps,
    };

    match serde_json::to_string(&response) {
        Ok(json) => Ok(RESTResponse::with_json(200, &json)),
        Err(e) => Ok(RESTResponse::with_text(
            500,
            &format!("Internal server error while retrieving DRep delegation distribution: {e}"),
        )),
    }
}

fn map_drep_choice(drep: &DRepChoice) -> Result<APIDRepChoice> {
    match drep {
        DRepChoice::Key(hash) => {
            let val = hash
                .to_bech32_with_hrp("drep")
                .map_err(|e| anyhow!("Bech32 encoding failed for DRep Key: {e}"))?;
            Ok(APIDRepChoice {
                drep_type: "Key".to_string(),
                value: Some(val),
            })
        }
        DRepChoice::Script(hash) => {
            let val = hash
                .to_bech32_with_hrp("drep_script")
                .map_err(|e| anyhow!("Bech32 encoding failed for DRep Script: {e}"))?;
            Ok(APIDRepChoice {
                drep_type: "Script".to_string(),
                value: Some(val),
            })
        }
        DRepChoice::Abstain => Ok(APIDRepChoice {
            drep_type: "Abstain".to_string(),
            value: None,
        }),
        DRepChoice::NoConfidence => Ok(APIDRepChoice {
            drep_type: "NoConfidence".to_string(),
            value: None,
        }),
    }
}

/// Extract epoch number from query parameters
pub fn extract_epoch_from_query(query: &HashMap<String, String>) -> Result<u64> {
    let epoch_str =
        query.get("epoch").ok_or_else(|| anyhow::anyhow!("Missing 'epoch' query parameter"))?;
    epoch_str.parse::<u64>().map_err(|_| anyhow::anyhow!("Invalid epoch value"))
}
