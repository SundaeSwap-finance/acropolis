//! REST handlers for Acropolis UTxO State module

use std::{collections::HashMap, sync::Arc};

use crate::state::{State, UTXOKey, Value};
use acropolis_common::messages::RESTResponse;
use anyhow::Result;
use tokio::sync::Mutex;

/// REST response structure for single UTxO balance
#[derive(serde::Serialize)]
pub struct UTxOBalanceRest {
    pub address: String,
    pub value: UTxOValueRest,
}

/// REST response structure for value (ADA + multi-assets grouped by policy)
#[derive(serde::Serialize)]
pub struct UTxOValueRest {
    pub coin: u64,
    pub multiassets: Option<Vec<PolicyAssetsRest>>,
}

/// REST response structure for multi-assets grouped by policy ID
#[derive(serde::Serialize)]
pub struct PolicyAssetsRest {
    /// Hex-encoded policy ID
    pub policy_id: String,

    /// List of assets under this policy
    pub assets: Vec<AssetRest>,
}

/// REST response structure for a single asset
#[derive(serde::Serialize)]
pub struct AssetRest {
    /// Hex-encoded asset name
    pub asset_name: String,

    /// Amount of this asset
    pub amount: u64,
}

/// Handles /utxos/{tx_hash:index}
pub async fn handle_single_utxo(
    state: Arc<Mutex<State>>,
    param: String,
) -> Result<RESTResponse, anyhow::Error> {
    let (tx_hash_str, index_str) = match param.split_once(':') {
        Some((tx, idx)) => (tx, idx),
        None => {
            return Ok(RESTResponse::with_text(
                400,
                &format!(
                    "Parameter must be in <tx_hash>:<index> format. Provided param: {}",
                    param
                ),
            ));
        }
    };

    let tx_hash_bytes = match hex::decode(tx_hash_str) {
        Ok(bytes) => bytes,
        Err(e) => {
            return Ok(RESTResponse::with_text(
                400,
                &format!("Invalid tx_hash: {e}"),
            ));
        }
    };

    let index = match index_str.parse::<u64>() {
        Ok(idx) => idx,
        Err(e) => {
            return Ok(RESTResponse::with_text(400, &format!("Invalid index: {e}")));
        }
    };

    let locked = state.lock().await;
    let key = UTXOKey::new(&tx_hash_bytes, index);

    let utxo_opt = match locked.lookup_utxo(&key).await {
        Ok(res) => res,
        Err(e) => {
            return Ok(RESTResponse::with_text(
                500,
                &format!("Internal server error while retrieving UTxO: {e}"),
            ));
        }
    };

    match utxo_opt {
        Some(utxo) => {
            let address_text = match utxo.address.to_string() {
                Ok(addr) => addr,
                Err(e) => {
                    return Ok(RESTResponse::with_text(
                        500,
                        &format!("Internal server error while retrieving UTxO: {e}"),
                    ));
                }
            };

            let response = UTxOBalanceRest {
                address: address_text,
                value: convert_value(&utxo.value),
            };

            match serde_json::to_string(&response) {
                Ok(body) => Ok(RESTResponse::with_json(200, &body)),
                Err(e) => Ok(RESTResponse::with_text(
                    500,
                    &format!("Internal server error while retrieving UTxO: {e}"),
                )),
            }
        }
        None => Ok(RESTResponse::with_text(
            404,
            &format!("UTxO not found. Provided UTxO: {}", param),
        )),
    }
}

fn convert_value(value: &Value) -> UTxOValueRest {
    let multiassets = value.multiassets.as_ref().map(|assets| {
        let mut grouped: HashMap<String, Vec<AssetRest>> = HashMap::new();

        for (policy, asset, amount) in assets {
            let policy_id = hex::encode(policy);
            let asset_name = hex::encode(asset);

            grouped.entry(policy_id).or_default().push(AssetRest {
                asset_name,
                amount: *amount,
            });
        }

        grouped
            .into_iter()
            .map(|(policy_id, assets)| PolicyAssetsRest { policy_id, assets })
            .collect()
    });

    UTxOValueRest {
        coin: value.coin,
        multiassets,
    }
}
