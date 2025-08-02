use std::sync::Arc;

use caryatid_sdk::Context;

use crate::{
    messages::{Message, StateQuery, StateQueryResponse},
    DRepChoice, KeyHash, StakeCredential,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AccountsStateQuery {
    GetAccountInfo { stake_key: Vec<u8> },
    GetAccountRewardHistory { stake_key: Vec<u8> },
    GetAccountHistory { stake_key: Vec<u8> },
    GetAccountDelegationHistory { stake_key: Vec<u8> },
    GetAccountRegistrationHistory { stake_key: Vec<u8> },
    GetAccountWithdrawalHistory { stake_key: Vec<u8> },
    GetAccountMIRHistory { stake_key: Vec<u8> },
    GetAccountAssociatedAddresses { stake_key: Vec<u8> },
    GetAccountAssets { stake_key: Vec<u8> },
    GetAccountAssetsTotals { stake_key: Vec<u8> },
    GetAccountUTxOs { stake_key: Vec<u8> },
    GetAccountBalance { stake_key: Vec<u8> },
    GetAccountDRepDelegation { stake_key: Vec<u8> },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AccountsStateQueryResponse {
    AccountInfo(AccountInfo),
    AccountRewardHistory(AccountRewardHistory),
    AccountHistory(AccountHistory),
    AccountDelegationHistory(AccountDelegationHistory),
    AccountRegistrationHistory(AccountRegistrationHistory),
    AccountWithdrawalHistory(AccountWithdrawalHistory),
    AccountMIRHistory(AccountMIRHistory),
    AccountAssociatedAddresses(AccountAssociatedAddresses),
    AccountAssets(AccountAssets),
    AccountAssetsTotals(AccountAssetsTotals),
    AccountUTxOs(AccountUTxOs),
    AccountBalance(u64),
    AccountDRepDelegation(Option<DRepChoice>),
    NotFound,
    Error(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountInfo {
    pub utxo_value: u64,
    pub rewards: u64,
    pub delegated_spo: Option<KeyHash>,
    pub delegated_drep: Option<DRepChoice>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountRewardHistory {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountHistory {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountDelegationHistory {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountRegistrationHistory {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountWithdrawalHistory {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountMIRHistory {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountAssociatedAddresses {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountAssets {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountAssetsTotals {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountUTxOs {}

pub async fn sum_account_balances(
    context: Arc<Context<Message>>,
    stake_credentials: &[StakeCredential],
) -> Result<u64, String> {
    let mut total: u64 = 0;

    for cred in stake_credentials.iter() {
        let msg = Arc::new(Message::StateQuery(StateQuery::Accounts(
            AccountsStateQuery::GetAccountBalance {
                stake_key: cred.get_hash(),
            },
        )));

        match context.message_bus.request("accounts-state", msg).await {
            Ok(raw) => match Arc::try_unwrap(raw).unwrap_or_else(|arc| (*arc).clone()) {
                Message::StateQueryResponse(StateQueryResponse::Accounts(
                    AccountsStateQueryResponse::AccountBalance(amount),
                )) => {
                    total = total.saturating_add(amount);
                }
                other => {
                    return Err(format!(
                        "Unexpected accounts-state response for {:?}: {:?}",
                        cred, other
                    ));
                }
            },
            Err(e) => {
                return Err(format!("Failed to query balance for {:?}: {}", cred, e));
            }
        }
    }

    Ok(total)
}
