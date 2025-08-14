//! Acropolis DRepState: State storage

use acropolis_common::{
    messages::{
        GovernanceProceduresMessage, Message, StateQuery, StateQueryResponse, TxCertificatesMessage,
    },
    queries::{
        accounts::{AccountsStateQuery, AccountsStateQueryResponse, DEFAULT_ACCOUNTS_QUERY_TOPIC},
        get_query_topic,
        governance::{DRepActionUpdate, DRepMetadata, DRepUpdateEvent, VoteRecord},
    },
    Anchor, Credential, DRepChoice, DRepCredential, Lovelace, StakeCredential, TxCertificate,
    Voter,
};
use anyhow::{anyhow, Result};
use caryatid_sdk::Context;
use serde_with::serde_as;
use std::{collections::HashMap, sync::Arc};
use tracing::{info, warn};

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DRepRecord {
    pub deposit: Lovelace,
    pub anchor: Option<Anchor>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HistoricalDRepState {
    // Populated from the reg field in:
    // - DRepRegistration
    // - DRepDeregistration
    // - DRepUpdate
    pub info: Option<DRepRecordExtended>,
    pub updates: Option<Vec<DRepUpdateEvent>>,
    pub metadata: Option<DRepMetadata>,

    // Populated from the drep and credential fields in:
    // - VoteDelegation
    // - StakeAndVoteDelegation
    // - StakeRegistrationAndVoteDelegation
    // - StakeRegistrationAndStakeAndVoteDelegation
    pub delegators: Option<Vec<Credential>>,

    // Populated from voting_procedures in GovernanceProceduresMessage
    pub votes: Option<Vec<VoteRecord>>,
}

impl HistoricalDRepState {
    pub fn from_config(cfg: &DRepStorageConfig) -> Self {
        Self {
            info: cfg.store_info.then(DRepRecordExtended::default),
            updates: cfg.store_updates.then(Vec::new),
            metadata: cfg.store_metadata.then(|| DRepMetadata { anchor: None }),
            delegators: cfg.store_delegators.then(Vec::new),
            votes: cfg.store_votes.then(Vec::new),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct DRepRecordExtended {
    pub deposit: Lovelace,
    pub expired: bool,
    pub retired: bool,
    pub active_epoch: Option<u64>,
    pub last_active_epoch: u64,
}

impl DRepRecord {
    pub fn new(deposit: Lovelace, anchor: Option<Anchor>) -> Self {
        Self { deposit, anchor }
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct DRepStorageConfig {
    pub store_info: bool,
    pub store_delegators: bool,
    pub store_metadata: bool,
    pub store_updates: bool,
    pub store_votes: bool,
}

impl DRepStorageConfig {
    fn any_enabled(&self) -> bool {
        self.store_info
            || self.store_delegators
            || self.store_metadata
            || self.store_updates
            || self.store_votes
    }
}

#[derive(Debug, Default, Clone)]
pub struct State {
    pub config: DRepStorageConfig,
    pub dreps: HashMap<DRepCredential, DRepRecord>,
    pub historical_dreps: Option<HashMap<DRepCredential, HistoricalDRepState>>,
}

impl State {
    pub fn new(config: DRepStorageConfig) -> Self {
        Self {
            config,
            dreps: HashMap::new(),
            historical_dreps: if config.any_enabled() {
                Some(HashMap::new())
            } else {
                None
            },
        }
    }

    pub fn active_drep_list(&self) -> Vec<(DRepCredential, Lovelace)> {
        self.dreps.iter().map(|(d, r)| (d.clone(), r.deposit)).collect()
    }

    pub fn list(&self) -> Vec<DRepCredential> {
        self.dreps.keys().cloned().collect()
    }

    pub fn get_drep_info(
        &self,
        credential: &DRepCredential,
    ) -> Result<Option<&DRepRecordExtended>, &'static str> {
        let hist = self
            .historical_dreps
            .as_ref()
            .ok_or("Historical DRep storage is disabled by configuration.")?;
        match hist.get(credential) {
            Some(e) => {
                e.info.as_ref().ok_or("DRep info storage is disabled by configuration.").map(Some)
            }
            None => Ok(None),
        }
    }

    pub fn get_drep_delegators(
        &self,
        credential: &DRepCredential,
    ) -> Result<Option<&Vec<Credential>>, &'static str> {
        let hist = self
            .historical_dreps
            .as_ref()
            .ok_or("Historical DRep storage is disabled by configuration.")?;
        match hist.get(credential) {
            Some(e) => e
                .delegators
                .as_ref()
                .ok_or("DRep delegator storage is disabled by configuration.")
                .map(Some),
            None => Ok(None),
        }
    }

    pub fn get_drep_anchor(
        &self,
        credential: &DRepCredential,
    ) -> Result<Option<&Anchor>, &'static str> {
        let hist = self
            .historical_dreps
            .as_ref()
            .ok_or("Historical DRep storage is disabled by configuration.")?;
        match hist.get(credential) {
            Some(e) => {
                e.metadata.as_ref().ok_or("DRep metadata not found").map(|m| m.anchor.as_ref())
            }
            None => Ok(None),
        }
    }

    pub fn get_drep_updates(
        &self,
        credential: &DRepCredential,
    ) -> Result<Option<&Vec<DRepUpdateEvent>>, &'static str> {
        let hist = self
            .historical_dreps
            .as_ref()
            .ok_or("Historical DRep storage is disabled by configuration.")?;
        match hist.get(credential) {
            Some(e) => e
                .updates
                .as_ref()
                .ok_or("DRep updates storage is disabled by configuration.")
                .map(Some),
            None => Ok(None),
        }
    }

    pub fn get_drep_votes(
        &self,
        credential: &DRepCredential,
    ) -> Result<Option<&Vec<VoteRecord>>, &'static str> {
        let hist = self
            .historical_dreps
            .as_ref()
            .ok_or("Historical DRep storage is disabled by configuration.")?;
        match hist.get(credential) {
            Some(e) => {
                e.votes.as_ref().ok_or("DRep votes storage is disabled by configuration.").map(Some)
            }
            None => Ok(None),
        }
    }

    pub fn tick(&self) -> Result<()> {
        self.log_stats();
        Ok(())
    }

    pub async fn process_certificates(
        &mut self,
        context: Arc<Context<Message>>,
        tx_cert_msg: &TxCertificatesMessage,
    ) -> Result<()> {
        let mut batched_delegators = Vec::new();
        let store_delegators = self.config.store_delegators;

        for tx_cert in &tx_cert_msg.certificates {
            if store_delegators {
                if let Some((cred, drep)) = Self::extract_delegation_fields(tx_cert) {
                    batched_delegators.push((cred, drep));
                    continue;
                }
            }

            if let Err(e) = self.process_one_cert(tx_cert) {
                tracing::error!("Error processing tx_cert: {e}");
            }
        }

        // Batched delegations to reduce redundant queries to accounts_state
        if store_delegators && !batched_delegators.is_empty() {
            if let Err(e) = self.update_delegators(&context, batched_delegators).await {
                tracing::error!("Error processing batched delegators: {e}");
            }
        }

        Ok(())
    }

    pub async fn process_votes(
        &mut self,
        governance_msg: &GovernanceProceduresMessage,
    ) -> Result<()> {
        if !self.config.store_votes {
            return Ok(());
        }

        let Some(hist_map) = self.historical_dreps.as_mut() else {
            return Ok(());
        };

        for (tx_hash, voting_procedures) in &governance_msg.voting_procedures {
            for (voter, single_votes) in &voting_procedures.votes {
                let drep_cred = match voter {
                    Voter::DRepKey(k) => DRepCredential::AddrKeyHash(k.to_vec()),
                    Voter::DRepScript(s) => DRepCredential::ScriptHash(s.to_vec()),
                    _ => continue,
                };

                let cfg = self.config.clone();
                let entry = hist_map
                    .entry(drep_cred)
                    .or_insert_with(|| HistoricalDRepState::from_config(&cfg));

                // ensure votes vec exists if we created from a config that didn’t set it before
                if entry.votes.is_none() {
                    entry.votes = Some(Vec::new());
                }
                let votes = entry.votes.as_mut().unwrap();

                for (_gaid, vp) in &single_votes.voting_procedures {
                    votes.push(VoteRecord {
                        tx_hash: tx_hash.clone(),
                        cert_index: vp.vote_index,
                        vote: vp.vote.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn update_drep_expirations(
        &mut self,
        current_epoch: u64,
        expired_epoch_param: u32,
    ) -> Result<()> {
        let expired_offset = expired_epoch_param as u64;

        // If historical storage isn’t enabled, nothing to do.
        let Some(historical_dreps) = self.historical_dreps.as_mut() else {
            return Ok(());
        };

        for (_cred, drep_record) in historical_dreps.iter_mut() {
            if let Some(info) = drep_record.info.as_mut() {
                if let (Some(active_epoch), false) = (info.active_epoch, info.expired) {
                    if active_epoch + expired_offset <= current_epoch {
                        info.expired = true;
                    }
                }
            }
        }

        Ok(())
    }

    // Private helper functions
    fn log_stats(&self) {
        info!(count = self.dreps.len());
    }

    fn process_one_cert(&mut self, tx_cert: &TxCertificate) -> Result<bool> {
        match tx_cert {
            TxCertificate::DRepRegistration(reg) => {
                let new = match self.dreps.get_mut(&reg.reg.credential) {
                    Some(drep) => {
                        if reg.reg.deposit != 0 {
                            return Err(anyhow!(
                                "DRep registration {:?}: replacement requires deposit = 0, got {}",
                                reg.reg.credential,
                                reg.reg.deposit
                            ));
                        }
                        drep.anchor = reg.reg.anchor.clone();
                        false
                    }
                    None => {
                        self.dreps.insert(
                            reg.reg.credential.clone(),
                            DRepRecord::new(reg.reg.deposit, reg.reg.anchor.clone()),
                        );
                        true
                    }
                };

                if self.historical_dreps.is_some() {
                    if let Err(err) = self.update_historical(&reg.reg.credential, true, |entry| {
                        if let Some(info) = entry.info.as_mut() {
                            info.deposit = reg.reg.deposit;
                            info.expired = false;
                            info.retired = false;
                            info.active_epoch = Some(reg.epoch);
                            info.last_active_epoch = reg.epoch;
                        }
                        if let Some(updates) = entry.updates.as_mut() {
                            updates.push(DRepUpdateEvent {
                                tx_hash: reg.tx_hash.clone(),
                                cert_index: reg.cert_index,
                                action: DRepActionUpdate::Registered,
                            });
                        }
                        if let Some(metadata) = entry.metadata.as_mut() {
                            metadata.anchor = reg.reg.anchor.clone();
                        }
                    }) {
                        return Err(anyhow!("Failed to update DRep on registration: {err}"));
                    }
                }

                Ok(new)
            }

            TxCertificate::DRepDeregistration(reg) => {
                // Update live state
                if self.dreps.remove(&reg.reg.credential).is_none() {
                    return Err(anyhow!(
                        "DRep deregistration {:?}: credential not found",
                        reg.reg.credential
                    ));
                }

                // Update history if enabled
                if let Err(err) = self.update_historical(&reg.reg.credential, false, |entry| {
                    if let Some(info) = entry.info.as_mut() {
                        info.deposit = 0;
                        info.expired = false;
                        info.retired = true;
                        info.active_epoch = None;
                        info.last_active_epoch = reg.epoch;
                    }
                    if let Some(updates) = entry.updates.as_mut() {
                        updates.push(DRepUpdateEvent {
                            tx_hash: reg.tx_hash.clone(),
                            cert_index: reg.cert_index,
                            action: DRepActionUpdate::Deregistered,
                        });
                    }
                }) {
                    return Err(anyhow!("Failed to update DRep on deregistration: {err}"));
                }

                Ok(true)
            }

            TxCertificate::DRepUpdate(reg) => {
                // Update live state
                let drep = self.dreps.get_mut(&reg.reg.credential).ok_or_else(|| {
                    anyhow!("DRep update {:?}: credential not found", reg.reg.credential)
                })?;
                drep.anchor = reg.reg.anchor.clone();

                // Update history if enabled
                if let Err(err) = self.update_historical(&reg.reg.credential, false, |entry| {
                    if let Some(info) = entry.info.as_mut() {
                        info.expired = false;
                        info.retired = false;
                        info.last_active_epoch = reg.epoch;
                    }
                    if let Some(updates) = entry.updates.as_mut() {
                        updates.push(DRepUpdateEvent {
                            tx_hash: reg.tx_hash.clone(),
                            cert_index: reg.cert_index,
                            action: DRepActionUpdate::Updated,
                        });
                    }
                    if let Some(anchor) = &reg.reg.anchor {
                        if let Some(metadata) = entry.metadata.as_mut() {
                            metadata.anchor = Some(anchor.clone());
                        }
                    }
                }) {
                    tracing::warn!("Historical update failed: {err}");
                }

                Ok(false)
            }

            _ => Ok(false),
        }
    }

    fn update_historical<F>(
        &mut self,
        credential: &DRepCredential,
        create_if_missing: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut HistoricalDRepState),
    {
        let hist = self
            .historical_dreps
            .as_mut()
            .ok_or_else(|| anyhow!("No historical map configured"))?;

        if create_if_missing {
            let cfg = self.config.clone();
            let entry = hist
                .entry(credential.clone())
                .or_insert_with(|| HistoricalDRepState::from_config(&cfg));
            f(entry);
        } else if let Some(entry) = hist.get_mut(credential) {
            f(entry);
        } else {
            warn!("Tried to update unknown DRep credential: {:?}", credential);
        }

        Ok(())
    }

    async fn update_delegators(
        &mut self,
        context: &Arc<Context<Message>>,
        delegators: Vec<(&StakeCredential, &DRepChoice)>,
    ) -> Result<()> {
        let stake_keys: Vec<_> = delegators.iter().map(|(sc, _)| sc.get_hash()).collect();
        let stake_key_to_input: HashMap<_, _> = delegators
            .iter()
            .zip(&stake_keys)
            .map(|((sc, drep), key)| (key.clone(), (*sc, *drep)))
            .collect();

        let msg = Arc::new(Message::StateQuery(StateQuery::Accounts(
            AccountsStateQuery::GetAccountsDrepDelegationsMap { stake_keys },
        )));

        let accounts_query_topic = get_query_topic(context.clone(), DEFAULT_ACCOUNTS_QUERY_TOPIC);
        let response = context.message_bus.request(&accounts_query_topic, msg).await?;
        let message = Arc::try_unwrap(response).unwrap_or_else(|arc| (*arc).clone());

        // TODO: Ensure AccountsStateQueryResponse is for the correct block
        let result_map = match message {
            Message::StateQueryResponse(StateQueryResponse::Accounts(
                AccountsStateQueryResponse::AccountsDrepDelegationsMap(map),
            )) => map,
            _ => {
                return Err(anyhow!("Unexpected accounts-state response"));
            }
        };

        for (stake_key, old_drep_opt) in result_map {
            let (delegator, new_drep_choice) = match stake_key_to_input.get(&stake_key) {
                Some(pair) => *pair,
                None => continue,
            };

            let new_drep_cred = match drep_choice_to_credential(new_drep_choice) {
                Some(c) => c,
                None => continue,
            };

            if let Some(old_drep) = old_drep_opt {
                if let Some(old_drep_cred) = drep_choice_to_credential(&old_drep) {
                    if old_drep_cred != new_drep_cred {
                        self.update_historical(&old_drep_cred, false, |entry| {
                            if let Some(delegators) = entry.delegators.as_mut() {
                                delegators.retain(|s| s != delegator);
                            }
                        })?;
                    }
                }
            }

            // Add delegator to new DRep
            match self.update_historical(&new_drep_cred, true, |entry| {
                if let Some(delegators) = entry.delegators.as_mut() {
                    if !delegators.contains(delegator) {
                        delegators.push(delegator.clone());
                    }
                }
            }) {
                Ok(_) => {}
                Err(err) => return Err(anyhow!("Failed to update new delegator: {err}")),
            }
        }

        Ok(())
    }

    fn extract_delegation_fields(cert: &TxCertificate) -> Option<(&StakeCredential, &DRepChoice)> {
        match cert {
            TxCertificate::VoteDelegation(d) => Some((&d.credential, &d.drep)),
            TxCertificate::StakeAndVoteDelegation(d) => Some((&d.credential, &d.drep)),
            TxCertificate::StakeRegistrationAndVoteDelegation(d) => Some((&d.credential, &d.drep)),
            TxCertificate::StakeRegistrationAndStakeAndVoteDelegation(d) => {
                Some((&d.credential, &d.drep))
            }
            _ => None,
        }
    }
}

fn drep_choice_to_credential(choice: &DRepChoice) -> Option<DRepCredential> {
    match choice {
        DRepChoice::Key(k) => Some(DRepCredential::AddrKeyHash(k.clone())),
        DRepChoice::Script(k) => Some(DRepCredential::ScriptHash(k.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::state::{DRepRecord, DRepStorageConfig, State};
    use acropolis_common::{
        state_history::StateHistory, Anchor, BlockInfo, BlockStatus, Credential, DRepCredential,
        DRepDeregistration, DRepDeregistrationWithPos, DRepRegistration, DRepUpdate,
        DRepUpdateWithPos, Era, TxCertificate,
    };

    const CRED_1: [u8; 28] = [
        123, 222, 247, 170, 243, 201, 37, 233, 124, 164, 45, 54, 241, 25, 176, 70, 154, 18, 204,
        164, 161, 126, 207, 239, 198, 144, 3, 80,
    ];
    const CRED_2: [u8; 28] = [
        124, 223, 248, 171, 244, 202, 38, 234, 125, 165, 46, 55, 242, 26, 177, 71, 155, 19, 205,
        165, 162, 127, 208, 240, 199, 145, 4, 81,
    ];

    impl State {
        pub fn get_count(&self) -> usize {
            self.dreps.len()
        }
        pub fn get_drep(&self, credential: &DRepCredential) -> Option<&DRepRecord> {
            self.dreps.get(credential)
        }
    }

    #[test]
    fn test_drep_process_one_certificate() {
        let tx_cred = Credential::AddrKeyHash(CRED_1.to_vec());
        let tx_cert = TxCertificate::DRepRegistration(acropolis_common::DRepRegistrationWithPos {
            reg: DRepRegistration {
                credential: tx_cred.clone(),
                deposit: 500000000,
                anchor: None,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        let mut state = State::new(DRepStorageConfig::default());

        assert_eq!(state.process_one_cert(&tx_cert).unwrap(), true);
        assert_eq!(state.get_count(), 1);
        let tx_cert_record = DRepRecord {
            deposit: 500000000,
            anchor: None,
        };
        assert_eq!(
            state.get_drep(&tx_cred).unwrap().deposit,
            tx_cert_record.deposit
        );
    }

    #[test]
    fn test_drep_do_not_replace_existing_certificate() {
        let tx_cred = Credential::AddrKeyHash(CRED_1.to_vec());
        let tx_cert = TxCertificate::DRepRegistration(acropolis_common::DRepRegistrationWithPos {
            reg: DRepRegistration {
                credential: tx_cred.clone(),
                deposit: 500000000,
                anchor: None,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        let mut state = State::new(DRepStorageConfig::default());
        assert_eq!(state.process_one_cert(&tx_cert).unwrap(), true);

        let bad_tx_cert =
            TxCertificate::DRepRegistration(acropolis_common::DRepRegistrationWithPos {
                reg: DRepRegistration {
                    credential: tx_cred.clone(),
                    deposit: 600000000,
                    anchor: None,
                },
                tx_hash: [0u8; 32],
                cert_index: 1,
                epoch: 1,
            });
        assert!(state.process_one_cert(&bad_tx_cert).is_err());

        assert_eq!(state.get_count(), 1);
        let tx_cert_record = DRepRecord {
            deposit: 500000000,
            anchor: None,
        };
        assert_eq!(
            state.get_drep(&tx_cred).unwrap().deposit,
            tx_cert_record.deposit
        );
    }

    #[test]
    fn test_drep_update_certificate() {
        let tx_cred = Credential::AddrKeyHash(CRED_1.to_vec());
        let tx_cert = TxCertificate::DRepRegistration(acropolis_common::DRepRegistrationWithPos {
            reg: DRepRegistration {
                credential: tx_cred.clone(),
                deposit: 500000000,
                anchor: None,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        let mut state = State::new(DRepStorageConfig::default());
        assert_eq!(state.process_one_cert(&tx_cert).unwrap(), true);

        let anchor = Anchor {
            url: "https://poop.bike".into(),
            data_hash: vec![0x13, 0x37],
        };
        let update_anchor_tx_cert = TxCertificate::DRepUpdate(DRepUpdateWithPos {
            reg: DRepUpdate {
                credential: tx_cred.clone(),
                anchor: Some(anchor.clone()),
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });

        assert_eq!(
            state.process_one_cert(&update_anchor_tx_cert).unwrap(),
            false
        );

        assert_eq!(state.get_count(), 1);
        let tx_cert_record = DRepRecord {
            deposit: 500000000,
            anchor: Some(anchor),
        };
        assert_eq!(
            state.get_drep(&tx_cred).unwrap().anchor,
            tx_cert_record.anchor
        );
    }

    #[test]
    fn test_drep_do_not_update_nonexistent_certificate() {
        let tx_cred = Credential::AddrKeyHash(CRED_1.to_vec());
        let tx_cert = TxCertificate::DRepRegistration(acropolis_common::DRepRegistrationWithPos {
            reg: DRepRegistration {
                credential: tx_cred.clone(),
                deposit: 500000000,
                anchor: None,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        let mut state = State::new(DRepStorageConfig::default());
        assert_eq!(state.process_one_cert(&tx_cert).unwrap(), true);

        let anchor = Anchor {
            url: "https://poop.bike".into(),
            data_hash: vec![0x13, 0x37],
        };

        let update_anchor_tx_cert = TxCertificate::DRepUpdate(DRepUpdateWithPos {
            reg: DRepUpdate {
                credential: Credential::AddrKeyHash(CRED_2.to_vec()),
                anchor: Some(anchor.clone()),
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });

        assert!(state.process_one_cert(&update_anchor_tx_cert).is_err());

        assert_eq!(state.get_count(), 1);
        let tx_cert_record = DRepRecord {
            deposit: 500000000,
            anchor: Some(anchor),
        };
        assert_eq!(
            state.get_drep(&tx_cred).unwrap().deposit,
            tx_cert_record.deposit
        );
    }

    #[test]
    fn test_drep_deregister() {
        let tx_cred = Credential::AddrKeyHash(CRED_1.to_vec());
        let tx_cert = TxCertificate::DRepRegistration(acropolis_common::DRepRegistrationWithPos {
            reg: DRepRegistration {
                credential: tx_cred.clone(),
                deposit: 500000000,
                anchor: None,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        let mut state = State::new(DRepStorageConfig::default());
        assert_eq!(state.process_one_cert(&tx_cert).unwrap(), true);

        let unregister_tx_cert = TxCertificate::DRepDeregistration(DRepDeregistrationWithPos {
            reg: DRepDeregistration {
                credential: tx_cred.clone(),
                refund: 500000000,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        assert_eq!(state.process_one_cert(&unregister_tx_cert).unwrap(), true);
        assert_eq!(state.get_count(), 0);
        assert!(state.get_drep(&tx_cred).is_none());
    }

    #[test]
    fn test_drep_do_not_deregister_nonexistent_cert() {
        let tx_cred = Credential::AddrKeyHash(CRED_1.to_vec());
        let tx_cert = TxCertificate::DRepRegistration(acropolis_common::DRepRegistrationWithPos {
            reg: DRepRegistration {
                credential: tx_cred.clone(),
                deposit: 500000000,
                anchor: None,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        let mut state = State::new(DRepStorageConfig::default());
        assert_eq!(state.process_one_cert(&tx_cert).unwrap(), true);

        let unregister_tx_cert = TxCertificate::DRepDeregistration(DRepDeregistrationWithPos {
            reg: DRepDeregistration {
                credential: Credential::AddrKeyHash(CRED_2.to_vec()),
                refund: 500000000,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        assert!(state.process_one_cert(&unregister_tx_cert).is_err());
        assert_eq!(state.get_count(), 1);
        assert_eq!(state.get_drep(&tx_cred).unwrap().deposit, 500000000);
    }

    // Create a block for testing
    fn create_block(status: BlockStatus, slot: u64, number: u64) -> BlockInfo {
        BlockInfo {
            status,
            slot,
            number,
            hash: vec![],
            epoch: 99,
            new_epoch: false,
            era: Era::Byron,
        }
    }

    fn reg_with_pos(
        credential: &Credential,
        deposit: u64,
        anchor: Option<Anchor>,
        epoch: u64,
    ) -> TxCertificate {
        TxCertificate::DRepRegistration(acropolis_common::DRepRegistrationWithPos {
            reg: DRepRegistration {
                credential: credential.clone(),
                deposit,
                anchor,
            },
            tx_hash: [0u8; 32],
            cert_index: 0,
            epoch,
        })
    }

    fn dereg_with_pos(credential: &Credential, refund: u64, epoch: u64) -> TxCertificate {
        TxCertificate::DRepDeregistration(acropolis_common::DRepDeregistrationWithPos {
            reg: DRepDeregistration {
                credential: credential.clone(),
                refund,
            },
            tx_hash: [0u8; 32],
            cert_index: 0,
            epoch,
        })
    }

    fn update_with_pos(
        credential: &Credential,
        anchor: Option<Anchor>,
        epoch: u64,
    ) -> TxCertificate {
        TxCertificate::DRepUpdate(DRepUpdateWithPos {
            reg: DRepUpdate {
                credential: credential.clone(),
                anchor,
            },
            tx_hash: [0u8; 32],
            cert_index: 0,
            epoch,
        })
    }

    fn cred(n: u8) -> Credential {
        Credential::AddrKeyHash(vec![
            n, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27,
        ])
    }

    #[test]
    fn rollback_removes_future_registered_drep() {
        use acropolis_common::state_history::StateHistory;

        let mut history = StateHistory::<State>::new("DRepRollbackAdd");
        let cfg = DRepStorageConfig {
            store_info: true,
            store_delegators: false,
            store_metadata: true,
            store_updates: true,
            store_votes: false,
        };
        let c = Credential::AddrKeyHash(CRED_1.to_vec());

        // register drep at block 10
        let b10 = create_block(BlockStatus::Volatile, 10, 10);
        {
            let mut s = history.get_or_init_with(|| State::new(cfg));
            s.process_one_cert(&reg_with_pos(&c, 1_000_000, None, 10)).unwrap();
            history.commit(&b10, s);
        }
        assert!(history.get_current_state().get_drep(&c).is_some());

        // rollback to 10
        let rb = create_block(BlockStatus::RolledBack, 10, 10);
        let rolled = history.get_rolled_back_state(&rb);
        history.commit(&rb, rolled);

        // check that drep does not exist
        assert!(history.get_current_state().get_drep(&c).is_none());
    }

    #[test]
    fn rollback_reinstates_future_deregistered_drep() {
        let mut history = StateHistory::<State>::new("DRepStateTest");
        let c = cred(2);

        let cfg = DRepStorageConfig {
            store_info: true,
            store_delegators: false,
            store_metadata: true,
            store_updates: true,
            store_votes: false,
        };

        // register drep at block 10
        let b10 = create_block(BlockStatus::Volatile, 10, 10);
        {
            let mut s = history.get_or_init_with(|| State::new(cfg));
            s.process_one_cert(&reg_with_pos(&c, 5_000_000, None, 10)).unwrap();
            history.commit(&b10, s);
        }
        // Check that drep was registered
        {
            let s = history.get_current_state();
            assert_eq!(s.get_count(), 1);
            assert_eq!(s.get_drep(&c).unwrap().deposit, 5_000_000);
        }

        // deregister drep at block 11
        let b11 = create_block(BlockStatus::Volatile, 11, 11);
        {
            let mut s = history.get_current_state();
            s.process_one_cert(&dereg_with_pos(&c, 5_000_000, 11)).unwrap();
            history.commit(&b11, s);
        }
        // Check that drep was deregistered
        {
            let s = history.get_current_state();
            assert!(s.get_drep(&c).is_none());
        }

        // rollback to 11
        let rb_to_11 = create_block(BlockStatus::RolledBack, 11, 11);
        {
            let s = history.get_rolled_back_state(&rb_to_11);
            history.commit(&rb_to_11, s);
        }
        // Ensure drep is restored
        {
            let s = history.get_current_state();
            assert_eq!(s.get_count(), 1);
            assert_eq!(s.get_drep(&c).unwrap().deposit, 5_000_000);
        }
    }

    #[test]
    fn rollback_reverts_future_anchor_update() {
        use acropolis_common::state_history::StateHistory;

        let mut history = StateHistory::<State>::new("DRepRollbackModify");
        let cfg = DRepStorageConfig {
            store_info: true,
            store_delegators: false,
            store_metadata: true,
            store_updates: true,
            store_votes: false,
        };
        let c = Credential::AddrKeyHash(CRED_1.to_vec());
        let anchor_a = Anchor {
            url: "https://a.example".into(),
            data_hash: vec![1, 2, 3],
        };
        let anchor_b = Anchor {
            url: "https://b.example".into(),
            data_hash: vec![4, 5, 6],
        };

        // register drep at block 10
        let b10 = create_block(BlockStatus::Volatile, 10, 10);
        {
            let mut s = history.get_or_init_with(|| State::new(cfg));
            s.process_one_cert(&reg_with_pos(&c, 5_000_000, Some(anchor_a.clone()), 10)).unwrap();
            history.commit(&b10, s);
        }

        // check that anchor was set
        assert_eq!(
            history.get_current_state().get_drep(&c).unwrap().anchor.as_ref(),
            Some(&anchor_a)
        );

        // update drep registration at block 11
        let b11 = create_block(BlockStatus::Volatile, 11, 11);
        {
            let mut s = history.get_current_state();
            s.process_one_cert(&update_with_pos(&c, Some(anchor_b.clone()), 11)).unwrap();
            history.commit(&b11, s);
        }
        // Check that anchor was updated
        assert_eq!(
            history.get_current_state().get_drep(&c).unwrap().anchor.as_ref(),
            Some(&anchor_b)
        );

        // rollback to 11
        let rb = create_block(BlockStatus::RolledBack, 11, 11);
        let rolled = history.get_rolled_back_state(&rb);
        history.commit(&rb, rolled);

        // check that anchor was restored to previous
        assert_eq!(
            history.get_current_state().get_drep(&c).unwrap().anchor.as_ref(),
            Some(&anchor_a)
        );
    }

    #[test]
    fn update_drep_expirations_marks_expired_when_past_offset() {
        let mut history = StateHistory::<State>::new("DRepExpirations");
        let cfg = DRepStorageConfig {
            store_info: true,
            store_delegators: false,
            store_metadata: true,
            store_updates: true,
            store_votes: false,
        };
        let c = Credential::AddrKeyHash(CRED_1.to_vec());

        // register drep with active_epoch 5
        let b1 = create_block(BlockStatus::Volatile, 5, 5);
        {
            let mut s = history.get_or_init_with(|| State::new(cfg));
            s.process_one_cert(&reg_with_pos(&c, 1_000_000, None, 5)).unwrap();
            history.commit(&b1, s);
        }

        // create a block past retired param (3 epochs later for this test)
        let b2 = create_block(BlockStatus::Volatile, 8, 8);
        {
            let mut s = history.get_current_state();
            s.update_drep_expirations(10, 3).unwrap();
            history.commit(&b2, s);
        }

        // check that drep is marked as expired
        let s = history.get_current_state();
        assert!(s.get_drep_info(&c).unwrap().unwrap().expired);
    }
}
