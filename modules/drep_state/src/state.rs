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
    /// Returns true if any of the fields are enabled
    fn store_any(&self) -> bool {
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
        let enable_hist = config.store_any();

        Self {
            config: config,
            dreps: std::collections::HashMap::new(),
            historical_dreps: if enable_hist {
                Some(HashMap::<DRepCredential, HistoricalDRepState>::new())
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
        let historical = self
            .historical_dreps
            .as_ref()
            .ok_or("DRep info storage is disabled by configuration.")?;

        let entry = match historical.get(credential) {
            Some(e) => e,
            None => return Ok(None),
        };

        match &entry.info {
            Some(info) => Ok(Some(info)),
            None => Err("DRep info storage is disabled by configuration."),
        }
    }

    pub fn get_drep_delegators(
        &self,
        credential: &DRepCredential,
    ) -> Result<Option<&Vec<Credential>>, &'static str> {
        let historical = self
            .historical_dreps
            .as_ref()
            .ok_or("DRep info storage is disabled by configuration.")?;

        let entry = match historical.get(credential) {
            Some(e) => e,
            None => return Ok(None),
        };

        match &entry.delegators {
            Some(vec) => Ok(Some(vec)),
            None => Err("DRep delegator storage is disabled by configuration."),
        }
    }

    pub fn get_drep_anchor(
        &self,
        credential: &DRepCredential,
    ) -> Result<Option<&Anchor>, &'static str> {
        let historical = self
            .historical_dreps
            .as_ref()
            .ok_or("DRep info storage is disabled by configuration.")?;

        let entry = match historical.get(credential) {
            Some(e) => e,
            None => return Ok(None),
        };

        let metadata = entry.metadata.as_ref().ok_or("DRep metadata not found")?;

        Ok(metadata.anchor.as_ref())
    }

    pub fn get_drep_updates(
        &self,
        credential: &DRepCredential,
    ) -> Result<Option<&Vec<DRepUpdateEvent>>, &'static str> {
        let historical = self
            .historical_dreps
            .as_ref()
            .ok_or("DRep updates storage is disabled by configuration.")?;

        let entry = match historical.get(credential) {
            Some(e) => e,
            None => return Ok(None),
        };

        match &entry.updates {
            Some(updates) => Ok(Some(updates)),
            None => Err("DRep updates storage is disabled by configuration."),
        }
    }

    pub fn get_drep_votes(
        &self,
        credential: &DRepCredential,
    ) -> Result<Option<&Vec<VoteRecord>>, &'static str> {
        let historical = self
            .historical_dreps
            .as_ref()
            .ok_or("DRep votes storage is disabled by configuration.")?;

        let entry = match historical.get(credential) {
            Some(e) => e,
            None => return Ok(None),
        };

        match &entry.votes {
            Some(votes) => Ok(Some(votes)),
            None => Err("DRep votes storage is disabled by configuration."),
        }
    }

    pub async fn log_stats(&self) {
        info!(count = self.dreps.len());
    }

    pub async fn tick(&self) -> Result<()> {
        self.log_stats().await;
        Ok(())
    }

    fn process_registration_certificate_sync(&mut self, tx_cert: &TxCertificate) -> Result<bool> {
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

                match self.update_historical(&reg.reg.credential, |entry| {
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
                    Ok(_) => {}
                    Err(err) => {
                        return Err(anyhow!("Failed to update DRep on registration: {err}"))
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
                if let Err(err) = self.update_historical_if_exists(&reg.reg.credential, |entry| {
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
                if let Err(err) = self.update_historical_if_exists(&reg.reg.credential, |entry| {
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

    pub async fn handle_certificates(
        &mut self,
        context: Arc<Context<Message>>,
        tx_cert_msg: &TxCertificatesMessage,
    ) -> Result<()> {
        let mut batched_delegators = Vec::new();

        for tx_cert in &tx_cert_msg.certificates {
            match tx_cert {
                TxCertificate::VoteDelegation(d) if self.config.store_delegators => {
                    batched_delegators.push((&d.credential, &d.drep));
                }
                TxCertificate::StakeAndVoteDelegation(d) if self.config.store_delegators => {
                    batched_delegators.push((&d.credential, &d.drep));
                }
                TxCertificate::StakeRegistrationAndVoteDelegation(d)
                    if self.config.store_delegators =>
                {
                    batched_delegators.push((&d.credential, &d.drep));
                }
                TxCertificate::StakeRegistrationAndStakeAndVoteDelegation(d)
                    if self.config.store_delegators =>
                {
                    batched_delegators.push((&d.credential, &d.drep));
                }
                _ => {
                    if let Err(e) = self.process_registration_certificate_sync(tx_cert) {
                        tracing::error!("Error processing tx_cert: {e}");
                    }
                }
            }
        }

        // Batched delegations to reduce prior delegated DRep queries to accounts_state
        if self.config.store_delegators && !batched_delegators.is_empty() {
            if let Err(e) = self.update_delegators(context.clone(), batched_delegators).await {
                tracing::error!("Error processing batched delegators: {e}");
            }
        }

        Ok(())
    }

    pub async fn handle_votes(
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

    fn update_historical<F>(&mut self, credential: &DRepCredential, f: F) -> Result<()>
    where
        F: FnOnce(&mut HistoricalDRepState),
    {
        let hist = self
            .historical_dreps
            .as_mut()
            .ok_or_else(|| anyhow!("No historical map configured"))?;

        let cfg = self.config.clone();

        let entry = hist
            .entry(credential.clone())
            .or_insert_with(|| HistoricalDRepState::from_config(&cfg));

        f(entry);
        Ok(())
    }

    fn update_historical_if_exists<F>(&mut self, credential: &DRepCredential, f: F) -> Result<()>
    where
        F: FnOnce(&mut HistoricalDRepState),
    {
        if let Some(historical) = self.historical_dreps.as_mut() {
            if let Some(entry) = historical.get_mut(credential) {
                f(entry);
            } else {
                warn!("Tried to update unknown DRep credential: {:?}", credential);
            }
        }
        Ok(())
    }

    pub async fn update_delegators(
        &mut self,
        context: Arc<Context<Message>>,
        delegators: Vec<(&StakeCredential, &DRepChoice)>,
    ) -> Result<()> {
        let stake_keys: Vec<_> = delegators.iter().map(|(sc, _)| sc.get_hash()).collect();
        let stake_key_to_input: HashMap<_, _> = delegators
            .iter()
            .zip(&stake_keys)
            .map(|((sc, drep), key)| (key.clone(), (*sc, *drep)))
            .collect();

        let msg = Arc::new(Message::StateQuery(StateQuery::Accounts(
            AccountsStateQuery::GetAccountsDrepDelegationsMap {
                stake_keys: stake_keys.clone(),
            },
        )));

        let accounts_query_topic = get_query_topic(context.clone(), DEFAULT_ACCOUNTS_QUERY_TOPIC);
        let response = context.message_bus.request(&accounts_query_topic, msg).await?;
        let message = Arc::try_unwrap(response).unwrap_or_else(|arc| (*arc).clone());

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
                        match self.update_historical_if_exists(&old_drep_cred, |entry| {
                            if let Some(delegators) = entry.delegators.as_mut() {
                                delegators.retain(|s| s != delegator);
                            }
                        }) {
                            Ok(_) => {}
                            Err(err) => {
                                return Err(anyhow!("Failed to update old delegator: {err}"))
                            }
                        }
                    }
                }
            }

            // Add delegator to new DRep
            match self.update_historical(&new_drep_cred, |entry| {
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
        Anchor, Credential, DRepCredential, DRepDeregistration, DRepDeregistrationWithPos,
        DRepRegistration, DRepUpdate, DRepUpdateWithPos, TxCertificate,
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

        assert_eq!(
            state.process_registration_certificate_sync(&tx_cert).unwrap(),
            true
        );
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
        assert_eq!(
            state.process_registration_certificate_sync(&tx_cert).unwrap(),
            true
        );

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
        assert!(state.process_registration_certificate_sync(&bad_tx_cert).is_err());

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
        assert_eq!(
            state.process_registration_certificate_sync(&tx_cert).unwrap(),
            true
        );

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
            state.process_registration_certificate_sync(&update_anchor_tx_cert).unwrap(),
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
        assert_eq!(
            state.process_registration_certificate_sync(&tx_cert).unwrap(),
            true
        );

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

        assert!(state.process_registration_certificate_sync(&update_anchor_tx_cert).is_err());

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
        assert_eq!(
            state.process_registration_certificate_sync(&tx_cert).unwrap(),
            true
        );

        let unregister_tx_cert = TxCertificate::DRepDeregistration(DRepDeregistrationWithPos {
            reg: DRepDeregistration {
                credential: tx_cred.clone(),
                refund: 500000000,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        assert_eq!(
            state.process_registration_certificate_sync(&unregister_tx_cert).unwrap(),
            true
        );
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
        assert_eq!(
            state.process_registration_certificate_sync(&tx_cert).unwrap(),
            true
        );

        let unregister_tx_cert = TxCertificate::DRepDeregistration(DRepDeregistrationWithPos {
            reg: DRepDeregistration {
                credential: Credential::AddrKeyHash(CRED_2.to_vec()),
                refund: 500000000,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        assert!(state.process_registration_certificate_sync(&unregister_tx_cert).is_err());
        assert_eq!(state.get_count(), 1);
        assert_eq!(state.get_drep(&tx_cred).unwrap().deposit, 500000000);
    }
}
