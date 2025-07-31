//! Acropolis DRepState: State storage

use acropolis_common::{
    messages::TxCertificatesMessage,
    queries::governance::{DRepActionUpdate, DRepMetadata, DRepUpdateEvent},
    Anchor, DRepCredential, Lovelace, StakeAddress, TxCertificate, Vote,
};
use anyhow::{anyhow, Result};
use serde_with::serde_as;
use std::collections::HashMap;
use tracing::{error, info};

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DRepRecord {
    pub deposit: Lovelace,
    pub anchor: Option<Anchor>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct HistoricalDRepState {
    // Populated from the reg field in:
    // - DRepRegistration
    // - DRepDeregistration
    // - DRepUpdate
    pub info: Option<DRepRecordExtended>,

    // Populated from the same certificates as info
    pub updates: Option<Vec<DRepUpdateEvent>>,

    // Populated from the reg.anchor field in DRep certificates
    pub metadata: Option<DRepMetadata>,

    // Populated from the drep and credential fields in:
    // - VoteDelegation
    // - StakeAndVoteDelegation
    // - StakeRegistrationAndVoteDelegation
    // - StakeRegistrationAndStakeAndVoteDelegation
    pub delegators: Option<Vec<StakeAddress>>,

    // Populated from voting_procedures in GovernanceProceduresMessage
    pub votes: Option<Vec<VoteRecord>>,
}

impl HistoricalDRepState {
    pub fn with_config(cfg: &DRepStorageConfig) -> Self {
        Self {
            info: cfg.store_info.then_some(DRepRecordExtended::default()),
            updates: cfg.store_updates.then_some(Vec::new()),
            metadata: cfg.store_metadata.then_some(DRepMetadata::default()),
            delegators: cfg.store_delegators.then_some(Vec::new()),
            votes: cfg.store_votes.then_some(Vec::new()),
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

#[derive(serde::Serialize, serde::Deserialize)]
pub struct VoteRecord {
    pub tx_hash: String,
    pub cert_index: u64,
    pub vote: Vote,
}

impl DRepRecord {
    pub fn new(deposit: Lovelace, anchor: Option<Anchor>) -> Self {
        Self { deposit, anchor }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DRepStorageConfig {
    pub store_info: bool,
    pub store_delegators: bool,
    pub store_metadata: bool,
    pub store_updates: bool,
    pub store_votes: bool,
}

impl DRepStorageConfig {
    pub fn enabled(&self) -> bool {
        self.store_info
            || self.store_delegators
            || self.store_metadata
            || self.store_updates
            || self.store_votes
    }
}

pub struct State {
    config: DRepStorageConfig,
    dreps: HashMap<DRepCredential, DRepRecord>,
    historical_dreps: Option<HashMap<DRepCredential, HistoricalDRepState>>,
}

impl State {
    pub fn new(storage_config: DRepStorageConfig) -> Self {
        Self {
            config: storage_config.clone(),
            dreps: HashMap::new(),
            historical_dreps: storage_config.enabled().then(HashMap::new),
        }
    }

    #[allow(dead_code)]
    pub fn get_count(&self) -> usize {
        self.dreps.len()
    }

    #[allow(dead_code)]
    pub fn get_drep(&self, credential: &DRepCredential) -> Option<&DRepRecord> {
        self.dreps.get(credential)
    }

    pub fn get_historical_drep(&self, credential: &DRepCredential) -> Option<&HistoricalDRepState> {
        self.historical_dreps.as_ref().and_then(|map| map.get(credential))
    }

    pub fn active_drep_list(&self) -> Vec<(DRepCredential, Lovelace)> {
        self.dreps.iter().map(|(d, r)| (d.clone(), r.deposit)).collect()
    }

    pub fn list(&self) -> Vec<DRepCredential> {
        self.dreps.keys().cloned().collect()
    }

    async fn log_stats(&self) {
        info!(count = self.dreps.len());
    }

    pub async fn tick(&self) -> Result<()> {
        self.log_stats().await;
        Ok(())
    }

    fn process_one_certificate(&mut self, tx_cert: &TxCertificate) -> Result<bool> {
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

                self.update_historical(&reg.reg.credential, |entry| {
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
                });

                Ok(new)
            }

            TxCertificate::DRepDeregistration(reg) => {
                let result = if self.dreps.remove(&reg.reg.credential).is_none() {
                    Err(anyhow!(
                        "DRep deregistration {:?}: credential not found",
                        reg.reg.credential
                    ))
                } else {
                    Ok(true)
                };

                self.update_historical_if_exists(&reg.reg.credential, |entry| {
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
                });

                result
            }

            TxCertificate::DRepUpdate(reg) => {
                let result = match self.dreps.get_mut(&reg.reg.credential) {
                    Some(drep) => {
                        drep.anchor = reg.reg.anchor.clone();
                        Ok(false)
                    }
                    None => Err(anyhow!(
                        "DRep update {:?}: credential not found",
                        reg.reg.credential
                    )),
                };

                self.update_historical_if_exists(&reg.reg.credential, |entry| {
                    if let Some(info) = entry.info.as_mut() {
                        info.expired = false;
                        info.retired = false;
                        info.last_active_epoch = reg.epoch;
                    }
                    if let Some(updates) = entry.updates.as_mut() {
                        updates.push(DRepUpdateEvent {
                            tx_hash: reg.tx_hash.clone(),
                            cert_index: reg.cert_index,
                            action: DRepActionUpdate::Registered,
                        });
                    }
                    if let Some(anchor) = &reg.reg.anchor {
                        if let Some(metadata) = entry.metadata.as_mut() {
                            metadata.anchor = Some(anchor.clone());
                        }
                    }
                });

                result
            }

            _ => Ok(false),
        }
    }

    pub async fn handle(&mut self, tx_cert_msg: &TxCertificatesMessage) -> Result<()> {
        for tx_cert in tx_cert_msg.certificates.iter() {
            if let Err(e) = self.process_one_certificate(tx_cert) {
                tracing::error!("Error processing tx_cert {}", e);
            }
        }

        Ok(())
    }

    fn update_historical<F>(&mut self, credential: &DRepCredential, f: F)
    where
        F: FnOnce(&mut HistoricalDRepState),
    {
        if let Some(historical) = self.historical_dreps.as_mut() {
            let entry = historical
                .entry(credential.clone())
                .or_insert_with(|| HistoricalDRepState::with_config(&self.config));
            f(entry);
        }
    }

    fn update_historical_if_exists<F>(&mut self, credential: &DRepCredential, f: F)
    where
        F: FnOnce(&mut HistoricalDRepState),
    {
        if let Some(historical) = self.historical_dreps.as_mut() {
            if let Some(entry) = historical.get_mut(credential) {
                f(entry);
            } else {
                error!("Tried to update unknown DRep credential: {:?}", credential);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::state::{DRepRecord, DRepStorageConfig, State};
    use acropolis_common::{
        Anchor, Credential, DRepDeregistration, DRepDeregistrationWithPos, DRepRegistration,
        DRepUpdate, DRepUpdateWithPos, TxCertificate,
    };

    const CRED_1: [u8; 28] = [
        123, 222, 247, 170, 243, 201, 37, 233, 124, 164, 45, 54, 241, 25, 176, 70, 154, 18, 204,
        164, 161, 126, 207, 239, 198, 144, 3, 80,
    ];
    const CRED_2: [u8; 28] = [
        124, 223, 248, 171, 244, 202, 38, 234, 125, 165, 46, 55, 242, 26, 177, 71, 155, 19, 205,
        165, 162, 127, 208, 240, 199, 145, 4, 81,
    ];

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
        assert_eq!(state.process_one_certificate(&tx_cert).unwrap(), true);
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
        assert_eq!(state.process_one_certificate(&tx_cert).unwrap(), true);

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
        assert!(state.process_one_certificate(&bad_tx_cert).is_err());

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
        assert_eq!(state.process_one_certificate(&tx_cert).unwrap(), true);

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
            state.process_one_certificate(&update_anchor_tx_cert).unwrap(),
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
        assert_eq!(state.process_one_certificate(&tx_cert).unwrap(), true);

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

        assert!(state.process_one_certificate(&update_anchor_tx_cert).is_err());

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
        assert_eq!(state.process_one_certificate(&tx_cert).unwrap(), true);

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
            state.process_one_certificate(&unregister_tx_cert).unwrap(),
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
        assert_eq!(state.process_one_certificate(&tx_cert).unwrap(), true);

        let unregister_tx_cert = TxCertificate::DRepDeregistration(DRepDeregistrationWithPos {
            reg: DRepDeregistration {
                credential: Credential::AddrKeyHash(CRED_2.to_vec()),
                refund: 500000000,
            },
            tx_hash: [0u8; 32],
            cert_index: 1,
            epoch: 1,
        });
        assert!(state.process_one_certificate(&unregister_tx_cert).is_err());
        assert_eq!(state.get_count(), 1);
        assert_eq!(state.get_drep(&tx_cred).unwrap().deposit, 500000000);
    }
}
