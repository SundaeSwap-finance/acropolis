use crate::state::DRepDelegationDistribution;
use acropolis_common::KeyHash;
use std::collections::BTreeMap;

/// Seperate DistributionHistory to avoid duplicating history in StateHistory used for rollbacks
#[derive(Default, Debug, Clone)]
pub struct DistributionHistory {
    /// DRep Delegation Distribution per epoch
    pub drdd: BTreeMap<u64, DRepDelegationDistribution>,

    /// Stake Pool Delegation Distribution per epoch
    pub spdd: BTreeMap<u64, BTreeMap<KeyHash, u64>>,
}

impl DistributionHistory {
    pub fn insert_drdd(&mut self, epoch: u64, drdd: DRepDelegationDistribution) {
        self.drdd.insert(epoch, drdd);
    }

    pub fn insert_spdd(&mut self, epoch: u64, spdd: BTreeMap<KeyHash, u64>) {
        self.spdd.insert(epoch, spdd);
    }

    pub fn get_drdd(&self, epoch: u64) -> Option<&DRepDelegationDistribution> {
        self.drdd.get(&epoch)
    }

    pub fn get_spdd(&self, epoch: u64) -> Option<&BTreeMap<KeyHash, u64>> {
        self.spdd.get(&epoch)
    }
}
