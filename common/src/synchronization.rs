use crate::{messages::Message, BlockInfo};
use caryatid_sdk::message_bus::Subscription;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::mpsc;

/// Enum of message kinds we care about for aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageType {
    Certs,
    Governance,
}

/// A container for all messages of a single block.
#[derive(Debug)]
pub struct AggregateBlock {
    pub block_info: BlockInfo,
    pub messages: HashMap<MessageType, Arc<Message>>,
}

impl AggregateBlock {
    pub fn new(block_info: BlockInfo) -> Self {
        Self {
            block_info,
            messages: HashMap::new(),
        }
    }

    pub fn insert(&mut self, kind: MessageType, msg: Arc<Message>) {
        self.messages.insert(kind, msg);
    }

    pub fn is_ready(&self, required: &[MessageType]) -> bool {
        required.iter().all(|k| self.messages.contains_key(k))
    }
}

/// Synchronizer that buffers blocks until all required messages are available.
pub struct MessageSynchronizer {
    pending: BTreeMap<u64, AggregateBlock>,
    required: Vec<MessageType>,
}

impl MessageSynchronizer {
    pub fn new(required: Vec<MessageType>) -> Self {
        Self {
            pending: BTreeMap::new(),
            required,
        }
    }

    pub fn insert(&mut self, bi: BlockInfo, kind: MessageType, msg: Arc<Message>) {
        let entry = self.pending.entry(bi.number).or_insert_with(|| AggregateBlock::new(bi));
        entry.insert(kind, msg);
    }

    /// Returns the next ready block (lowest number that has all required messages).
    pub fn take_ready(&mut self) -> Option<AggregateBlock> {
        if let Some((&k, agg)) = self.pending.iter().next() {
            if self.required.iter().all(|kind| agg.messages.contains_key(kind)) {
                return self.pending.remove(&k);
            }
        }
        None
    }

    /// Returns the block numbers that are waiting for required messages.
    pub fn waiting_for(&self) -> Vec<u64> {
        self.pending
            .iter()
            .filter(|(_, agg)| !agg.is_ready(&self.required))
            .map(|(bn, _)| *bn)
            .collect()
    }
}

/// Spawns async forwarders for each subscription and returns their receivers.
/// Each forwarder runs in its own task and pushes messages into its channel.
pub fn spawn_forwarders(
    subs: Vec<Option<Box<dyn Subscription<Message> + Send>>>,
) -> Vec<mpsc::UnboundedReceiver<Arc<Message>>> {
    let mut receivers = Vec::new();

    for mut sub in subs.into_iter().flatten() {
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Ok((_, msg)) = sub.read().await {
                if tx.send(msg).is_err() {
                    break;
                }
            }
        });

        receivers.push(rx);
    }

    receivers
}
