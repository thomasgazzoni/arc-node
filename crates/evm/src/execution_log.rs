//! Transaction execution events emitted after their state has been committed.

use alloy_primitives::{Log, B256};

/// One transaction executed while validating an Engine API payload.
#[derive(Debug, Clone)]
pub struct ExecutionLogEvent {
    pub block_number: u64,
    pub block_hash: B256,
    pub parent_hash: B256,
    pub block_timestamp: u64,
    pub transaction_index: u64,
    pub transaction_hash: B256,
    pub gas_used: u64,
    pub success: bool,
    pub logs: Vec<Log>,
}

/// Nonblocking destination for transaction execution events.
pub trait ExecutionLogSink: std::fmt::Debug + Send + Sync + 'static {
    /// Whether any consumer currently needs events. A false result allows the
    /// executor to skip per-transaction event allocation entirely.
    fn wants_events(&self) -> bool {
        true
    }

    fn try_send(&self, event: ExecutionLogEvent);
}
