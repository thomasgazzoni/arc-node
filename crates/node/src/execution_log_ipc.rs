//! Bounded V3 transaction execution stream over a Unix socket.

use std::{
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use alloy_primitives::B256;
use arc_evm::execution_log::{ExecutionLogEvent, ExecutionLogSink};
use tokio::{
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
};

const QUEUE_CAPACITY: usize = 1024;
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
const MAX_CLIENTS: usize = 8;
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_millis(100);
const QUEUE_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const QUEUE_BUDGET_UNIT: usize = 1024;

struct QueuedEvent {
    sequence: u64,
    event: ExecutionLogEvent,
    _budget: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct ChannelSink {
    tx: mpsc::Sender<QueuedEvent>,
    budget: Arc<Semaphore>,
    clients: Arc<AtomicUsize>,
    sequence: Arc<AtomicU64>,
}

impl ExecutionLogSink for ChannelSink {
    fn wants_events(&self) -> bool {
        self.clients.load(Ordering::Relaxed) > 0
    }

    fn try_send(&self, event: ExecutionLogEvent) {
        let size = estimated_size(&event);
        let units = size.div_ceil(QUEUE_BUDGET_UNIT);
        let Ok(units) = u32::try_from(units) else {
            metrics::counter!("arc_execution_log_dropped_frames_total").increment(1);
            return;
        };
        let Ok(permit) = Arc::clone(&self.budget).try_acquire_many_owned(units) else {
            metrics::counter!("arc_execution_log_dropped_frames_total").increment(1);
            return;
        };
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        if self
            .tx
            .try_send(QueuedEvent {
                sequence,
                event,
                _budget: permit,
            })
            .is_err()
        {
            metrics::counter!("arc_execution_log_dropped_frames_total").increment(1);
        }
    }
}

struct SocketCleanup(PathBuf);

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Bind the socket and start its bounded background fanout task.
pub async fn start(path: PathBuf, permissions: u32) -> eyre::Result<Arc<dyn ExecutionLogSink>> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::remove_file(&path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let listener = UnixListener::bind(&path)?;
    tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(permissions)).await?;

    let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
    let clients = Arc::new(AtomicUsize::new(0));
    let sequence = Arc::new(AtomicU64::new(0));
    tokio::spawn(run_server(
        listener,
        rx,
        SocketCleanup(path),
        clients.clone(),
        sequence.clone(),
    ));
    Ok(Arc::new(ChannelSink {
        tx,
        budget: Arc::new(Semaphore::new(QUEUE_BUDGET_BYTES / QUEUE_BUDGET_UNIT)),
        clients,
        sequence,
    }))
}

fn estimated_size(event: &ExecutionLogEvent) -> usize {
    event.logs.iter().fold(168usize, |size, log| {
        size.saturating_add(25)
            .saturating_add(log.data.topics().len().saturating_mul(32))
            .saturating_add(log.data.data.len())
    })
}

fn is_after_join_barrier(event_sequence: u64, joined_after_sequence: u64) -> bool {
    event_sequence > joined_after_sequence
}

async fn run_server(
    listener: UnixListener,
    mut rx: mpsc::Receiver<QueuedEvent>,
    _cleanup: SocketCleanup,
    client_count: Arc<AtomicUsize>,
    latest_sequence: Arc<AtomicU64>,
) {
    struct Client {
        stream: UnixStream,
        joined_after_sequence: u64,
    }

    let mut clients: Vec<Client> = Vec::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) if clients.len() < MAX_CLIENTS => {
                    let joined_after_sequence = latest_sequence.load(Ordering::Acquire);
                    clients.push(Client { stream, joined_after_sequence });
                    client_count.fetch_add(1, Ordering::Relaxed);
                    metrics::gauge!("arc_execution_log_clients").set(clients.len() as f64);
                }
                Ok((_stream, _)) => metrics::counter!("arc_execution_log_rejected_clients_total").increment(1),
                Err(error) => tracing::warn!(%error, "execution log socket accept failed"),
            },
            event = rx.recv() => {
                let Some(event) = event else { break };
                let Some(frame) = encode_v3(&event.event) else {
                    metrics::counter!("arc_execution_log_encode_errors_total").increment(1);
                    continue;
                };
                let mut index = 0;
                while index < clients.len() {
                    if !is_after_join_barrier(event.sequence, clients[index].joined_after_sequence) {
                        index += 1;
                        continue;
                    }
                    let result = tokio::time::timeout(CLIENT_WRITE_TIMEOUT, clients[index].stream.write_all(&frame)).await;
                    if !matches!(result, Ok(Ok(()))) {
                        metrics::counter!("arc_execution_log_write_errors_total").increment(1);
                        clients.swap_remove(index);
                        client_count.fetch_sub(1, Ordering::Relaxed);
                        metrics::gauge!("arc_execution_log_clients").set(clients.len() as f64);
                    } else {
                        index += 1;
                    }
                }
            }
        }
    }
}

fn encode_v3(event: &ExecutionLogEvent) -> Option<Vec<u8>> {
    let mut body = Vec::with_capacity(
        168 + event
            .logs
            .iter()
            .map(|log| 25 + log.data.data.len() + 32 * log.data.topics().len())
            .sum::<usize>(),
    );
    body.extend([3, 1, u8::from(event.success), 0]);
    body.extend(event.block_number.to_be_bytes());
    body.extend(event.transaction_index.to_be_bytes());
    body.extend(event.gas_used.to_be_bytes());
    body.extend(event.transaction_hash.as_slice());
    body.extend(B256::ZERO.as_slice());
    body.extend(event.block_timestamp.to_be_bytes());
    body.extend(event.block_hash.as_slice());
    body.extend(event.parent_hash.as_slice());
    body.extend(u32::try_from(event.logs.len()).ok()?.to_be_bytes());
    for log in &event.logs {
        body.extend(log.address.as_slice());
        body.push(u8::try_from(log.data.topics().len()).ok()?);
        body.extend(u32::try_from(log.data.data.len()).ok()?.to_be_bytes());
        for topic in log.data.topics() {
            body.extend(topic.as_slice());
        }
        body.extend(log.data.data.as_ref());
    }
    if body.len() > MAX_FRAME_SIZE {
        return None;
    }
    let mut frame = Vec::with_capacity(body.len() + 4);
    frame.extend(u32::try_from(body.len()).ok()?.to_be_bytes());
    frame.extend(body);
    Some(frame)
}

#[cfg(test)]
#[path = "execution_log_ipc_tests.rs"]
mod tests;
