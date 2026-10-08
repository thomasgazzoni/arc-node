use super::*;
use alloy_primitives::{Address, Bytes, Log, LogData};
use tokio::io::AsyncReadExt;

fn sample_event() -> ExecutionLogEvent {
    ExecutionLogEvent {
        block_number: 7,
        block_hash: B256::repeat_byte(1),
        parent_hash: B256::repeat_byte(2),
        block_timestamp: 99,
        transaction_index: 0,
        transaction_hash: B256::repeat_byte(3),
        gas_used: 42,
        success: true,
        logs: vec![Log {
            address: Address::repeat_byte(4),
            data: LogData::new_unchecked(vec![B256::repeat_byte(5)], Bytes::from_static(b"xyz")),
        }],
    }
}

#[test]
fn join_barrier_rejects_queued_frames_and_accepts_future_frames() {
    assert!(!is_after_join_barrier(4, 4));
    assert!(!is_after_join_barrier(3, 4));
    assert!(is_after_join_barrier(5, 4));
}

#[test]
fn v3_encoder_matches_protocol_offsets_and_zero_state_id() {
    let event = sample_event();
    let frame = encode_v3(&event).unwrap();
    let expected_hex = concat!(
        "000000e40301010000000000000000070000000000000000000000000000002a",
        "0303030303030303030303030303030303030303030303030303030303030303",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000063010101010101010101010101010101010101010101010101",
        "0101010101010101020202020202020202020202020202020202020202020202",
        "0202020202020202000000010404040404040404040404040404040404040404",
        "01000000030505050505050505050505050505050505050505050505050505050505050505",
        "78797a"
    );
    let expected: Vec<u8> = expected_hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    assert_eq!(
        frame, expected,
        "wire bytes must match the shared V3 fixture"
    );
    assert_eq!(
        u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize,
        frame.len() - 4
    );
    assert_eq!(&frame[4..8], &[3, 1, 1, 0]);
    assert_eq!(&frame[64..96], &[0; 32]);
    assert_eq!(&frame[96..104], &99u64.to_be_bytes());
    assert_eq!(&frame[104..136], event.block_hash.as_slice());
    assert_eq!(&frame[136..168], event.parent_hash.as_slice());
}

#[test]
fn bounded_sink_drops_on_queue_exhaustion_and_releases_budget() {
    let (tx, mut rx) = mpsc::channel(1);
    let budget = Arc::new(Semaphore::new(64));
    let sink = ChannelSink {
        tx,
        budget: budget.clone(),
        clients: Arc::new(AtomicUsize::new(1)),
        sequence: Arc::new(AtomicU64::new(0)),
    };

    sink.try_send(sample_event());
    sink.try_send(sample_event());
    assert_eq!(rx.len(), 1);
    assert!(budget.available_permits() < 64);
    drop(rx.try_recv().expect("queued frame"));
    assert_eq!(budget.available_permits(), 64);
}

#[tokio::test]
async fn reconnect_receives_future_frames_only_and_fragmented_reads_work() {
    let path = std::env::temp_dir().join(format!(
        "arc-execution-log-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let sink = start(path.clone(), 0o600).await.unwrap();
    assert!(!sink.wants_events());
    sink.try_send(sample_event());

    let mut client = UnixStream::connect(&path).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(sink.wants_events());
    let mut future_event = sample_event();
    future_event.transaction_hash = B256::repeat_byte(9);
    sink.try_send(future_event);

    let mut prefix = [0u8; 4];
    for byte in &mut prefix {
        client.read_exact(std::slice::from_mut(byte)).await.unwrap();
    }
    let body_len = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0; body_len];
    for byte in &mut body {
        client.read_exact(std::slice::from_mut(byte)).await.unwrap();
    }
    assert_eq!(&body[28..60], B256::repeat_byte(9).as_slice());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), client.read_u8())
            .await
            .is_err()
    );

    drop(client);
    drop(sink);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!path.exists());
}
