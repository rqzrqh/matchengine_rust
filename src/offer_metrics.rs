use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BUCKET_WIDTH: Duration = Duration::from_millis(10);
const BUCKET_COUNT: usize = 200;

fn duration_nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn atomic_add_duration(target: &AtomicU64, duration: Duration) {
    target.fetch_add(duration_nanos_u64(duration), Ordering::Relaxed);
}

fn record_atomic_max(target: &AtomicU64, value: u64) {
    let mut observed = target.load(Ordering::Relaxed);
    while value > observed {
        match target.compare_exchange_weak(observed, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(current) => observed = current,
        }
    }
}

#[derive(Default)]
struct OfferConsumerBucket {
    generation: AtomicU64,
    messages: AtomicU64,
    payload_bytes: AtomicU64,
    recv_wait_nanos: AtomicU64,
    recv_wait_max_nanos: AtomicU64,
    parse_nanos: AtomicU64,
    parse_max_nanos: AtomicU64,
    send_nanos: AtomicU64,
    send_max_nanos: AtomicU64,
    recv_errors: AtomicU64,
}

impl OfferConsumerBucket {
    fn reset_for_generation(&self, generation: u64) {
        self.messages.store(0, Ordering::Relaxed);
        self.payload_bytes.store(0, Ordering::Relaxed);
        self.recv_wait_nanos.store(0, Ordering::Relaxed);
        self.recv_wait_max_nanos.store(0, Ordering::Relaxed);
        self.parse_nanos.store(0, Ordering::Relaxed);
        self.parse_max_nanos.store(0, Ordering::Relaxed);
        self.send_nanos.store(0, Ordering::Relaxed);
        self.send_max_nanos.store(0, Ordering::Relaxed);
        self.recv_errors.store(0, Ordering::Relaxed);
        self.generation.store(generation, Ordering::Release);
    }
}

pub struct OfferConsumerStats {
    start: Instant,
    start_unix_ms: u64,
    buckets: Vec<OfferConsumerBucket>,
    total_messages: AtomicU64,
    total_payload_bytes: AtomicU64,
    total_recv_wait_nanos: AtomicU64,
    total_parse_nanos: AtomicU64,
    total_send_nanos: AtomicU64,
    total_recv_errors: AtomicU64,
}

impl OfferConsumerStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            start_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
                .unwrap_or(0),
            buckets: (0..BUCKET_COUNT)
                .map(|_| OfferConsumerBucket::default())
                .collect(),
            total_messages: AtomicU64::new(0),
            total_payload_bytes: AtomicU64::new(0),
            total_recv_wait_nanos: AtomicU64::new(0),
            total_parse_nanos: AtomicU64::new(0),
            total_send_nanos: AtomicU64::new(0),
            total_recv_errors: AtomicU64::new(0),
        })
    }

    fn generation_for_elapsed(&self, elapsed: Duration) -> u64 {
        (elapsed.as_nanos() / BUCKET_WIDTH.as_nanos()) as u64
    }

    fn bucket_for_now(&self) -> (&OfferConsumerBucket, u64) {
        let generation = self.generation_for_elapsed(self.start.elapsed());
        let bucket = &self.buckets[(generation as usize) % BUCKET_COUNT];
        if bucket.generation.load(Ordering::Acquire) != generation {
            bucket.reset_for_generation(generation);
        }
        (bucket, generation)
    }

    pub fn record_message(
        &self,
        payload_bytes: usize,
        recv_wait: Duration,
        parse_elapsed: Duration,
        send_elapsed: Duration,
    ) {
        let (bucket, _) = self.bucket_for_now();
        let recv_wait_nanos = duration_nanos_u64(recv_wait);
        let parse_nanos = duration_nanos_u64(parse_elapsed);
        let send_nanos = duration_nanos_u64(send_elapsed);

        bucket.messages.fetch_add(1, Ordering::Relaxed);
        bucket
            .payload_bytes
            .fetch_add(payload_bytes as u64, Ordering::Relaxed);
        bucket.recv_wait_nanos.fetch_add(recv_wait_nanos, Ordering::Relaxed);
        bucket.parse_nanos.fetch_add(parse_nanos, Ordering::Relaxed);
        bucket.send_nanos.fetch_add(send_nanos, Ordering::Relaxed);
        record_atomic_max(&bucket.recv_wait_max_nanos, recv_wait_nanos);
        record_atomic_max(&bucket.parse_max_nanos, parse_nanos);
        record_atomic_max(&bucket.send_max_nanos, send_nanos);

        self.total_messages.fetch_add(1, Ordering::Relaxed);
        self.total_payload_bytes
            .fetch_add(payload_bytes as u64, Ordering::Relaxed);
        atomic_add_duration(&self.total_recv_wait_nanos, recv_wait);
        atomic_add_duration(&self.total_parse_nanos, parse_elapsed);
        atomic_add_duration(&self.total_send_nanos, send_elapsed);
    }

    pub fn record_recv_error(&self, recv_wait: Duration) {
        let (bucket, _) = self.bucket_for_now();
        let recv_wait_nanos = duration_nanos_u64(recv_wait);
        bucket.recv_errors.fetch_add(1, Ordering::Relaxed);
        bucket.recv_wait_nanos.fetch_add(recv_wait_nanos, Ordering::Relaxed);
        record_atomic_max(&bucket.recv_wait_max_nanos, recv_wait_nanos);
        self.total_recv_errors.fetch_add(1, Ordering::Relaxed);
        atomic_add_duration(&self.total_recv_wait_nanos, recv_wait);
    }

    pub fn snapshot(&self) -> OfferConsumerSnapshot {
        let current_generation = self.generation_for_elapsed(self.start.elapsed());
        let oldest_generation = current_generation.saturating_sub(BUCKET_COUNT as u64 - 1);
        let mut buckets = Vec::new();

        for bucket in &self.buckets {
            let generation = bucket.generation.load(Ordering::Acquire);
            if generation < oldest_generation || generation > current_generation {
                continue;
            }
            let messages = bucket.messages.load(Ordering::Relaxed);
            let recv_errors = bucket.recv_errors.load(Ordering::Relaxed);
            if messages == 0 && recv_errors == 0 {
                continue;
            }
            buckets.push(OfferConsumerBucketSnapshot {
                generation,
                start_ms: generation * BUCKET_WIDTH.as_millis() as u64,
                start_unix_ms: self.start_unix_ms + generation * BUCKET_WIDTH.as_millis() as u64,
                messages,
                payload_bytes: bucket.payload_bytes.load(Ordering::Relaxed),
                recv_wait_nanos: bucket.recv_wait_nanos.load(Ordering::Relaxed),
                recv_wait_max_nanos: bucket.recv_wait_max_nanos.load(Ordering::Relaxed),
                parse_nanos: bucket.parse_nanos.load(Ordering::Relaxed),
                parse_max_nanos: bucket.parse_max_nanos.load(Ordering::Relaxed),
                send_nanos: bucket.send_nanos.load(Ordering::Relaxed),
                send_max_nanos: bucket.send_max_nanos.load(Ordering::Relaxed),
                recv_errors,
            });
        }
        buckets.sort_by_key(|bucket| bucket.generation);

        OfferConsumerSnapshot {
            bucket_width_ms: BUCKET_WIDTH.as_millis() as u64,
            window_buckets: BUCKET_COUNT,
            current_generation,
            start_unix_ms: self.start_unix_ms,
            current_elapsed_ms: self.start.elapsed().as_millis().min(u64::MAX as u128) as u64,
            total_messages: self.total_messages.load(Ordering::Relaxed),
            total_payload_bytes: self.total_payload_bytes.load(Ordering::Relaxed),
            total_recv_wait_nanos: self.total_recv_wait_nanos.load(Ordering::Relaxed),
            total_parse_nanos: self.total_parse_nanos.load(Ordering::Relaxed),
            total_send_nanos: self.total_send_nanos.load(Ordering::Relaxed),
            total_recv_errors: self.total_recv_errors.load(Ordering::Relaxed),
            buckets,
        }
    }
}

pub struct OfferConsumerBucketSnapshot {
    pub generation: u64,
    pub start_ms: u64,
    pub start_unix_ms: u64,
    pub messages: u64,
    pub payload_bytes: u64,
    pub recv_wait_nanos: u64,
    pub recv_wait_max_nanos: u64,
    pub parse_nanos: u64,
    pub parse_max_nanos: u64,
    pub send_nanos: u64,
    pub send_max_nanos: u64,
    pub recv_errors: u64,
}

pub struct OfferConsumerSnapshot {
    pub bucket_width_ms: u64,
    pub window_buckets: usize,
    pub current_generation: u64,
    pub start_unix_ms: u64,
    pub current_elapsed_ms: u64,
    pub total_messages: u64,
    pub total_payload_bytes: u64,
    pub total_recv_wait_nanos: u64,
    pub total_parse_nanos: u64,
    pub total_send_nanos: u64,
    pub total_recv_errors: u64,
    pub buckets: Vec<OfferConsumerBucketSnapshot>,
}
