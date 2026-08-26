//! Serving capacity, modelled well enough to make queue depth real.
//!
//! The demo needs metrics that respond to load rather than a configured
//! constant, because the routing question under test is what a caller does when
//! one site is busier than another. A number read from the environment cannot
//! answer that.
//!
//! Capacity is a semaphore. A request holds a permit for its service time, and
//! arrivals past capacity wait for one. Queue depth is then an observation of
//! the mock rather than a setting: offer more load than a site can serve and
//! the depth rises on its own.

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Semaphore;

/// How busy the mock currently is.
#[derive(Debug)]
pub struct Load {
    /// Permits, one per concurrently served request.
    slots: Semaphore,
    /// Concurrency. Held separately because acquiring changes the permit count,
    /// and mutable because losing replicas is one of the shifts worth staging.
    capacity: AtomicU64,
    /// Requests admitted and not yet finished, running and waiting alike.
    in_flight: AtomicU64,
    /// Requests completed since start.
    served: AtomicU64,
    /// How long one request occupies a slot.
    service: std::time::Duration,
}

impl Load {
    /// A server of `capacity` concurrent requests, each taking `service_ms`.
    ///
    /// Capacity floors at one. A zero would make every request wait forever,
    /// which reads as a hang rather than as the misconfiguration it is.
    #[must_use]
    pub fn new(capacity: u64, service_ms: u64) -> Self {
        let capacity = capacity.max(1);
        let permits = usize::try_from(capacity).unwrap_or(usize::MAX);
        Self {
            slots: Semaphore::new(permits),
            capacity: AtomicU64::new(capacity),
            in_flight: AtomicU64::new(0),
            served: AtomicU64::new(0),
            service: std::time::Duration::from_millis(service_ms),
        }
    }

    /// Serve one request, waiting for a slot if every one is taken.
    ///
    /// The count rises before the wait, so a request queueing for a slot is
    /// already visible in the metrics. Counting only at admission would hide
    /// exactly the backlog the caller is trying to route around.
    pub async fn serve(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        // A permit is only dropped when the semaphore closes, which this never
        // does. Serving without one still reports honestly, so a lost permit
        // degrades the model rather than the process.
        let held = self.slots.acquire().await.ok();
        tokio::time::sleep(self.service).await;
        drop(held);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.served.fetch_add(1, Ordering::Relaxed);
    }

    /// Concurrency the site currently has.
    pub fn capacity(&self) -> u64 {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Resize capacity, as a site gaining or losing replicas would.
    ///
    /// This is the other half of a load shift. Traffic moving between sites and
    /// a site losing the capacity to serve what it already has look similar in
    /// a queue depth and call for different routing, so the harness has to be
    /// able to stage both.
    ///
    /// Shrinking takes permits away as they are returned rather than
    /// interrupting requests in flight, which is what draining a replica does.
    pub fn resize(&self, capacity: u64) -> u64 {
        let capacity = capacity.max(1);
        let previous = self.capacity.swap(capacity, Ordering::Relaxed);
        if capacity > previous {
            let added = usize::try_from(capacity.saturating_sub(previous)).unwrap_or(0);
            self.slots.add_permits(added);
        } else if capacity < previous {
            let removed = usize::try_from(previous.saturating_sub(capacity)).unwrap_or(0);
            self.slots.forget_permits(removed);
        }
        previous
    }

    /// Requests holding a slot right now.
    ///
    /// Clamped at zero because a shrink can leave more requests in flight than
    /// the new capacity, and a site over its capacity is not running a negative
    /// number of requests.
    pub fn running(&self) -> u64 {
        let free = u64::try_from(self.slots.available_permits()).unwrap_or(0);
        self.capacity().saturating_sub(free)
    }

    /// Requests admitted but still waiting for a slot.
    pub fn waiting(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed).saturating_sub(self.running())
    }

    /// Requests completed since start.
    pub fn served(&self) -> u64 {
        self.served.load(Ordering::Relaxed)
    }

    /// Fraction of capacity in use, from zero to one.
    ///
    /// Stands in for cache occupancy, which tracks concurrency closely enough
    /// for a routing demo and needs no separate model.
    pub fn utilization(&self) -> f64 {
        let ratio = f64::from(u32::try_from(self.running()).unwrap_or(u32::MAX))
            / f64::from(u32::try_from(self.capacity().max(1)).unwrap_or(u32::MAX));
        ratio.clamp(0.0, 1.0)
    }
}
