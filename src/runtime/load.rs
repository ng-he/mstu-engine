//! What the engine is costing the machine it runs on.
//!
//! Its own process, not the whole machine: the question a pipeline raises is
//! what it costs, and everything a pipeline does happens in here.

use std::time::{Duration, Instant};

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

use crate::runtime::alloc;

/// How often the figures are worth taking again. Below this the processor
/// reading is noise: it is a difference between two samples, and a short
/// difference is mostly scheduling.
pub const INTERVAL: Duration = Duration::from_secs(1);

/// What the engine is using.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Load {
    /// Percent of one core, so two busy cores read as 200.
    pub cpu: f32,

    /// Resident memory of the whole process, in bytes.
    pub memory: u64,

    /// Of that, what the pipeline is holding in message arenas.
    pub pipeline: u64,
    pub chunks: u64,
}

/// Takes the readings, keeping the little state the processor figure needs.
pub struct Meter {
    system: System,
    pid: Pid,
    taken: Option<Instant>,
    last: Load,
}

impl Default for Meter {
    fn default() -> Self {
        Self::new()
    }
}

impl Meter {
    pub fn new() -> Self {
        Self {
            system: System::new(),
            pid: Pid::from_u32(std::process::id()),
            taken: None,
            last: Load {
                cpu: 0.0,
                memory: 0,
                pipeline: 0,
                chunks: 0,
            },
        }
    }

    /// The readings, taken again at most every `INTERVAL`.
    pub fn read(&mut self) -> Load {
        let (chunks, pipeline) = alloc::live();

        self.last.pipeline = pipeline as u64;
        self.last.chunks = chunks as u64;

        let due = self
            .taken
            .map(|taken| taken.elapsed() >= INTERVAL)
            .unwrap_or(true);

        if !due {
            return self.last;
        }

        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );

        if let Some(process) = self.system.process(self.pid) {
            self.last.cpu = process.cpu_usage();
            self.last.memory = process.memory();
        }

        self.taken = Some(Instant::now());
        self.last
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_engine_can_say_what_it_is_using() {
        let mut meter = Meter::new();
        let load = meter.read();

        assert!(load.memory > 0, "a running process holds memory");
        assert!(load.cpu >= 0.0);
    }

    /// The arena figures are the allocator's own, so they are taken every
    /// time rather than on the slow beat.
    #[test]
    fn what_the_pipeline_holds_is_read_every_time() {
        let mut meter = Meter::new();

        meter.read();

        let (chunks, bytes) = alloc::live();
        let load = meter.read();

        assert_eq!(load.chunks, chunks as u64);
        assert_eq!(load.pipeline, bytes as u64);
    }

    /// Between beats the processor figure stands still rather than being
    /// measured over a span too short to mean anything.
    #[test]
    fn the_processor_is_not_read_twice_in_a_moment() {
        let mut meter = Meter::new();

        let first = meter.read();
        let second = meter.read();

        assert_eq!(first.cpu, second.cpu);
    }
}
