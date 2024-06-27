use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerStatus {
    Busy,
    Idle,
}

#[derive(Debug, Default)]
pub struct AtomicWorkerStatus {
    busy: AtomicBool,
}

impl AtomicWorkerStatus {
    pub fn store(&self, status: WorkerStatus) {
        let val = match status {
            WorkerStatus::Busy => true,
            WorkerStatus::Idle => false,
        };

        self.busy.store(val, Ordering::SeqCst);
    }

    pub fn load(&self) -> WorkerStatus {
        if self.busy.load(Ordering::SeqCst) {
            WorkerStatus::Busy
        } else {
            WorkerStatus::Idle
        }
    }
}
