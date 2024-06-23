use crate::worker::{WorkerId, WorkerStatus};
use crate::HEARTBEAT_TIMEOUT;
use chrono::{DateTime, Utc};
use rand::seq::IteratorRandom;
use std::collections::{BTreeMap, BTreeSet};
use tracing::{info, warn};

#[derive(Debug, Default)]
pub struct SupervisedWorkers {
    workers: BTreeMap<WorkerId, SupervisedWorker>,
    idling_workers: BTreeSet<WorkerId>,
}

impl SupervisedWorkers {
    pub fn add(
        &mut self,
        id: WorkerId,
        status: WorkerStatus,
        now: DateTime<Utc>,
    ) {
        let worker = SupervisedWorker { last_heard_at: now };

        if self.workers.insert(id, worker).is_none() {
            info!(?id, "worker joined the cluster");

            if let WorkerStatus::Idle = status {
                self.idling_workers.insert(id);
            }
        } else {
            // Note that we care about the worker's status only during the first
            // heartbeat - that's because the worker has a race condition
            // between retrieving the Pg notification and updating the worker's
            // status, making it possible for us to observe a partial state
            // here.
            //
            // The scenario goes:
            //
            // - T+0: supervisor sends the "task dispatched" notification and
            //        marks the worker as busy
            // - T+1: worker retrieves the "task dispatched" notification
            // - T+2: worker sends a heartbeat, still saying "status: idle"
            // - T+3: supervisor retrieves the heartbeat, marks the worker as
            //        idling
            // - T+4: worker updates its own state to say "busy"
            // - whoopsie
            //
            // If this happened, the supervisor could try to assign two tasks to
            // the same worker, which isn't the end of the world, but something
            // we are simply trying to avoid.
        }
    }

    pub fn mark_as_idle(&mut self, id: WorkerId) {
        self.idling_workers.insert(id);
    }

    pub fn choose_idling(&mut self) -> Option<WorkerId> {
        let id = self
            .idling_workers
            .iter()
            .choose(&mut rand::thread_rng())
            .copied()?;

        self.idling_workers.remove(&id);

        Some(id)
    }

    /// Garbage-collects workers, i.e. goes through all of them and removes
    /// those workers which we haven't heard from in a long time.
    pub fn gc(&mut self, now: DateTime<Utc>) {
        let had_workers = !self.workers.is_empty();

        let dead_worker_ids: Vec<_> = self
            .workers
            .iter()
            .filter_map(|(id, worker)| {
                let last_heard_in =
                    (now - worker.last_heard_at).to_std().unwrap_or_default();

                if last_heard_in >= HEARTBEAT_TIMEOUT {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        for id in dead_worker_ids {
            warn!(?id, "worker seems to have died, cleaning up");

            self.workers.remove(&id);
            self.idling_workers.remove(&id);

            // TODO if this worker had any task assigned to it, mark that task
            //      as "interrupted"
        }

        if had_workers && self.workers.is_empty() {
            warn!("aii caramba, *all* workers seem dead");

            warn!(
                "tasks will not be dispatched until workers come back to life"
            );
        }
    }
}

#[derive(Debug)]
struct SupervisedWorker {
    last_heard_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::dt;

    #[test]
    fn choose_idling() {
        let mut target = SupervisedWorkers::default();
        let now = dt("2018-01-01 12:00:00");
        let w1 = WorkerId::from(1);
        let w2 = WorkerId::from(2);
        let w3 = WorkerId::from(3);

        target.add(w1, WorkerStatus::Idle, now);
        target.add(w2, WorkerStatus::Busy, now);
        target.add(w3, WorkerStatus::Idle, now);

        for _ in 0..2 {
            let actual = target.choose_idling().unwrap();

            assert!(actual == w1 || actual == w3);
        }

        assert!(target.choose_idling().is_none());
    }

    #[test]
    fn gc() {
        let mut target = SupervisedWorkers::default();
        let w1 = WorkerId::from(1);
        let w2 = WorkerId::from(2);
        let w3 = WorkerId::from(3);

        target.add(w1, WorkerStatus::Idle, dt("2018-01-01 12:00:06"));
        target.add(w2, WorkerStatus::Idle, dt("2018-01-01 12:00:00"));
        target.add(w3, WorkerStatus::Idle, dt("2018-01-01 12:00:12"));

        target.gc(dt("2018-01-01 12:00:10"));

        let actual: Vec<_> = target.workers.keys().copied().collect();
        let expected = vec![w1, w3];

        assert_eq!(expected, actual);
    }
}
