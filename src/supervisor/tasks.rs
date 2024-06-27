use crate::task::TaskId;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tokio::time::{self, Instant, Sleep};

/// Keeps track on pending tasks and wakes the supervisor up when a task is
/// ready to be dispatched.
///
/// TODO we should refresh the tasks if system time changes - this shouldn't
///      happen too frequently (since things like DST are already covered by us
///      relying on UTC), but still
///
///      maybe having a background thread that compares its cached time with the
///      system's time every minute or so would do the job?
///
///      (other solutions will be probably more OS-dependent)
///
/// N.B. there's no need to keep all tasks in the memory - in principle we could
///      just keep the `n` closest tasks or even just *the* closest task and
///      simply query database for a new task after dispatching one, it's just a
///      different trade-off
#[derive(Debug, Default)]
pub struct PendingTasks {
    tasks: BinaryHeap<PendingTask>,
    is_active: bool,

    // N.B. I think we don't really have to have a waker here, because we'll get
    //      woken up anyway as a part of the supervisor's main `select!`, but
    //      won't hurt to keep a waker just in case
    waker: Option<Waker>,
}

impl PendingTasks {
    pub fn push(
        &mut self,
        id: TaskId,
        scheduled_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) {
        let at = scheduled_at
            .and_then(|at| (at - now).to_std().ok())
            .map(|at| Box::pin(time::sleep(at)));

        self.tasks.push(PendingTask { at, id });

        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    /// Pauses the component so that we'll always return `Poll::Pending` until
    /// someone resumes us.
    ///
    /// Supervisor calls this function when all the workers are busy (or when
    /// there's no workers present at all).
    pub fn pause(&mut self) {
        self.is_active = false;
    }

    pub fn resume(&mut self) {
        self.is_active = true;

        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl Future for PendingTasks {
    type Output = TaskId;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        if !self.is_active {
            self.waker = Some(cx.waker().clone());

            return Poll::Pending;
        }

        let this = &mut *self;

        let poll = if let Some(mut task) = this.tasks.peek_mut() {
            if let Some(at) = &mut task.at {
                at.poll_unpin(cx).map(|_| task.id)
            } else {
                Poll::Ready(task.id)
            }
        } else {
            this.waker = Some(cx.waker().clone());

            Poll::Pending
        };

        if poll.is_ready() {
            // N.B. ideally we'd do it inside the `if let` above, but borrowck
            // complains
            self.tasks.pop();
        }

        poll
    }
}

impl Unpin for PendingTasks {
    //
}

#[derive(Debug)]
struct PendingTask {
    at: Option<Pin<Box<Sleep>>>,
    id: TaskId,
}

impl PendingTask {
    fn deadline(&self) -> Option<Instant> {
        self.at.as_ref().map(|at| at.deadline())
    }
}

impl PartialEq for PendingTask {
    fn eq(&self, other: &Self) -> bool {
        self.deadline() == other.deadline() && self.id == other.id
    }
}

impl Eq for PendingTask {
    //
}

impl PartialOrd for PendingTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // TODO when a couple of tasks are scheduled on the same time, we will
        //      dispatch them in accordance to their ids (so basically randomly,
        //      considering that we use UUIDs) - so it would probably make more
        //      sense to compare deadlines, then "created at"s, and only then
        //      ids

        other
            .deadline()
            .cmp(&self.deadline())
            .then_with(|| self.id.cmp(&other.id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::dt;
    use futures::FutureExt;
    use std::task::Waker;
    use std::time::Duration;

    #[tokio::test]
    async fn test() {
        let mut cx = Context::from_waker(Waker::noop());
        let mut target = PendingTasks::default();

        // ---

        time::pause();

        let now = dt("2018-01-01 12:00:00");

        target.push(TaskId::from(1), Some(dt("2018-01-01 13:00:00")), now);
        target.push(TaskId::from(2), None, now);
        target.push(TaskId::from(3), Some(dt("2018-01-01 12:30:00")), now);
        target.push(TaskId::from(4), Some(dt("2018-01-01 10:00:00")), now);
        target.push(TaskId::from(5), None, now);

        // ---

        // We start paused, so even though there are some tasks we could return,
        // we report `Pending`
        assert_eq!(Poll::Pending, target.poll_unpin(&mut cx));

        target.resume();

        // T=12:00 - a couple of tasks were created without the deadline
        assert_eq!(Poll::Ready(TaskId::from(5)), target.poll_unpin(&mut cx));
        assert_eq!(Poll::Ready(TaskId::from(4)), target.poll_unpin(&mut cx));
        assert_eq!(Poll::Ready(TaskId::from(2)), target.poll_unpin(&mut cx));
        assert_eq!(Poll::Pending, target.poll_unpin(&mut cx));

        // T=12:31
        time::advance(Duration::from_mins(31)).await;

        assert_eq!(Poll::Ready(TaskId::from(3)), target.poll_unpin(&mut cx));
        assert_eq!(Poll::Pending, target.poll_unpin(&mut cx));

        // T=12:56
        time::advance(Duration::from_mins(25)).await;
        assert_eq!(Poll::Pending, target.poll_unpin(&mut cx));

        // T=13:11
        time::advance(Duration::from_mins(10)).await;

        assert_eq!(Poll::Ready(TaskId::from(1)), target.poll_unpin(&mut cx));
        assert_eq!(Poll::Pending, target.poll_unpin(&mut cx));
    }
}
