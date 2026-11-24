use std::{
    collections::HashSet,
    fmt,
    rc::Rc,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
};

use crate::{Derive, Error, JobResult, Key, LocalFuture, LocalStream, Pin, Stream, Value};

pub(super) enum Computed {
    Value(Value),
    Job(Work),
}

impl Computed {
    pub(super) fn erase<T: 'static>(result: Derive<T>) -> Self {
        match result {
            Derive::Value(value) => Self::Value(Rc::new(value)),
            Derive::Future(future) => Self::Job(Work::Future(Box::pin(async move {
                future.await.map(|value| Rc::new(value) as Value)
            }))),
            Derive::Stream(stream) => Self::Job(Work::Stream(Box::pin(ErasedStream(stream)))),
        }
    }
}

struct ErasedStream<T>(LocalStream<T>);

impl<T: 'static> Stream for ErasedStream<T> {
    type Item = JobResult<Value>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0
            .as_mut()
            .poll_next(cx)
            .map(|item| item.map(|result| result.map(|value| Rc::new(value) as Value)))
    }
}

pub(super) enum Work {
    Future(LocalFuture<Value>),
    Stream(LocalStream<Value>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum EvaluationDomain {
    Main,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct FlightId {
    pub(super) node: Key,
    pub(super) domain: EvaluationDomain,
    pub(super) question: u64,
    pub(super) generation: u64,
}

enum StreamPhase {
    First,
    AwaitingCommit,
    Resumed,
}

pub(super) struct Flight {
    pub(super) id: FlightId,
    work: Work,
    phase: StreamPhase,
}

pub(super) enum Progress {
    Pending,
    Item(Value),
    Done(Option<JobResult<Value>>),
}

impl Flight {
    pub(super) fn new(id: FlightId, work: Work) -> Self {
        Self {
            id,
            work,
            phase: StreamPhase::First,
        }
    }

    pub(super) fn can_poll(&self) -> bool {
        !matches!(self.phase, StreamPhase::AwaitingCommit)
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Progress {
        match &mut self.work {
            Work::Future(future) => match future.as_mut().poll(cx) {
                Poll::Pending => Progress::Pending,
                Poll::Ready(result) => Progress::Done(Some(result)),
            },
            Work::Stream(stream) => match stream.as_mut().poll_next(cx) {
                Poll::Pending => Progress::Pending,
                Poll::Ready(Some(Ok(value))) => {
                    self.phase = StreamPhase::AwaitingCommit;
                    Progress::Item(value)
                }
                Poll::Ready(Some(Err(error))) => Progress::Done(Some(Err(error))),
                Poll::Ready(None) if matches!(self.phase, StreamPhase::First) => {
                    Progress::Done(Some(Err(Rc::new(EmptyStream))))
                }
                Poll::Ready(None) => Progress::Done(None),
            },
        }
    }

    pub(super) fn resume_after_commit(&mut self) -> Option<FlightId> {
        if self.can_poll() {
            return None;
        }
        self.phase = StreamPhase::Resumed;
        Some(self.id)
    }
}

#[derive(Debug)]
struct EmptyStream;

impl fmt::Display for EmptyStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("stream ended before its first item")
    }
}

impl Error for EmptyStream {}

pub(super) struct Mailbox {
    state: Mutex<MailboxState>,
    wake_host: Box<dyn Fn() + Send + Sync>,
}

#[derive(Default)]
struct MailboxState {
    ready: Vec<FlightId>,
    dedup: HashSet<FlightId>,
    posted: bool,
    closed: bool,
}

impl Mailbox {
    pub(super) fn new(wake_host: impl Fn() + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::default(),
            wake_host: Box::new(wake_host),
        })
    }

    pub(super) fn request_tick(&self, flight: Option<FlightId>) {
        {
            let mut state = self.state.lock().expect("wake mailbox");
            if state.closed {
                return;
            }
            if let Some(id) = flight {
                if state.dedup.insert(id) {
                    state.ready.push(id);
                }
            }
            if state.posted {
                return;
            }
            state.posted = true;
        }
        (self.wake_host)();
    }

    pub(super) fn drain(&self) -> Vec<FlightId> {
        let mut state = self.state.lock().expect("wake mailbox");
        state.posted = false;
        state.dedup.clear();
        std::mem::take(&mut state.ready)
    }

    pub(super) fn close(&self) {
        let mut state = self.state.lock().expect("wake mailbox");
        *state = MailboxState {
            closed: true,
            ..MailboxState::default()
        };
    }

    pub(super) fn waker(self: &Arc<Self>, id: FlightId) -> Waker {
        Waker::from(Arc::new(TaskWake {
            id,
            mailbox: Arc::clone(self),
        }))
    }
}

struct TaskWake {
    id: FlightId,
    mailbox: Arc<Mailbox>,
}

impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.mailbox.request_tick(Some(self.id));
    }
}
