use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    io,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use signals::{
    Derive, Drive, Failure, FlushError, Frame, FrameSink, HandleError, JobResult, ReadError,
    Register, Runtime, Stream,
};

struct Sink;

impl FrameSink<()> for Sink {
    fn publish(&mut self, _: Frame<()>) -> Result<(), Failure> {
        Ok(())
    }
}

#[derive(Default)]
struct ManualState {
    polls: usize,
    drops: usize,
    result: Option<JobResult<i32>>,
    waker: Option<Waker>,
}

struct ManualFuture(Rc<RefCell<ManualState>>);

impl Future for ManualFuture {
    type Output = JobResult<i32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.0.borrow_mut();
        state.polls += 1;
        state.waker = Some(cx.waker().clone());
        state.result.take().map_or(Poll::Pending, Poll::Ready)
    }
}

impl Drop for ManualFuture {
    fn drop(&mut self) {
        self.0.borrow_mut().drops += 1;
    }
}

#[test]
fn a_worker_wake_is_deduplicated_and_polled_on_the_owner_thread() {
    let posts = Arc::new(AtomicUsize::new(0));
    let host_posts = Arc::clone(&posts);
    let mut runtime = Runtime::new(move || {
        host_posts.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    let job = Rc::new(RefCell::new(ManualState::default()));
    let mut source = None;
    runtime
        .install(|root| {
            let job = Rc::clone(&job);
            source = Some(
                root.memo(move |_| Ok(Derive::Future(Box::pin(ManualFuture(Rc::clone(&job)))))),
            );
        })
        .unwrap();
    let source = source.unwrap();

    assert_eq!(posts.load(Ordering::SeqCst), 1);
    assert_eq!(job.borrow().polls, 0);
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(job.borrow().polls, 1);
    assert!(matches!(
        runtime.committed(source),
        Err(ReadError::Pending(_))
    ));
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(job.borrow().polls, 1);

    job.borrow_mut().result = Some(Ok(42));
    let waker = job.borrow().waker.clone().unwrap();
    std::thread::spawn(move || {
        waker.wake_by_ref();
        waker.wake();
    })
    .join()
    .unwrap();
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    assert_eq!(job.borrow().polls, 1);

    runtime.flush(&mut Sink).unwrap();
    assert_eq!(*runtime.committed(source).unwrap(), 42);
    assert_eq!(job.borrow().polls, 2);
    assert_eq!(job.borrow().drops, 1);
}

#[test]
fn a_transitive_write_preempts_a_queued_wake_without_an_extra_poll() {
    let old = Rc::new(RefCell::new(ManualState::default()));
    let new = Rc::new(RefCell::new(ManualState::default()));
    let mut handles = None;
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    runtime
        .install(|root| {
            let (input, write) = root.signal(1);
            let transit = root.memo(move |cx| Ok(Derive::Value(*cx.get(input)? * 2)));
            let old = Rc::clone(&old);
            let new = Rc::clone(&new);
            let source = root.memo(move |cx| {
                let job = if *cx.get(transit)? == 2 { &old } else { &new };
                Ok(Derive::Future(Box::pin(ManualFuture(Rc::clone(job)))))
            });
            let dependent = root.memo(move |cx| Ok(Derive::Value(*cx.get(source)? * 10)));
            handles = Some((dependent, write));
        })
        .unwrap();
    let (dependent, write) = handles.unwrap();
    runtime.flush(&mut Sink).unwrap();
    assert!(matches!(
        runtime.committed(dependent),
        Err(ReadError::Pending(_))
    ));

    old.borrow_mut().result = Some(Ok(20));
    let stale = old.borrow().waker.clone().unwrap();
    stale.wake_by_ref();
    runtime.set(write, 2).unwrap();
    runtime.flush(&mut Sink).unwrap();
    assert_eq!((old.borrow().polls, old.borrow().drops), (1, 1));
    assert_eq!(new.borrow().polls, 1);
    assert!(matches!(
        runtime.committed(dependent),
        Err(ReadError::Pending(_))
    ));

    stale.wake();
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(new.borrow().polls, 1);
    new.borrow_mut().result = Some(Ok(40));
    new.borrow().waker.as_ref().unwrap().wake_by_ref();
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(*runtime.committed(dependent).unwrap(), 400);
}

#[test]
fn a_worker_wake_during_poll_posts_the_next_tick() {
    let posts = Arc::new(AtomicUsize::new(0));
    let host = Arc::clone(&posts);
    let mut runtime = Runtime::<()>::new(move || {
        host.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    let polls = Rc::new(Cell::new(0));
    let mut handle = None;
    runtime
        .install(|root| {
            let polls = Rc::clone(&polls);
            handle = Some(root.memo(move |_| {
                let polls = Rc::clone(&polls);
                Ok(Derive::Future(Box::pin(std::future::poll_fn(move |cx| {
                    let count = polls.get();
                    polls.set(count + 1);
                    if count == 0 {
                        let wake = cx.waker().clone();
                        std::thread::spawn(move || wake.wake()).join().unwrap();
                        Poll::Pending
                    } else {
                        Poll::Ready(Ok(42))
                    }
                }))))
            }));
        })
        .unwrap();
    let handle = handle.unwrap();
    let before = posts.load(Ordering::SeqCst);
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(posts.load(Ordering::SeqCst), before + 1);
    assert_eq!(polls.get(), 1);
    assert!(matches!(
        runtime.committed(handle),
        Err(ReadError::Pending(_))
    ));
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(polls.get(), 2);
    assert_eq!(*runtime.committed(handle).unwrap(), 42);
}

#[test]
fn disposed_wakers_cannot_poll_a_reused_slot_and_stop_posting_after_runtime_drop() {
    let posts = Arc::new(AtomicUsize::new(0));
    let host_posts = Arc::clone(&posts);
    let mut runtime = Runtime::<()>::new(move || {
        host_posts.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    let disposed = Rc::new(RefCell::new(ManualState::default()));
    let current = Rc::new(RefCell::new(ManualState::default()));
    let install = |runtime: &mut Runtime<()>, job: &Rc<RefCell<ManualState>>| {
        let mut handle = None;
        let owner = runtime
            .install(|root| {
                let job = Rc::clone(job);
                handle =
                    Some(root.memo(move |_| {
                        Ok(Derive::Future(Box::pin(ManualFuture(Rc::clone(&job)))))
                    }));
            })
            .unwrap();
        (owner, handle.unwrap())
    };
    let (owner, old_handle) = install(&mut runtime, &disposed);
    runtime.flush(&mut Sink).unwrap();
    let stale = disposed.borrow().waker.clone().unwrap();
    runtime.dispose(owner).unwrap();
    assert_eq!(disposed.borrow().drops, 1);
    let (_, new_handle) = install(&mut runtime, &current);
    runtime.flush(&mut Sink).unwrap();
    stale.wake_by_ref();
    runtime.flush(&mut Sink).unwrap();
    assert!(matches!(
        runtime.committed(old_handle),
        Err(ReadError::Handle(HandleError::Disposed))
    ));
    assert!(matches!(
        runtime.committed(new_handle),
        Err(ReadError::Pending(_))
    ));
    assert_eq!(current.borrow().polls, 1);

    let live_waker = current.borrow().waker.clone().unwrap();
    drop(runtime);
    assert_eq!(current.borrow().drops, 1);
    let posted = posts.load(Ordering::SeqCst);
    stale.wake();
    live_waker.wake();
    assert_eq!(posts.load(Ordering::SeqCst), posted);
}

#[derive(Default)]
struct StreamState {
    polls: usize,
    drops: usize,
    steps: VecDeque<Poll<Option<JobResult<i32>>>>,
    waker: Option<Waker>,
}

struct ManualStream(Rc<RefCell<StreamState>>);

impl Stream for ManualStream {
    type Item = JobResult<i32>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.0.borrow_mut();
        state.polls += 1;
        state.waker = Some(cx.waker().clone());
        state.steps.pop_front().expect("unexpected stream poll")
    }
}

impl Drop for ManualStream {
    fn drop(&mut self) {
        self.0.borrow_mut().drops += 1;
    }
}

#[test]
fn a_stream_resumes_after_each_commit_and_retains_its_last_item_on_pending_and_eof() {
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    let stream = Rc::new(RefCell::new(StreamState {
        steps: VecDeque::from([
            Poll::Ready(Some(Ok(3))),
            Poll::Pending,
            Poll::Ready(Some(Ok(3))),
            Poll::Ready(None),
        ]),
        ..StreamState::default()
    }));
    let mut handle = None;
    runtime
        .install(|root| {
            let stream = Rc::clone(&stream);
            let source =
                root.memo(move |_| Ok(Derive::Stream(Box::pin(ManualStream(Rc::clone(&stream))))));
            root.render(move |cx| Ok(*cx.get(source)?), |_| ());
            handle = Some(source);
        })
        .unwrap();
    let handle = handle.unwrap();
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(*runtime.committed(handle).unwrap(), 3);
    assert_eq!(stream.borrow().polls, 1);
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(stream.borrow().polls, 2);
    assert_eq!(*runtime.committed(handle).unwrap(), 3);
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(stream.borrow().polls, 2);

    stream.borrow().waker.as_ref().unwrap().wake_by_ref();
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(stream.borrow().polls, 3);
    runtime.flush(&mut Sink).unwrap();
    assert_eq!((stream.borrow().polls, stream.borrow().drops), (4, 1));
    assert_eq!(*runtime.committed(handle).unwrap(), 3);
}

#[test]
fn stream_first_eof_is_a_failure_and_an_error_item_stops_the_stream() {
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    let stream = Rc::new(RefCell::new(StreamState {
        steps: VecDeque::from([
            Poll::Ready(Some(Ok(3))),
            Poll::Ready(Some(Err(
                Rc::new(io::Error::other("item failed")) as Failure
            ))),
        ]),
        ..StreamState::default()
    }));
    let mut handles = None;
    runtime
        .install(|root| {
            let stream = Rc::clone(&stream);
            let failed =
                root.memo(move |_| Ok(Derive::Stream(Box::pin(ManualStream(Rc::clone(&stream))))));
            let empty = root.memo(|_| {
                let stream = StreamState {
                    steps: VecDeque::from([Poll::Ready(None)]),
                    ..StreamState::default()
                };
                Ok(Derive::Stream(Box::pin(ManualStream(Rc::new(
                    RefCell::new(stream),
                )))))
            });
            handles = Some((failed, empty));
        })
        .unwrap();
    let (failed, empty) = handles.unwrap();
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(*runtime.committed(failed).unwrap(), 3);
    match runtime.committed(empty) {
        Err(ReadError::Failed(error)) => {
            assert_eq!(error.to_string(), "stream ended before its first item")
        }
        other => panic!("expected an empty stream failure, got {other:?}"),
    }
    runtime.flush(&mut Sink).unwrap();
    match runtime.committed(failed) {
        Err(ReadError::Failed(error)) => assert_eq!(error.to_string(), "item failed"),
        other => panic!("expected a terminal stream failure, got {other:?}"),
    }
    stream.borrow().waker.as_ref().unwrap().wake_by_ref();
    runtime.flush(&mut Sink).unwrap();
    assert_eq!((stream.borrow().polls, stream.borrow().drops), (2, 1));
}

#[test]
fn a_poll_panic_unwinds_and_poisons_the_runtime() {
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    runtime
        .install(|root| {
            root.memo::<()>(|_| Ok(Derive::Future(Box::pin(async { panic!("poll panic") }))));
        })
        .unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| runtime.flush(&mut Sink))).is_err());
    assert!(matches!(
        runtime.flush(&mut Sink),
        Err(FlushError::Poisoned)
    ));
}

#[test]
fn a_direct_write_cancels_a_flight_without_recomputing_a_manual_answer() {
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    let job = Rc::new(RefCell::new(ManualState::default()));
    let runs = Rc::new(Cell::new(0));
    let mut handles = None;
    runtime
        .install(|root| {
            let job = Rc::clone(&job);
            let runs = Rc::clone(&runs);
            handles = Some(root.derived_signal(move |_| {
                runs.set(runs.get() + 1);
                Ok(Derive::Future(Box::pin(ManualFuture(Rc::clone(&job)))))
            }));
        })
        .unwrap();
    let (source, write) = handles.unwrap();
    runtime.flush(&mut Sink).unwrap();
    job.borrow().waker.as_ref().unwrap().wake_by_ref();
    runtime.set(write, 42).unwrap();
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(*runtime.committed(source).unwrap(), 42);
    assert_eq!(
        (runs.get(), job.borrow().polls, job.borrow().drops),
        (1, 1, 1)
    );
}

#[test]
fn latest_excludes_queued_inputs_and_retains_an_answer_across_a_new_question() {
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    let job = Rc::new(RefCell::new(ManualState {
        result: Some(Ok(7)),
        ..ManualState::default()
    }));
    let mut handles = None;
    runtime
        .install(|root| {
            let (input, write) = root.signal(1);
            let job = Rc::clone(&job);
            let source = root.memo(move |cx| {
                cx.get(input)?;
                Ok(Derive::Future(Box::pin(ManualFuture(Rc::clone(&job)))))
            });
            handles = Some((input, write, source));
        })
        .unwrap();
    let (input, write, source) = handles.unwrap();
    assert!(matches!(runtime.latest(input), Err(ReadError::Pending(_))));
    assert!(matches!(runtime.latest(source), Err(ReadError::Pending(_))));
    runtime.flush(&mut Sink).unwrap();
    runtime.set(write, 2).unwrap();
    assert_eq!(*runtime.latest(input).unwrap(), 1);
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(*runtime.latest(input).unwrap(), 2);
    assert_eq!(*runtime.latest(source).unwrap(), 7);
    assert_eq!(job.borrow().polls, 2);
}

#[test]
fn refresh_reuses_the_answer_until_ready_and_last_command_wins() {
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    let old = Rc::new(RefCell::new(ManualState {
        result: Some(Ok(1)),
        ..ManualState::default()
    }));
    let refreshed = Rc::new(RefCell::new(ManualState::default()));
    let seen = Rc::new(RefCell::new(Vec::new()));
    let mut handles = None;
    runtime
        .install(|root| {
            let old = Rc::clone(&old);
            let refreshed = Rc::clone(&refreshed);
            let seen = Rc::clone(&seen);
            let runs = Cell::new(0);
            let (source, write) = root.derived_signal(move |_| {
                let n = runs.get();
                runs.set(n + 1);
                let job = if n == 0 { &old } else { &refreshed };
                Ok(Derive::Future(Box::pin(ManualFuture(Rc::clone(job)))))
            });
            root.render(
                move |cx| {
                    let value = match cx.get(source) {
                        Ok(value) => *value,
                        Err(ReadError::Pending(_)) => -1,
                        Err(error) => return Err(error),
                    };
                    seen.borrow_mut().push(value);
                    Ok(value)
                },
                |_| (),
            );
            handles = Some((source, write));
        })
        .unwrap();
    let (source, write) = handles.unwrap();
    runtime.flush(&mut Sink).unwrap();

    runtime.set(write, 9).unwrap();
    runtime.refresh(source).unwrap();
    assert_eq!(*runtime.latest(source).unwrap(), 1);
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(*runtime.committed(source).unwrap(), 1);
    assert_eq!(*runtime.latest(source).unwrap(), 1);
    assert_eq!(*seen.borrow(), vec![1]);
    assert_eq!((refreshed.borrow().polls, refreshed.borrow().drops), (1, 0));
    assert!(matches!(
        runtime.update(write, |value| value + 1),
        Err(ReadError::Pending(_))
    ));

    runtime.refresh(source).unwrap();
    runtime.set(write, 3).unwrap();
    refreshed.borrow().waker.as_ref().unwrap().wake_by_ref();
    runtime.flush(&mut Sink).unwrap();
    assert_eq!(*runtime.committed(source).unwrap(), 3);
    assert_eq!(*seen.borrow(), vec![1, 3]);
    assert_eq!((refreshed.borrow().polls, refreshed.borrow().drops), (1, 1));
}
