use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use signals::{
    Derive, Drive, Failure, Frame, FrameOp, FrameSink, JobResult, ReadError, Register, Runtime,
};

#[derive(Default)]
struct Frames(Vec<Vec<String>>);

impl FrameSink<String> for Frames {
    fn publish(&mut self, frame: Frame<String>) -> Result<(), Failure> {
        self.0.push(
            frame
                .ops
                .into_iter()
                .filter_map(|op| match op {
                    FrameOp::Patch(_, patch) => Some(patch),
                    _ => None,
                })
                .collect(),
        );
        Ok(())
    }
}

#[derive(Default)]
struct Job {
    result: Option<i32>,
    wake: Option<Waker>,
}

struct Manual(Rc<RefCell<Job>>);

impl Future for Manual {
    type Output = JobResult<i32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut job = self.0.borrow_mut();
        job.wake = Some(cx.waker().clone());
        job.result
            .take()
            .map_or(Poll::Pending, |value| Poll::Ready(Ok(value)))
    }
}

fn complete(job: &Rc<RefCell<Job>>, value: i32) {
    let mut job = job.borrow_mut();
    job.result = Some(value);
    job.wake.as_ref().unwrap().wake_by_ref();
}

#[test]
fn affect_declarations_take_effect_on_the_next_tick_and_end_independently() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut frames = Frames::default();
    let job = Rc::new(RefCell::new(Job::default()));
    let mut source = None;
    let owner = runtime
        .install(|root| {
            let job = Rc::clone(&job);
            source =
                Some(root.memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job)))))));
        })
        .unwrap();
    let source = source.unwrap();
    let other = runtime.install(|_| {}).unwrap();
    let a = runtime.affects(owner, source).unwrap();
    let b = runtime.affects(other, source).unwrap();

    runtime.flush(&mut frames).unwrap();
    assert!(!runtime.is_pending(source).unwrap());
    assert!(matches!(runtime.latest(source), Err(ReadError::Pending(_))));

    complete(&job, 7);
    runtime.flush(&mut frames).unwrap();
    assert!(runtime.is_pending(source).unwrap());
    runtime.end_affect(a).unwrap();
    runtime.end_affect(a).unwrap();
    runtime.flush(&mut frames).unwrap();
    assert!(runtime.is_pending(source).unwrap());
    runtime.dispose(other).unwrap();
    assert!(runtime.is_pending(source).unwrap());
    runtime.flush(&mut frames).unwrap();
    assert!(!runtime.is_pending(source).unwrap());
    runtime.end_affect(b).unwrap();

    let c = runtime.affects(owner, source).unwrap();
    runtime.end_affect(a).unwrap();
    assert!(!runtime.is_pending(source).unwrap());
    runtime.flush(&mut frames).unwrap();
    assert!(runtime.is_pending(source).unwrap());
    runtime.end_affect(c).unwrap();
    assert!(runtime.is_pending(source).unwrap());
    runtime.flush(&mut frames).unwrap();
    assert!(!runtime.is_pending(source).unwrap());
}

#[test]
fn disposing_a_source_read_only_by_a_lane_posts_its_fallback_tick() {
    let posts = Arc::new(AtomicUsize::new(0));
    let host = Arc::clone(&posts);
    let mut rt = Runtime::new(move || {
        host.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    let mut source = None;
    let source_owner = rt.install(|root| source = Some(root.signal(42).0)).unwrap();
    let source = source.unwrap();
    let mut flag = None;
    let view_owner = rt
        .install(|root| {
            let (selector, _) = root.signal(0);
            flag = Some(selector);
            root.render_ahead(
                move |cx| {
                    if cx.is_pending(selector)? {
                        match cx.get(source) {
                            Ok(value) => Ok(*value),
                            Err(ReadError::Handle(signals::HandleError::Disposed)) => Ok(-1),
                            Err(error) => Err(error),
                        }
                    } else {
                        Ok(0)
                    }
                },
                |value| value.to_string(),
            );
        })
        .unwrap();
    let mut frames = Frames::default();
    rt.flush(&mut frames).unwrap();
    rt.affects(view_owner, flag.unwrap()).unwrap();
    rt.flush(&mut frames).unwrap();
    assert_eq!(frames.0.last().unwrap(), &["42"]);
    let before = posts.load(Ordering::Relaxed);
    rt.dispose(source_owner).unwrap();
    assert_eq!(posts.load(Ordering::Relaxed), before + 1);
    rt.flush(&mut frames).unwrap();
    assert_eq!(frames.0.last().unwrap(), &["-1"]);
}

#[test]
fn switching_away_from_an_affect_owner_posts_the_removal_tick() {
    let posts = Arc::new(AtomicUsize::new(0));
    let host = posts.clone();
    let mut runtime = Runtime::new(move || {
        host.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    let first_branch = Rc::new(std::cell::Cell::new(None));
    let mut handles = None;
    let mut frames = Frames::default();
    runtime
        .install(|root| {
            let (key, switch) = root.signal(1);
            let (value, _) = root.signal(10);
            let first_branch = first_branch.clone();
            root.switch(
                move |cx| Ok(*cx.get(key)?),
                move |branch, active| {
                    if *active == 1 {
                        first_branch.set(Some(branch.owner()));
                    }
                },
            );
            root.render_ahead(
                move |cx| cx.is_pending(value),
                |pending| format!("affect={pending}"),
            );
            handles = Some((value, switch));
        })
        .unwrap();
    let (value, switch) = handles.unwrap();
    runtime.flush(&mut frames).unwrap();
    runtime.affects(first_branch.get().unwrap(), value).unwrap();
    runtime.flush(&mut frames).unwrap();
    assert!(runtime.is_pending(value).unwrap());
    assert_eq!(frames.0.pop().unwrap(), vec!["affect=true"]);

    runtime.set(switch, 2).unwrap();
    let before = posts.load(Ordering::Relaxed);
    runtime.flush(&mut frames).unwrap();
    assert_eq!(posts.load(Ordering::Relaxed), before + 1);
    assert!(runtime.is_pending(value).unwrap());
    runtime.flush(&mut frames).unwrap();
    assert!(!runtime.is_pending(value).unwrap());
    assert_eq!(frames.0.pop().unwrap(), vec!["affect=false"]);
}

#[test]
fn initial_loading_fallback_and_ahead_display_share_the_first_frame() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut frames = Frames::default();
    let job = Rc::new(RefCell::new(Job::default()));
    runtime
        .install(|root| {
            let (id, _) = root.signal(1);
            let job = Rc::clone(&job);
            let title = root.memo(move |cx| {
                cx.get(id)?;
                Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job)))))
            });
            root.loading(
                move |cx| {
                    cx.get(id)?;
                    Ok(())
                },
                move |content| {
                    content.render(move |cx| Ok(*cx.get(title)?), |value| value.to_string())
                },
                |fallback| fallback.render(|_| Ok("Loading"), |value| value.to_string()),
            );
            root.render_ahead(
                move |cx| Ok((*cx.get(id)?, *cx.latest(id)?, cx.is_pending(id)?)),
                |state| format!("{state:?}"),
            );
        })
        .unwrap();

    runtime.flush(&mut frames).unwrap();
    assert_eq!(frames.0.len(), 1);
    assert!(frames.0[0].contains(&"Loading".to_owned()));
    assert!(frames.0[0].contains(&"(1, 1, false)".to_owned()));
}

#[test]
fn a_tracked_pending_state_waits_until_the_first_answer() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut frames = Frames::default();
    let job = Rc::new(RefCell::new(Job::default()));
    let mut source = None;
    runtime
        .install(|root| {
            let job = Rc::clone(&job);
            let value = root.memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job))))));
            source = Some(value);
            root.render_ahead(
                move |cx| match cx.is_pending(value) {
                    Err(ReadError::Pending(_)) => Ok("unanswered"),
                    Ok(false) => Ok("settled"),
                    Ok(true) => Ok("pending"),
                    Err(error) => Err(error),
                },
                |status| status.to_string(),
            );
        })
        .unwrap();
    let source = source.unwrap();
    runtime.flush(&mut frames).unwrap();
    assert_eq!(frames.0.pop().unwrap(), vec!["unanswered"]);
    assert!(!runtime.is_pending(source).unwrap());
    complete(&job, 1);
    runtime.flush(&mut frames).unwrap();
    assert_eq!(frames.0.pop().unwrap(), vec!["settled"]);
    assert!(!runtime.is_pending(source).unwrap());
}

#[test]
fn only_a_changed_question_is_pending_while_its_answer_is_staged() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut frames = Frames::default();
    let jobs: [Rc<RefCell<Job>>; 4] = Default::default();
    let mut handles = None;
    runtime
        .install(|root| {
            let (id, write) = root.signal(1);
            let (gate, block) = root.signal(1);
            let [refresh, changed, first_gate, second_gate] = jobs.clone();
            let calls = std::cell::Cell::new(0);
            let source = root.memo(move |cx| {
                let count = calls.get();
                calls.set(count + 1);
                match (*cx.get(id)?, count) {
                    (1, 0) => Ok(Derive::Value(10)),
                    (1, _) => Ok(Derive::Future(Box::pin(Manual(Rc::clone(&refresh))))),
                    (2, _) => Ok(Derive::Future(Box::pin(Manual(Rc::clone(&changed))))),
                    _ => unreachable!(),
                }
            });
            let gate = root.memo(move |cx| match *cx.get(gate)? {
                1 => Ok(Derive::Value(10)),
                2 => Ok(Derive::Future(Box::pin(Manual(Rc::clone(&first_gate))))),
                3 => Ok(Derive::Future(Box::pin(Manual(Rc::clone(&second_gate))))),
                _ => unreachable!(),
            });
            root.render(
                move |cx| Ok((*cx.get(source)?, *cx.get(gate)?)),
                |pair| format!("{pair:?}"),
            );
            handles = Some((source, write, block));
        })
        .unwrap();
    let (source, write, block) = handles.unwrap();
    runtime.flush(&mut frames).unwrap();

    runtime.refresh(source).unwrap();
    runtime.set(block, 2).unwrap();
    runtime.flush(&mut frames).unwrap();
    assert!(!runtime.is_pending(source).unwrap());
    complete(&jobs[0], 20);
    runtime.flush(&mut frames).unwrap();
    assert!(!runtime.is_pending(source).unwrap());
    assert_eq!(*runtime.latest(source).unwrap(), 20);
    assert_eq!(*runtime.committed(source).unwrap(), 10);
    complete(&jobs[2], 20);
    runtime.flush(&mut frames).unwrap();
    assert_eq!(*runtime.committed(source).unwrap(), 20);

    runtime.set(write, 2).unwrap();
    runtime.set(block, 3).unwrap();
    assert!(!runtime.is_pending(source).unwrap());
    runtime.flush(&mut frames).unwrap();
    assert!(runtime.is_pending(source).unwrap());
    complete(&jobs[1], 30);
    runtime.flush(&mut frames).unwrap();
    assert!(runtime.is_pending(source).unwrap());
    assert_eq!(*runtime.latest(source).unwrap(), 30);
    assert_eq!(*runtime.committed(source).unwrap(), 20);
    complete(&jobs[3], 30);
    runtime.flush(&mut frames).unwrap();
    assert!(!runtime.is_pending(source).unwrap());
    assert_eq!(*runtime.committed(source).unwrap(), 30);
}

#[test]
fn untracked_reads_of_held_answers_do_not_call_value_equality() {
    struct Checked(i32, Rc<std::cell::Cell<bool>>);
    impl PartialEq for Checked {
        fn eq(&self, other: &Self) -> bool {
            assert!(!self.1.get(), "application equality during a public read");
            self.0 == other.0
        }
    }
    let armed = Rc::new(std::cell::Cell::new(false));
    let mut rt = Runtime::new(|| {}).unwrap();
    let mut handles = None;
    rt.install(|root| {
        let (id, write) = root.signal(Checked(1, armed.clone()));
        let compare = armed.clone();
        let ahead = root
            .ahead_memo(move |cx| Ok(Derive::Value(Checked(cx.latest(id)?.0, compare.clone()))));
        let gate = root.ahead_memo(move |cx| {
            if cx.latest(id)?.0 == 2 {
                Ok(Derive::Future(Box::pin(std::future::pending())))
            } else {
                Ok(Derive::Value(0))
            }
        });
        root.render_ahead(
            move |cx| Ok(cx.get(ahead)?.0 + *cx.get(gate)?),
            |v| v.to_string(),
        );
        handles = Some((id, write, ahead));
    })
    .unwrap();
    let (id, write, ahead) = handles.unwrap();
    let mut frames = Frames::default();
    rt.flush(&mut frames).unwrap();
    rt.set(write, Checked(2, armed.clone())).unwrap();
    rt.flush(&mut frames).unwrap();
    armed.set(true);
    for source in [id, ahead] {
        assert_eq!(rt.committed(source).unwrap().0, 1);
        assert_eq!(rt.latest(source).unwrap().0, 2);
        assert!(rt.is_pending(source).unwrap());
    }
}

#[test]
fn lane_only_render_failure_reaches_errored_and_recovers_to_the_same_value() {
    let mut rt = Runtime::new(|| {}).unwrap();
    let mut write = None;
    rt.install(|root| {
        let (id, set_id) = root.signal(1);
        write = Some(set_id);
        let gate = root.memo(move |cx| {
            if *cx.get(id)? == 1 {
                Ok(Derive::Value(1))
            } else {
                Ok(Derive::Future(Box::pin(std::future::pending())))
            }
        });
        root.render(move |cx| Ok(*cx.get(gate)?), |v| format!("main={v}"));
        root.render_ahead(move |cx| Ok(*cx.latest(id)?), |v| format!("latest={v}"));
        root.errored(
            move |content| {
                content.render_ahead(
                    move |cx| {
                        if *cx.get(id)? == 1 && *cx.latest(id)? == 2 {
                            Err(ReadError::Failed(Rc::new(std::io::Error::other(
                                "lane failed",
                            ))))
                        } else {
                            Ok(10)
                        }
                    },
                    |v| format!("content={v}"),
                );
            },
            |fallback, error| {
                fallback.render(move |cx| Ok(cx.get(error)?.to_string()), |v| v.to_string())
            },
        );
    })
    .unwrap();
    let mut frames = Frames::default();
    rt.flush(&mut frames).unwrap();
    let write = write.unwrap();
    rt.set(write, 2).unwrap();
    rt.flush(&mut frames).unwrap();
    assert_eq!(frames.0[1..], [vec!["latest=2", "lane failed"]]);
    rt.set(write, 3).unwrap();
    rt.flush(&mut frames).unwrap();
    assert_eq!(frames.0[2..], [vec!["latest=3", "content=10"]]);
}

#[test]
fn unhandled_lane_error_waits_for_its_group_then_poisons() {
    let mut rt = Runtime::new(|| {}).unwrap();
    let job = Rc::new(RefCell::new(Job::default()));
    let mut handles = None;
    rt.install(|root| {
        let (id, set) = root.signal(1);
        let gate = root.ahead_memo({
            let job = job.clone();
            move |cx| match (*cx.get(id)?, *cx.latest(id)?) {
                (1, 1) => Ok(Derive::Value(0)),
                (1, 2) => Ok(Derive::Future(Box::pin(Manual(job.clone())))),
                (2, 2) => Ok(Derive::Future(Box::pin(std::future::pending()))),
                state => panic!("unexpected inputs {state:?}"),
            }
        });
        root.render_ahead(move |cx| Ok(*cx.get(gate)?), |v| v.to_string());
        root.render_ahead(
            move |cx| {
                let latest = *cx.latest(id)?;
                if latest == *cx.get(id)? {
                    Ok(latest)
                } else {
                    Err(ReadError::Failed(Rc::new(std::io::Error::other(
                        "unhandled",
                    ))))
                }
            },
            |v| v.to_string(),
        );
        handles = Some((id, set));
    })
    .unwrap();
    let (id, set) = handles.unwrap();
    let mut frames = Frames::default();
    rt.flush(&mut frames).unwrap();
    rt.set(set, 2).unwrap();
    rt.flush(&mut frames).unwrap();
    assert_eq!(frames.0.len(), 1);
    complete(&job, 20);
    assert!(matches!(
        rt.flush(&mut frames),
        Err(signals::FlushError::Poisoned)
    ));
    assert_eq!(frames.0.len(), 1);
    assert!(matches!(
        rt.committed(id),
        Err(ReadError::Handle(signals::HandleError::Poisoned))
    ));
}

#[test]
fn each_lane_publishes_shared_latest_displays_with_its_own_answer() {
    for (status_first, lane_first) in [(false, true), (true, true), (false, false)] {
        let mut runtime = Runtime::new(|| {}).unwrap();
        let mut frames = Frames::default();
        let gate = Rc::new(RefCell::new(Job::default()));
        let main = Rc::new(RefCell::new(Job::default()));
        let lane = Rc::new(RefCell::new(Job::default()));
        let mut writes = None;
        let mut ahead = None;
        runtime
            .install(|root| {
                let (id, write) = root.signal(1);
                let (independent, other) = root.signal(8);
                writes = Some((write, other));
                let gate = Rc::clone(&gate);
                let title = root.memo(move |cx| {
                    if *cx.get(id)? == 1 {
                        Ok(Derive::Value(10))
                    } else {
                        Ok(Derive::Future(Box::pin(Manual(Rc::clone(&gate)))))
                    }
                });
                root.render(
                    move |cx| Ok(*cx.get(title)?),
                    |value| format!("title={value}"),
                );
                root.render_ahead(
                    move |cx| Ok(*cx.latest(id)?),
                    |value| format!("latest={value}"),
                );
                root.render_ahead(
                    move |cx| Ok(*cx.latest(independent)?),
                    |value| format!("independent={value}"),
                );
                let main = Rc::clone(&main);
                let lane = Rc::clone(&lane);
                let derived = root.ahead_memo(move |cx| {
                    let state = if status_first {
                        let pending = cx.is_pending(id)?;
                        (*cx.get(id)?, *cx.latest(id)?, pending)
                    } else {
                        (*cx.get(id)?, *cx.latest(id)?, cx.is_pending(id)?)
                    };
                    match state {
                        (1, 1, false) => Ok(Derive::Value(100)),
                        (1, 2, true) => Ok(Derive::Future(Box::pin(Manual(Rc::clone(&lane))))),
                        (2, 2, false) => Ok(Derive::Future(Box::pin(Manual(Rc::clone(&main))))),
                        _ => panic!("inconsistent read snapshot: {state:?}"),
                    }
                });
                root.render_ahead(
                    move |cx| Ok(*cx.get(derived)?),
                    |value| format!("derived={value}"),
                );
                ahead = Some(derived);
            })
            .unwrap();
        let (write, other) = writes.unwrap();
        let ahead = ahead.unwrap();
        runtime.flush(&mut frames).unwrap();
        assert_eq!(frames.0.len(), 1);
        assert!(frames.0[0].contains(&"derived=100".to_owned()));

        runtime.set(write, 2).unwrap();
        runtime.flush(&mut frames).unwrap();
        assert_eq!(frames.0.len(), 1);
        assert!(main.borrow().wake.is_some() && lane.borrow().wake.is_some());

        runtime.set(other, 9).unwrap();
        runtime.flush(&mut frames).unwrap();
        assert_eq!(frames.0.pop().unwrap(), vec!["independent=9"]);

        if lane_first {
            complete(&lane, 200);
            runtime.flush(&mut frames).unwrap();
            assert_eq!(frames.0.pop().unwrap(), vec!["latest=2", "derived=200"]);
            assert_eq!(*runtime.committed(ahead).unwrap(), 200);
            assert_eq!(*runtime.latest(ahead).unwrap(), 200);
        }
        complete(&gate, 20);
        runtime.flush(&mut frames).unwrap();
        assert_eq!(frames.0.len(), 1);

        complete(&main, 300);
        runtime.flush(&mut frames).unwrap();
        let expected = if lane_first {
            vec!["title=20", "derived=300"]
        } else {
            vec!["title=20", "latest=2", "derived=300"]
        };
        assert_eq!(frames.0.pop().unwrap(), expected);
        if !lane_first {
            complete(&lane, 200);
            runtime.flush(&mut frames).unwrap();
            assert_eq!(frames.0.len(), 1);
            assert_eq!(*runtime.committed(ahead).unwrap(), 300);
        }
    }
}
