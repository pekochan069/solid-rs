use std::{
    cell::{Cell, RefCell},
    future::Future,
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use signals::{
    Derive, Drive, Failure, FlushError, Frame, FrameOp, FrameSink, JobResult, Register, Runtime,
    Stream,
};

#[derive(Default)]
struct Job {
    value: Option<i32>,
    waker: Option<Waker>,
    drops: usize,
}

impl Job {
    fn complete(job: &Rc<RefCell<Self>>, value: i32) {
        let mut job = job.borrow_mut();
        job.value = Some(value);
        job.waker.take().unwrap().wake();
    }
}

struct Manual(Rc<RefCell<Job>>);

impl Future for Manual {
    type Output = JobResult<i32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut job = self.0.borrow_mut();
        if let Some(value) = job.value.take() {
            Poll::Ready(Ok(value))
        } else {
            job.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for Manual {
    fn drop(&mut self) {
        self.0.borrow_mut().drops += 1;
    }
}

#[derive(Default)]
struct Recorder(Vec<Frame<String>>);

impl FrameSink<String> for Recorder {
    fn publish(&mut self, frame: Frame<String>) -> Result<(), Failure> {
        self.0.push(frame);
        Ok(())
    }
}

impl Recorder {
    fn patches(&self) -> Vec<Vec<&str>> {
        self.0
            .iter()
            .map(|frame| {
                frame
                    .ops
                    .iter()
                    .filter_map(|op| match op {
                        FrameOp::Patch(_, patch) => Some(patch.as_str()),
                        _ => None,
                    })
                    .collect()
            })
            .collect()
    }
}

#[test]
fn a_render_hold_does_not_block_a_later_disjoint_update_on_the_same_owner() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let pending = Rc::new(RefCell::new(Job::default()));
    let mut handles = None;
    runtime
        .install(|root| {
            let (input, write) = root.signal(1);
            let (other, write_other) = root.signal(10);
            let pending = Rc::clone(&pending);
            let result = root.memo(move |cx| {
                if *cx.get(input)? == 1 {
                    Ok(Derive::Value(100))
                } else {
                    Ok(Derive::Future(Box::pin(Manual(Rc::clone(&pending)))))
                }
            });
            root.render(
                move |cx| Ok((*cx.get(input)?, *cx.get(result)?)),
                |v| format!("{v:?}"),
            );
            root.render(move |cx| Ok(*cx.get(other)?), |v| format!("other={v}"));
            let eager = root.memo(move |cx| Ok(Derive::Value(*cx.get(input)? + *cx.get(other)?)));
            handles = Some((input, write, other, write_other, eager));
        })
        .unwrap();
    let (input, write, other, write_other, eager) = handles.unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    runtime.set(write, 2).unwrap();
    assert_eq!(*runtime.latest(input).unwrap(), 1);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(input).unwrap(), 1);
    assert_eq!(*runtime.latest(input).unwrap(), 2);
    assert_eq!(sink.patches(), vec![vec!["(1, 100)", "other=10"]]);

    runtime.set(write, 2).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(input).unwrap(), 1);
    runtime.set(write_other, 11).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(input).unwrap(), 1);
    assert_eq!(*runtime.committed(other).unwrap(), 11);
    assert_eq!(*runtime.committed(eager).unwrap(), 11);
    assert_eq!(sink.patches()[1], vec!["other=11"]);
    assert_eq!(pending.borrow().drops, 0);

    Job::complete(&pending, 200);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(input).unwrap(), 2);
    assert_eq!(*runtime.committed(eager).unwrap(), 13);
    assert_eq!(sink.patches()[2], vec!["(2, 200)"]);
}

#[test]
fn switching_twice_keeps_a_visible_until_c_is_ready_and_drops_b() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let b = Rc::new(RefCell::new(Job::default()));
    let c = Rc::new(RefCell::new(Job::default()));
    let cleanup = Rc::new(RefCell::new(Vec::new()));
    let owners = Rc::new(RefCell::new(Vec::new()));
    let nested = Rc::new(Cell::new(None));
    let mut handles = None;
    runtime
        .install(|root| {
            let (choice, choose) = root.signal(0);
            handles = Some((choice, choose));
            let (b, c) = (Rc::clone(&b), Rc::clone(&c));
            let (cleanup, owners) = (Rc::clone(&cleanup), Rc::clone(&owners));
            let nested = Rc::clone(&nested);
            root.switch(
                move |cx| Ok(*cx.get(choice)?),
                move |branch, key| {
                    let key = *key;
                    owners.borrow_mut().push(branch.owner());
                    let cleanup = Rc::clone(&cleanup);
                    if key == 0 {
                        let nested = Rc::clone(&nested);
                        branch.switch(|_| Ok(()), move |child, _| nested.set(Some(child.owner())));
                    }
                    branch.on_cleanup(move || cleanup.borrow_mut().push(key));
                    let (offset, _) = branch.signal(5);
                    let job = match key {
                        0 => None,
                        1 => Some(Rc::clone(&b)),
                        _ => Some(Rc::clone(&c)),
                    };
                    let value = branch.memo(move |cx| {
                        let offset = *cx.get(offset)?;
                        Ok(match &job {
                            Some(job) => Derive::Future(Box::pin(Manual(Rc::clone(job)))),
                            None => Derive::Value(offset),
                        })
                    });
                    branch.render(
                        move |cx| Ok(*cx.get(value)?),
                        move |value| format!("{key}:{value}"),
                    );
                },
            );
        })
        .unwrap();
    let (choice, choose) = handles.unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec!["0:5"]]);

    runtime.set(choose, 1).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert!(cleanup.borrow().is_empty());
    assert_eq!(sink.0.len(), 1);
    let late = b.borrow().waker.clone().unwrap();

    runtime.set(choose, 2).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*cleanup.borrow(), vec![1]);
    assert_eq!(b.borrow().drops, 1);
    assert_eq!(*runtime.committed(choice).unwrap(), 0);
    assert_eq!(sink.0.len(), 1);
    late.wake();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.0.len(), 1);

    Job::complete(&c, 25);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(choice).unwrap(), 2);
    assert_eq!(sink.patches(), vec![vec!["0:5"], vec!["2:25"]]);
    assert_eq!(*cleanup.borrow(), vec![1, 0]);
    let [a, b, c] = owners.borrow()[..] else {
        panic!("expected three registered branches")
    };
    let last = &sink.0[1].ops;
    let child = nested.get().unwrap();
    let wrapper = sink.0[0]
        .ops
        .iter()
        .find_map(|op| match op {
            FrameOp::Attach { owner, parent, .. } if *owner == child => *parent,
            _ => None,
        })
        .unwrap();
    assert!(matches!(last.as_slice(), [
        FrameOp::Attach { owner, parent: Some(_), .. },
        FrameOp::Patch(patched, _), FrameOp::SetVisible(visible, true),
        FrameOp::Remove(removed_child), FrameOp::Remove(removed_wrapper), FrameOp::Remove(removed)
    ] if *owner == c && *patched == c && *visible == c && *removed_child == child && *removed_wrapper == wrapper && *removed == a));
    assert!(sink
        .0
        .iter()
        .flat_map(|frame| &frame.ops)
        .all(|op| { !matches!(op, FrameOp::Attach { owner, .. } if *owner == b) }));
}

#[test]
fn a_stream_with_a_staged_item_does_not_poll_again_until_the_frame_is_accepted() {
    struct CountingStream(Rc<Cell<usize>>);

    impl Stream for CountingStream {
        type Item = JobResult<i32>;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let count = self.0.get();
            self.0.set(count + 1);
            Poll::Ready(match count {
                0 => Some(Ok(41)),
                1 => Some(Ok(42)),
                _ => None,
            })
        }
    }

    let mut runtime = Runtime::new(|| {}).unwrap();
    let pending = Rc::new(RefCell::new(Job::default()));
    let polls = Rc::new(Cell::new(0));
    let mut navigate = None;
    runtime
        .install(|root| {
            let (route, switch_route) = root.signal(false);
            navigate = Some(switch_route);
            let (pending, polls) = (Rc::clone(&pending), Rc::clone(&polls));
            root.switch(
                move |cx| Ok(*cx.get(route)?),
                move |branch, route| {
                    if *route {
                        let (pending, polls) = (Rc::clone(&pending), Rc::clone(&polls));
                        let stream = branch.memo(move |_| {
                            Ok(Derive::Stream(Box::pin(CountingStream(Rc::clone(&polls)))))
                        });
                        let value = branch.memo(move |_| {
                            Ok(Derive::Future(Box::pin(Manual(Rc::clone(&pending)))))
                        });
                        branch.render(
                            move |cx| Ok((*cx.get(stream)?, *cx.get(value)?)),
                            |v| format!("{v:?}"),
                        );
                    } else {
                        branch.render(|_| Ok(()), |_| String::from("old"));
                    }
                },
            );
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    runtime.set(navigate.unwrap(), true).unwrap();
    runtime.flush(&mut sink).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(polls.get(), 1);
    assert_eq!(sink.patches(), vec![vec!["old"]]);

    Job::complete(&pending, 7);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(polls.get(), 1);
    assert_eq!(sink.patches()[1], vec!["(41, 7)"]);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(polls.get(), 2);
    assert_eq!(sink.patches()[2], vec!["(42, 7)"]);
}

#[test]
fn an_independent_write_updates_the_visible_branch_with_its_committed_inputs() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let pending = Rc::new(RefCell::new(Job::default()));
    let mut handles = None;
    runtime
        .install(|root| {
            let (route, navigate) = root.signal(0);
            let (label, rename) = root.signal("old");
            handles = Some((route, navigate, label, rename));
            let pending = Rc::clone(&pending);
            root.switch(
                move |cx| Ok(*cx.get(route)?),
                move |branch, key| {
                    if *key == 0 {
                        let display = branch
                            .memo(move |cx| Ok(Derive::Value((*cx.get(route)?, *cx.get(label)?))));
                        branch.render(
                            move |cx| Ok(*cx.get(display)?),
                            |v| format!("A {} {}", v.0, v.1),
                        );
                    } else {
                        let pending = Rc::clone(&pending);
                        let result = branch.memo(move |_| {
                            Ok(Derive::Future(Box::pin(Manual(Rc::clone(&pending)))))
                        });
                        branch.render(move |cx| Ok(*cx.get(result)?), |v| format!("B {v}"));
                    }
                },
            );
        })
        .unwrap();
    let (route, navigate, label, rename) = handles.unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    runtime.set(navigate, 1).unwrap();
    runtime.flush(&mut sink).unwrap();
    runtime.set(rename, "new").unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(route).unwrap(), 0);
    assert_eq!(*runtime.committed(label).unwrap(), "new");
    assert_eq!(sink.patches(), vec![vec!["A 0 old"], vec!["A 0 new"]]);

    Job::complete(&pending, 200);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(route).unwrap(), 1);
    assert_eq!(
        sink.patches(),
        vec![vec!["A 0 old"], vec!["A 0 new"], vec!["B 200"]]
    );
}

#[test]
fn a_common_render_reader_joins_candidate_writes_from_different_ticks() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let pending = Rc::new(RefCell::new(Job::default()));
    let mut handles = None;
    runtime
        .install(|root| {
            let (selected, select) = root.signal(false);
            let (label, rename) = root.signal("old");
            let pending = Rc::clone(&pending);
            let value = root.memo(move |cx| {
                Ok(if *cx.get(selected)? {
                    Derive::Future(Box::pin(Manual(Rc::clone(&pending))))
                } else {
                    Derive::Value(10)
                })
            });
            root.render(
                move |cx| Ok((*cx.get(label)?, *cx.get(value)?)),
                |v| format!("{} {}", v.0, v.1),
            );
            handles = Some((selected, select, label, rename));
        })
        .unwrap();
    let (selected, select, label, rename) = handles.unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    runtime.set(select, true).unwrap();
    runtime.flush(&mut sink).unwrap();
    runtime.set(rename, "new").unwrap();
    runtime.flush(&mut sink).unwrap();
    assert!(!*runtime.committed(selected).unwrap());
    assert_eq!(*runtime.committed(label).unwrap(), "old");
    assert_eq!(sink.patches(), vec![vec!["old 10"]]);

    Job::complete(&pending, 20);
    runtime.flush(&mut sink).unwrap();
    assert!(*runtime.committed(selected).unwrap());
    assert_eq!(*runtime.committed(label).unwrap(), "new");
    assert_eq!(sink.patches(), vec![vec!["old 10"], vec!["new 20"]]);
}

#[test]
fn a_hold_ends_when_the_last_reader_stops_reading_after_another_reader_is_disposed() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let pending = Rc::new(RefCell::new(Job::default()));
    let mut handles = None;
    runtime
        .install(|root| {
            let (selected, select) = root.signal(false);
            let pending = Rc::clone(&pending);
            let value = root.memo(move |cx| {
                Ok(if *cx.get(selected)? {
                    Derive::Future(Box::pin(Manual(Rc::clone(&pending))))
                } else {
                    Derive::Value(10)
                })
            });
            handles = Some((selected, select, value));
        })
        .unwrap();
    let (selected, select, value) = handles.unwrap();
    let first = runtime
        .install(|root| {
            root.render(move |cx| Ok(*cx.get(value)?), |v| format!("first {v}"));
        })
        .unwrap();
    let mut enable = None;
    runtime
        .install(|root| {
            let (enabled, write) = root.signal(true);
            enable = Some(write);
            root.render(
                move |cx| {
                    if *cx.get(enabled)? {
                        Ok(Some(*cx.get(value)?))
                    } else {
                        Ok(None)
                    }
                },
                |v| match *v {
                    Some(value) => format!("second {value}"),
                    None => String::from("disabled"),
                },
            );
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    runtime.set(select, true).unwrap();
    runtime.flush(&mut sink).unwrap();
    runtime.dispose(first).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert!(!*runtime.committed(selected).unwrap());
    assert_eq!(sink.0.len(), 1);

    runtime.set(enable.unwrap(), false).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert!(*runtime.committed(selected).unwrap());
    assert_eq!(
        sink.patches(),
        vec![vec!["first 10", "second 10"], vec!["disabled"]]
    );
    assert_eq!(pending.borrow().drops, 0);
}

#[test]
fn rejecting_a_replacement_cleans_up_only_the_unpublished_branch() {
    struct RejectAfterInitial(Recorder);

    impl FrameSink<String> for RejectAfterInitial {
        fn publish(&mut self, frame: Frame<String>) -> Result<(), Failure> {
            if self.0 .0.is_empty() {
                self.0.publish(frame)
            } else {
                Err(Rc::new(io::Error::other("host rejected replacement")))
            }
        }
    }

    let mut runtime = Runtime::new(|| {}).unwrap();
    let cleanup = Rc::new(RefCell::new(Vec::new()));
    let effects = Rc::new(RefCell::new(Vec::new()));
    let mut navigate = None;
    runtime
        .install(|root| {
            let (route, set_route) = root.signal(0);
            navigate = Some(set_route);
            let (cleanup, effects) = (Rc::clone(&cleanup), Rc::clone(&effects));
            root.switch(
                move |cx| Ok(*cx.get(route)?),
                move |branch, key| {
                    let key = *key;
                    let (cleanup, effects) = (Rc::clone(&cleanup), Rc::clone(&effects));
                    branch.on_cleanup(move || cleanup.borrow_mut().push(key));
                    branch.render(move |_| Ok(key), |v| format!("branch {v}"));
                    branch.effect(
                        move |_| Ok(key),
                        move |_, value| {
                            effects.borrow_mut().push(*value);
                            Ok(())
                        },
                    );
                },
            );
        })
        .unwrap();
    let mut sink = RejectAfterInitial(Recorder::default());
    runtime.flush(&mut sink).unwrap();
    runtime.set(navigate.unwrap(), 1).unwrap();
    let error = runtime.flush(&mut sink).unwrap_err();
    assert!(
        matches!(error, FlushError::Publish(error) if error.to_string() == "host rejected replacement")
    );
    assert_eq!(sink.0.patches(), vec![vec!["branch 0"]]);
    assert_eq!(*effects.borrow(), vec![0]);
    assert_eq!(*cleanup.borrow(), vec![1]);
    assert!(matches!(
        runtime.flush(&mut sink),
        Err(FlushError::Poisoned)
    ));
    drop(runtime);
    assert_eq!(*cleanup.borrow(), vec![1, 0]);
}

#[test]
fn ready_transactions_publish_in_order_and_only_successful_effects_apply() {
    struct AcceptOne(Recorder);

    impl FrameSink<String> for AcceptOne {
        fn publish(&mut self, frame: Frame<String>) -> Result<(), Failure> {
            if self.0 .0.is_empty() {
                self.0.publish(frame)
            } else {
                Err(Rc::new(io::Error::other("host rejected second frame")))
            }
        }
    }

    let mut runtime = Runtime::new(|| {}).unwrap();
    let effects = Rc::new(RefCell::new(Vec::new()));
    for name in ["first", "second"] {
        runtime
            .install(|root| {
                let effects = Rc::clone(&effects);
                root.render(move |_| Ok(name), |name| name.to_string());
                root.effect(
                    move |_| Ok(name),
                    move |_, name| {
                        effects.borrow_mut().push(*name);
                        Ok(())
                    },
                );
            })
            .unwrap();
    }
    let mut sink = AcceptOne(Recorder::default());
    assert!(matches!(
        runtime.flush(&mut sink),
        Err(FlushError::Publish(_))
    ));
    assert_eq!(sink.0.patches(), vec![vec!["first"]]);
    assert_eq!(*effects.borrow(), vec!["first"]);
}

struct VisibleBranch {
    runtime: Runtime<String>,
    sink: Recorder,
    route: signals::ReadRef<i32>,
    label: signals::ReadRef<i32>,
    rename: signals::WriteRef<i32>,
    replacement: Rc<RefCell<Job>>,
}

impl VisibleBranch {
    fn waiting(
        build: impl Fn(&mut signals::Registration<String>, signals::ReadRef<i32>, signals::ReadRef<i32>)
            + 'static,
    ) -> Self {
        let mut runtime = Runtime::new(|| {}).unwrap();
        let replacement = Rc::new(RefCell::new(Job::default()));
        let mut handles = None;
        runtime
            .install(|root| {
                let (route, navigate) = root.signal(0);
                let (label, rename) = root.signal(0);
                handles = Some((route, navigate, label, rename));
                let replacement = Rc::clone(&replacement);
                root.switch(
                    move |cx| Ok(*cx.get(route)?),
                    move |branch, key| {
                        if *key == 0 {
                            build(branch, route, label);
                        } else {
                            let replacement = Rc::clone(&replacement);
                            let result = branch.memo(move |_| {
                                Ok(Derive::Future(Box::pin(Manual(Rc::clone(&replacement)))))
                            });
                            branch.render(move |cx| Ok(*cx.get(result)?), |v| format!("B {v}"));
                        }
                    },
                );
            })
            .unwrap();
        let mut sink = Recorder::default();
        runtime.flush(&mut sink).unwrap();
        let (route, navigate, label, rename) = handles.unwrap();
        runtime.set(navigate, 1).unwrap();
        runtime.flush(&mut sink).unwrap();
        assert_eq!(sink.0.len(), 1);
        Self {
            runtime,
            sink,
            route,
            label,
            rename,
            replacement,
        }
    }

    fn rename(&mut self, label: i32) {
        self.runtime.set(self.rename, label).unwrap();
        self.runtime.flush(&mut self.sink).unwrap();
    }
}

#[test]
fn projected_failure_builds_one_fallback_with_the_accepted_error() {
    let attempts = Rc::new(Cell::new(0));
    let builds = Rc::new(Cell::new(0));
    let mut view = VisibleBranch::waiting({
        let attempts = attempts.clone();
        let builds = builds.clone();
        move |branch, route, label| {
            let attempts = attempts.clone();
            let builds = builds.clone();
            branch.errored(
                move |content| {
                    let attempts = attempts.clone();
                    content.render(
                        move |cx| {
                            let pair = (*cx.get(route)?, *cx.get(label)?);
                            if pair == (0, 1) {
                                attempts.set(attempts.get() + 1);
                                assert!(
                                    attempts.get() <= 8,
                                    "projected failure repeatedly evaluated"
                                );
                                return Err(signals::ReadError::Failed(Rc::new(io::Error::other(
                                    "visible error",
                                ))));
                            }
                            Ok(pair)
                        },
                        |v| format!("A {v:?}"),
                    );
                },
                move |fallback, error| {
                    builds.set(builds.get() + 1);
                    fallback.render(
                        move |cx| Ok(cx.get(error)?.to_string()),
                        |text| text.to_string(),
                    );
                },
            );
        }
    });
    view.rename(1);
    assert_eq!(attempts.get(), 1);
    assert_eq!(builds.get(), 1);
    assert_eq!(*view.runtime.committed(view.route).unwrap(), 0);
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 1);
    assert_eq!(
        view.sink.patches(),
        vec![vec!["A (0, 0)"], vec!["visible error"]]
    );
    Job::complete(&view.replacement, 5);
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(view.sink.patches()[2], vec!["B 5"]);
}

#[test]
fn projected_future_validates_its_inputs_before_polling_a_worker_wake() {
    let jobs = [
        Rc::new(RefCell::new(Job::default())),
        Rc::new(RefCell::new(Job::default())),
    ];
    let mut view = VisibleBranch::waiting({
        let jobs = jobs.clone();
        move |branch, route, label| {
            let jobs = jobs.clone();
            let result = branch.memo(move |cx| {
                let (route, label) = (*cx.get(route)?, *cx.get(label)?);
                Ok(if route == 0 && label > 0 {
                    Derive::Future(Box::pin(Manual(Rc::clone(&jobs[label as usize - 1]))))
                } else {
                    Derive::Value(10 + route)
                })
            });
            branch.render(move |cx| Ok(*cx.get(result)?), |v| format!("A {v}"));
        }
    });
    view.rename(1);
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 0);
    let stale = jobs[0]
        .borrow()
        .waker
        .clone()
        .expect("first projection must poll");
    std::thread::spawn(move || stale.wake()).join().unwrap();
    jobs[0].borrow_mut().value = Some(111);
    view.rename(2);
    assert_eq!(jobs[0].borrow().value, Some(111));
    assert_eq!(jobs[0].borrow().drops, 1);
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 0);
    assert_eq!(view.sink.patches(), vec![vec!["A 10"]]);
    let current = jobs[1]
        .borrow()
        .waker
        .clone()
        .expect("replacement must poll");
    jobs[1].borrow_mut().value = Some(222);
    std::thread::spawn(move || current.wake()).join().unwrap();
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 2);
    assert_eq!(*view.runtime.committed(view.route).unwrap(), 0);
    assert_eq!(view.sink.patches(), vec![vec!["A 10"], vec!["A 222"]]);
    Job::complete(&view.replacement, 7);
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(view.sink.patches()[2], vec!["B 7"]);
}

#[test]
fn projected_nested_switch_attaches_the_selected_tab_with_its_committed_route() {
    let mut view = VisibleBranch::waiting(|branch, route, label| {
        branch.switch(
            move |cx| {
                Ok(if *cx.get(route)? == 0 {
                    *cx.get(label)?
                } else {
                    0
                })
            },
            move |tab, key| {
                let key = *key;
                tab.render(
                    move |cx| Ok(*cx.get(route)?),
                    move |route| format!("A{key} route {route}"),
                );
            },
        );
    });
    view.rename(1);
    assert_eq!(*view.runtime.committed(view.route).unwrap(), 0);
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 1);
    assert_eq!(
        view.sink.patches(),
        vec![vec!["A0 route 0"], vec!["A1 route 0"]]
    );
    let replaced = view.sink.0[0]
        .ops
        .iter()
        .find_map(|op| match op {
            FrameOp::Patch(owner, _) => Some(*owner),
            _ => None,
        })
        .unwrap();
    assert!(
        matches!(&view.sink.0[1].ops[..], [FrameOp::Attach { owner, .. }, FrameOp::Patch(patched, _), FrameOp::SetVisible(visible,true), FrameOp::Remove(old)] if owner == patched && owner == visible && *old == replaced)
    );
    Job::complete(&view.replacement, 3);
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(view.sink.patches()[2], vec!["B 3"]);
}

#[test]
fn projected_answer_can_publish_while_the_same_memos_main_question_waits() {
    let main = Rc::new(RefCell::new(Job::default()));
    let frame = Rc::new(RefCell::new(Job::default()));
    let mut view = VisibleBranch::waiting({
        let (main, frame) = (main.clone(), frame.clone());
        move |branch, route, label| {
            let (main, frame) = (main.clone(), frame.clone());
            let result = branch.memo(move |cx| {
                let pair = (*cx.get(route)?, *cx.get(label)?);
                Ok(match pair {
                    (_, 0) => Derive::Value(0),
                    (0, _) => Derive::Future(Box::pin(Manual(frame.clone()))),
                    _ => Derive::Future(Box::pin(Manual(main.clone()))),
                })
            });
            branch.render(move |cx| Ok(*cx.get(result)?), |v| format!("A {v}"));
        }
    });
    view.rename(1);
    assert!(
        frame.borrow().waker.is_some(),
        "visible A must start its own job"
    );
    assert!(main.borrow().waker.is_some());
    Job::complete(&frame, 11);
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 1);
    assert_eq!(view.sink.patches(), vec![vec!["A 0"], vec!["A 11"]]);
    Job::complete(&main, 99);
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(view.sink.0.len(), 2);
}

#[test]
fn projected_stream_resumes_each_accepted_item_and_retires_with_its_owner() {
    struct Items {
        polls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
    }
    impl Stream for Items {
        type Item = JobResult<i32>;
        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let count = self.polls.get() + 1;
            self.polls.set(count);
            Poll::Ready(Some(Ok(count as i32)))
        }
    }
    impl Drop for Items {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }
    let (polls, drops) = (Rc::new(Cell::new(0)), Rc::new(Cell::new(0)));
    let mut view = VisibleBranch::waiting({
        let (polls, drops) = (polls.clone(), drops.clone());
        move |branch, route, label| {
            let (polls, drops) = (polls.clone(), drops.clone());
            let result = branch.memo(move |cx| {
                let pair = (*cx.get(route)?, *cx.get(label)?);
                Ok(if pair == (0, 1) {
                    Derive::Stream(Box::pin(Items {
                        polls: polls.clone(),
                        drops: drops.clone(),
                    }))
                } else {
                    Derive::Value(0)
                })
            });
            branch.render(move |cx| Ok(*cx.get(result)?), |v| format!("A {v}"));
        }
    });
    view.rename(1);
    assert_eq!(polls.get(), 1);
    assert_eq!(view.sink.patches()[1], vec!["A 1"]);
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(view.sink.patches()[2], vec!["A 2"]);
    Job::complete(&view.replacement, 50);
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(polls.get(), 2);
    assert_eq!(drops.get(), 1);
    assert_eq!(view.sink.patches()[3], vec!["B 50"]);
}

#[test]
fn disposing_a_visible_branch_drops_its_pending_projected_job_immediately() {
    let job = Rc::new(RefCell::new(Job::default()));
    let owner = Rc::new(Cell::new(None));
    let mut view = VisibleBranch::waiting({
        let (job, owner) = (job.clone(), owner.clone());
        move |branch, route, label| {
            owner.set(Some(branch.owner()));
            let job = job.clone();
            let result = branch.memo(move |cx| {
                Ok(if (*cx.get(route)?, *cx.get(label)?) == (0, 1) {
                    Derive::Future(Box::pin(Manual(job.clone())))
                } else {
                    Derive::Value(0)
                })
            });
            branch.render(move |cx| Ok(*cx.get(result)?), |v| v.to_string());
        }
    });
    view.rename(1);
    let wake = job.borrow().waker.clone().unwrap();
    view.runtime.dispose(owner.get().unwrap()).unwrap();
    assert_eq!(job.borrow().drops, 1);
    wake.wake();
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 1);
}

#[test]
fn projected_future_survives_the_commit_of_its_loading_fallback() {
    let job = Rc::new(RefCell::new(Job::default()));
    let attempts = Rc::new(Cell::new(0));
    let mut view = VisibleBranch::waiting({
        let job = job.clone();
        let attempts = attempts.clone();
        move |branch, route, label| {
            let job = job.clone();
            let attempts = attempts.clone();
            let result = branch.memo(move |cx| {
                Ok(if (*cx.get(route)?, *cx.get(label)?) == (0, 1) {
                    attempts.set(attempts.get() + 1);
                    Derive::Future(Box::pin(Manual(job.clone())))
                } else {
                    Derive::Value(0)
                })
            });
            branch.loading(
                move |cx| {
                    cx.get(label)?;
                    Ok(())
                },
                move |content| {
                    content.render(move |cx| Ok(*cx.get(result)?), |value| format!("A {value}"));
                },
                |fallback| {
                    fallback.render(|_| Ok(()), |_| String::from("waiting"));
                },
            );
        }
    });
    view.rename(1);
    assert_eq!(view.sink.patches()[1], vec!["waiting"]);
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 1);
    Job::complete(&job, 55);
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(view.sink.patches()[2], vec!["A 55"]);
    assert_eq!(attempts.get(), 1);
    assert_eq!(*view.runtime.committed(view.route).unwrap(), 0);
}

#[test]
fn projected_stream_is_dropped_when_an_independent_write_replaces_its_question() {
    struct Waiting {
        job: Rc<RefCell<Job>>,
        first: bool,
    }
    impl Stream for Waiting {
        type Item = JobResult<i32>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.first {
                self.first = false;
                Poll::Ready(Some(Ok(31)))
            } else {
                self.job.borrow_mut().waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
    impl Drop for Waiting {
        fn drop(&mut self) {
            self.job.borrow_mut().drops += 1;
        }
    }
    let job = Rc::new(RefCell::new(Job::default()));
    let mut view = VisibleBranch::waiting({
        let job = job.clone();
        move |branch, route, label| {
            let job = job.clone();
            let result = branch.memo(move |cx| {
                Ok(match (*cx.get(route)?, *cx.get(label)?) {
                    (0, 1) => Derive::Stream(Box::pin(Waiting {
                        job: job.clone(),
                        first: true,
                    })),
                    (_, label) => Derive::Value(label),
                })
            });
            branch.render(move |cx| Ok(*cx.get(result)?), |v| format!("A {v}"));
        }
    });
    view.rename(1);
    view.runtime.flush(&mut view.sink).unwrap();
    assert!(job.borrow().waker.is_some());
    view.rename(2);
    assert_eq!(job.borrow().drops, 1);
    assert_eq!(view.sink.patches()[2], vec!["A 2"]);
}

#[test]
fn superseding_a_pending_projected_tab_retires_the_unpublished_owner() {
    let job = Rc::new(RefCell::new(Job::default()));
    let retired = Rc::new(Cell::new(false));
    let mut view = VisibleBranch::waiting({
        let (job, retired) = (job.clone(), retired.clone());
        move |branch, route, label| {
            let (job, retired) = (job.clone(), retired.clone());
            branch.switch(
                move |cx| {
                    Ok(if *cx.get(route)? == 0 {
                        *cx.get(label)?
                    } else {
                        0
                    })
                },
                move |tab, key| {
                    let key = *key;
                    if key == 1 {
                        let (job, retired) = (job.clone(), retired.clone());
                        tab.on_cleanup(move || retired.set(true));
                        let pending =
                            tab.memo(move |_| Ok(Derive::Future(Box::pin(Manual(job.clone())))));
                        tab.render(
                            move |cx| Ok(*cx.get(pending)?),
                            |v| format!("unpublished {v}"),
                        );
                    } else {
                        tab.render(move |_| Ok(key), |v| format!("tab {v}"));
                    }
                },
            );
        }
    });
    view.rename(1);
    assert_eq!(view.sink.patches(), vec![vec!["tab 0"]]);
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 0);
    let stale = job.borrow().waker.clone().unwrap();
    view.rename(2);
    assert!(retired.get());
    assert_eq!(job.borrow().drops, 1);
    stale.wake();
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(view.sink.patches(), vec![vec!["tab 0"], vec!["tab 2"]]);
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 2);
    assert_eq!(*view.runtime.committed(view.route).unwrap(), 0);
}

#[test]
fn loading_in_a_projected_tab_recovers_its_first_content_after_attach() {
    let job = Rc::new(RefCell::new(Job::default()));
    let mut view = VisibleBranch::waiting({
        let job = job.clone();
        move |branch, route, label| {
            let job = job.clone();
            branch.switch(
                move |cx| {
                    Ok(if *cx.get(route)? == 0 {
                        *cx.get(label)?
                    } else {
                        0
                    })
                },
                move |tab, key| {
                    if *key == 0 {
                        tab.render(|_| Ok(()), |_| String::from("tab 0"));
                        return;
                    }
                    let job = job.clone();
                    let pending =
                        tab.memo(move |_| Ok(Derive::Future(Box::pin(Manual(job.clone())))));
                    tab.loading(
                        |_| Ok(()),
                        move |content| {
                            content.render(
                                move |cx| Ok(*cx.get(pending)?),
                                |v| format!("content {v}"),
                            );
                        },
                        |fallback| {
                            fallback.render(|_| Ok(()), |_| String::from("waiting"));
                        },
                    );
                },
            );
        }
    });
    view.rename(1);
    assert_eq!(view.sink.patches()[1], vec!["waiting"]);
    assert_eq!(*view.runtime.committed(view.label).unwrap(), 1);
    Job::complete(&job, 40);
    view.runtime.flush(&mut view.sink).unwrap();
    assert_eq!(view.sink.patches().last().unwrap(), &vec!["content 40"]);
}
