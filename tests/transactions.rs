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
