use std::{
    cell::{Cell, RefCell},
    future::Future,
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use signals::{
    Derive, Drive, Failure, Frame, FrameOp, FrameSink, JobResult, Register, RevealOrder, Runtime,
};

#[derive(Default)]
struct Job<T> {
    answer: Option<JobResult<T>>,
    wake: Option<Waker>,
}

fn failure(text: &str) -> Failure {
    Rc::new(io::Error::other(text))
}

impl<T> Job<T> {
    fn finish(job: &RefCell<Self>, answer: JobResult<T>) {
        let wake = {
            let mut job = job.borrow_mut();
            job.answer = Some(answer);
            job.wake.take().expect("job polled")
        };
        wake.wake();
    }
}

struct Manual<T>(Rc<RefCell<Job<T>>>);

impl<T> Future for Manual<T> {
    type Output = JobResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut job = self.0.borrow_mut();
        if let Some(answer) = job.answer.take() {
            Poll::Ready(answer)
        } else {
            job.wake = Some(cx.waker().clone());
            Poll::Pending
        }
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
                        FrameOp::Patch(_, text) => Some(text.as_str()),
                        _ => None,
                    })
                    .collect()
            })
            .collect()
    }
}

#[test]
fn initial_loading_and_a_same_tick_write_publish_together() {
    let job = Rc::new(RefCell::new(Job::default()));
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut write = None;
    runtime
        .install(|root| {
            let (read, set) = root.signal(0);
            write = Some(set);
            let job = Rc::clone(&job);
            let result = root.memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job))))));
            root.render(move |cx| Ok(*cx.get(read)?), |n| format!("write {n}"));
            root.loading(
                |_| Ok(()),
                move |content| content.render(move |cx| Ok(*cx.get(result)?), |n| format!("{n}")),
                |fallback| fallback.render(|_| Ok(()), |_| "waiting".to_owned()),
            );
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.set(write.unwrap(), 1).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec!["write 1", "waiting"]]);

    Job::finish(&job, Ok(3));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec!["write 1", "waiting"], vec!["3"]]);
}

#[test]
fn loading_does_not_absorb_a_reader_outside_its_content() {
    let job = Rc::new(RefCell::new(Job::default()));
    let mut runtime = Runtime::new(|| {}).unwrap();
    runtime
        .install(|root| {
            let job = Rc::clone(&job);
            let result = root.memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job))))));
            root.loading(
                |_| Ok(()),
                move |content| {
                    content.render(move |cx| Ok(*cx.get(result)?), |n| format!("inside {n}"))
                },
                |fallback| fallback.render(|_| Ok(()), |_| "waiting".to_owned()),
            );
            root.render(move |cx| Ok(*cx.get(result)?), |n| format!("outside {n}"));
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert!(sink.0.is_empty());

    Job::finish(&job, Ok(7));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec!["inside 7", "outside 7"]]);
}

#[test]
fn loading_rearms_from_on_versions_and_keeps_content_state_and_effects() {
    let jobs = [
        Rc::new(RefCell::new(Job::default())),
        Rc::new(RefCell::new(Job::default())),
    ];
    let effect = Rc::new(RefCell::new(Vec::new()));
    let content_owner = Rc::new(Cell::new(None));
    let local_write = Rc::new(Cell::new(None));
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut inputs = None;
    runtime
        .install(|root| {
            let (request, send) = root.signal(0);
            let (armed, arm) = root.signal(0);
            inputs = Some((request, send, arm));
            let jobs = jobs.clone();
            let result = root.memo(move |cx| {
                let request = *cx.get(request)?;
                Ok(match request {
                    0 => Derive::Value(10),
                    1 => Derive::Future(Box::pin(Manual(Rc::clone(&jobs[0])))),
                    _ => Derive::Future(Box::pin(Manual(Rc::clone(&jobs[1])))),
                })
            });
            let effect = Rc::clone(&effect);
            let content_owner = Rc::clone(&content_owner);
            let local_write = Rc::clone(&local_write);
            root.loading(
                move |cx| cx.get(armed).map(|_| ()),
                move |content| {
                    content_owner.set(Some(content.owner()));
                    let (local, write) = content.signal(0);
                    local_write.set(Some(write));
                    content.render(
                        move |cx| Ok((*cx.get(result)?, *cx.get(local)?)),
                        |v| format!("{v:?}"),
                    );
                    let effect = Rc::clone(&effect);
                    content.effect(
                        move |cx| Ok(*cx.get(local)?),
                        move |_, n| {
                            effect.borrow_mut().push(*n);
                            Ok(())
                        },
                    );
                },
                |fallback| fallback.render(|_| Ok(()), |_| "waiting".to_owned()),
            );
        })
        .unwrap();
    let (request, send, arm) = inputs.unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    let content_owner = content_owner.get().unwrap();
    assert_eq!(sink.patches(), vec![vec!["(10, 0)"]]);

    runtime.set(send, 1).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(request).unwrap(), 0);
    assert_eq!(sink.0.len(), 1);
    Job::finish(&jobs[0], Ok(11));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[1], vec!["(11, 0)"]);

    runtime.set(send, 2).unwrap();
    runtime.set(arm, 1).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(request).unwrap(), 2);
    assert_eq!(sink.patches()[2], vec!["waiting"]);
    assert!(sink.0[2]
        .ops
        .iter()
        .any(|op| matches!(op, FrameOp::SetVisible(owner, false) if *owner == content_owner)));

    let local_write = local_write.get().unwrap();
    for value in [7, 8] {
        runtime.set(local_write, value).unwrap();
        runtime.flush(&mut sink).unwrap();
    }
    assert_eq!(sink.0.len(), 3);
    assert_eq!(*effect.borrow(), vec![0]);

    Job::finish(&jobs[1], Ok(12));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[3], vec!["(12, 8)"]);
    assert_eq!(*effect.borrow(), vec![0, 8]);
    assert!(sink.0[3]
        .ops
        .iter()
        .any(|op| matches!(op, FrameOp::SetVisible(owner, true) if *owner == content_owner)));
}

#[test]
fn errored_keeps_its_fallback_state_and_recovers_to_the_same_value() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let job = Rc::new(RefCell::new(Job::default()));
    let fallback_builds = Rc::new(Cell::new(0));
    let fallback_cleanups = Rc::new(Cell::new(0));
    let mut write = None;
    runtime
        .install(|root| {
            let (mode, set) = root.signal(0);
            write = Some(set);
            let job = Rc::clone(&job);
            let result = root.memo(move |cx| match *cx.get(mode)? {
                0 | 4 => Ok(Derive::Value(42)),
                1 => Err(signals::ReadError::Failed(failure("first"))),
                2 => Err(signals::ReadError::Failed(failure("second"))),
                _ => Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job))))),
            });
            let fallback_builds = Rc::clone(&fallback_builds);
            let fallback_cleanups = Rc::clone(&fallback_cleanups);
            root.errored(
                move |content| {
                    content.render(move |cx| Ok(*cx.get(result)?), |n| format!("value {n}"))
                },
                move |fallback, error| {
                    fallback_builds.set(fallback_builds.get() + 1);
                    let fallback_cleanups = Rc::clone(&fallback_cleanups);
                    fallback.on_cleanup(move || fallback_cleanups.set(fallback_cleanups.get() + 1));
                    fallback.render(
                        move |cx| Ok(cx.get(error)?.to_string()),
                        |n| format!("error {n}"),
                    );
                },
            );
            root.render(
                move |cx| {
                    Ok(match cx.get(result) {
                        Ok(_) => "match value",
                        Err(signals::ReadError::Pending(_)) => "match pending",
                        Err(signals::ReadError::Failed(_)) => "match failure",
                        Err(error) => return Err(error),
                    })
                },
                |text| text.to_string(),
            );
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    let write = write.unwrap();
    for mode in 1..=3 {
        runtime.set(write, mode).unwrap();
        runtime.flush(&mut sink).unwrap();
    }
    assert_eq!(
        sink.patches(),
        vec![
            vec!["value 42", "match value"],
            vec!["match failure", "error first"],
            vec!["error second"],
            vec!["match pending"],
        ]
    );
    assert_eq!(fallback_builds.get(), 1);
    assert_eq!(fallback_cleanups.get(), 0);

    Job::finish(&job, Ok(42));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[4], vec!["value 42", "match value"]);
    assert_eq!(fallback_cleanups.get(), 1);
}

#[test]
fn together_uses_a_nested_sequential_groups_minimum_readiness() {
    let first = Rc::new(RefCell::new(Job::default()));
    let second = Rc::new(RefCell::new(Job::default()));
    let peer = Rc::new(RefCell::new(Job::default()));
    let mut runtime = Runtime::new(|| {}).unwrap();
    runtime
        .install(|root| {
            let first = Rc::clone(&first);
            let second = Rc::clone(&second);
            let peer = Rc::clone(&peer);
            root.reveal(RevealOrder::Together, false, |group| {
                group.reveal(RevealOrder::Sequential, false, |nested| {
                    pending_slot(nested, first, "first");
                    pending_slot(nested, second, "second");
                });
                pending_slot(group, peer, "peer");
            });
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(
        sink.patches(),
        vec![vec!["waiting first", "waiting second", "waiting peer"]]
    );
    Job::finish(&first, Ok(1));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.0.len(), 1);

    Job::finish(&peer, Ok(3));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(
        sink.patches(),
        vec![
            vec!["waiting first", "waiting second", "waiting peer"],
            vec!["first 1", "peer 3"],
        ]
    );
    Job::finish(&second, Ok(2));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[2], vec!["second 2"]);
}

fn pending_slot(reg: &mut impl Register<String>, job: Rc<RefCell<Job<i32>>>, label: &'static str) {
    reg.loading(
        |_| Ok(()),
        move |content| {
            let job = Rc::clone(&job);
            let value =
                content.memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job))))));
            content.render(
                move |cx| Ok(*cx.get(value)?),
                move |n| format!("{label} {n}"),
            );
        },
        move |fallback| fallback.render(|_| Ok(()), move |_| format!("waiting {label}")),
    );
}

#[test]
fn an_effect_apply_failure_reaches_the_nearest_errored_on_the_next_tick() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut write = None;
    runtime
        .install(|root| {
            let (value, set) = root.signal(0);
            write = Some(set);
            root.errored(
                move |content| {
                    content.render(move |cx| Ok(*cx.get(value)?), |n| format!("value {n}"));
                    content.effect(
                        move |cx| Ok(*cx.get(value)?),
                        |_, n| {
                            if *n == 0 {
                                Err(failure("apply"))
                            } else {
                                Ok(())
                            }
                        },
                    );
                },
                |fallback, error| {
                    fallback.render(
                        move |cx| Ok(cx.get(error)?.to_string()),
                        |n| format!("error {n}"),
                    );
                },
            );
        })
        .unwrap();
    let mut sink = Recorder::default();
    assert!(runtime.flush(&mut sink).is_ok());
    assert_eq!(sink.patches(), vec![vec!["value 0"]]);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[1], vec!["error apply"]);

    runtime.set(write.unwrap(), 1).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[2], vec!["value 1"]);
}

#[test]
fn an_unhandled_effect_apply_failure_poisons_the_next_tick() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    runtime
        .install(|root| {
            root.render(|_| Ok(()), |_| "published".to_owned());
            root.effect(|_| Ok(()), |_, _| Err(failure("apply")));
        })
        .unwrap();
    let mut sink = Recorder::default();
    assert!(runtime.flush(&mut sink).is_ok());
    assert_eq!(sink.patches(), vec![vec!["published"]]);
    assert!(matches!(
        runtime.flush(&mut sink),
        Err(signals::FlushError::Poisoned)
    ));
    assert_eq!(sink.0.len(), 1);
}

#[test]
fn a_hidden_loading_reader_does_not_join_an_external_hold_to_a_later_write() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let slow = Rc::new(RefCell::new(Job::default()));
    let outside = Rc::new(RefCell::new(Job::default()));
    let mut writes = None;
    runtime
        .install(|root| {
            let (blocked, block) = root.signal(false);
            let (label, rename) = root.signal("old");
            writes = Some((blocked, block, label, rename));
            let slow = Rc::clone(&slow);
            let outside = Rc::clone(&outside);
            let external = root.memo(move |cx| {
                Ok(if *cx.get(blocked)? {
                    Derive::Future(Box::pin(Manual(Rc::clone(&outside))))
                } else {
                    Derive::Value(0)
                })
            });
            root.render(
                move |cx| Ok(*cx.get(external)?),
                |value| format!("outside {value}"),
            );
            root.render(
                move |cx| Ok(*cx.get(label)?),
                |value| format!("label {value}"),
            );
            root.loading(
                |_| Ok(()),
                move |content| {
                    let slow = Rc::clone(&slow);
                    let value = content
                        .memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&slow))))));
                    content.render(
                        move |cx| Ok((*cx.get(blocked)?, *cx.get(label)?, *cx.get(value)?)),
                        |display| format!("{display:?}"),
                    );
                },
                |fallback| fallback.render(|_| Ok(()), |_| "waiting".to_owned()),
            );
        })
        .unwrap();
    let (blocked, block, label, rename) = writes.unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(
        sink.patches(),
        vec![vec!["outside 0", "label old", "waiting"]]
    );
    runtime.set(block, true).unwrap();
    runtime.flush(&mut sink).unwrap();
    runtime.set(rename, "new").unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(label).unwrap(), "new");
    assert!(!*runtime.committed(blocked).unwrap());
    assert_eq!(sink.patches()[1], vec!["label new"]);
    Job::finish(&outside, Ok(1));
    Job::finish(&slow, Ok(2));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(
        sink.patches()[2..],
        [vec!["outside 1"], vec!["(true, \"new\", 2)"]]
    );
}

#[test]
fn a_failed_answer_waits_for_another_reader_in_its_frame() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let broken = Rc::new(RefCell::new(Job::default()));
    let blocker = Rc::new(RefCell::new(Job::default()));
    let mut handles = None;
    runtime
        .install(|root| {
            let (request, set) = root.signal(false);
            handles = Some((request, set));
            let broken = Rc::clone(&broken);
            let blocker = Rc::clone(&blocker);
            let failed = root.memo(move |cx| {
                Ok(if *cx.get(request)? {
                    Derive::Future(Box::pin(Manual(Rc::clone(&broken))))
                } else {
                    Derive::Value(1)
                })
            });
            let outside = root.memo(move |cx| {
                Ok(if *cx.get(request)? {
                    Derive::Future(Box::pin(Manual(Rc::clone(&blocker))))
                } else {
                    Derive::Value(2)
                })
            });
            root.errored(
                move |reg| reg.render(move |cx| Ok(*cx.get(failed)?), |n| format!("inside {n}")),
                |reg, error| {
                    reg.render(
                        move |cx| Ok(cx.get(error)?.to_string()),
                        |n| format!("error {n}"),
                    )
                },
            );
            root.render(move |cx| Ok(*cx.get(outside)?), |n| format!("outside {n}"));
        })
        .unwrap();
    let (request, write) = handles.unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    runtime.set(write, true).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec!["inside 1", "outside 2"]]);

    Job::finish(&broken, Err(failure("broken")));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.0.len(), 1);
    assert!(!*runtime.committed(request).unwrap());

    Job::finish(&blocker, Ok(3));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[1], vec!["outside 3", "error broken"]);
    assert!(*runtime.committed(request).unwrap());
}

#[test]
fn loading_on_pending_waits_outside_the_boundary() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let on_job = Rc::new(RefCell::new(Job::default()));
    let content_job = Rc::new(RefCell::new(Job::default()));
    runtime
        .install(|root| {
            let job = Rc::clone(&on_job);
            let on = root.memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job))))));
            let job = Rc::clone(&content_job);
            let display = root.memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job))))));
            root.loading(
                move |cx| cx.get(on).map(|_| ()),
                move |reg| reg.render(move |cx| Ok(*cx.get(display)?), |n| format!("value {n}")),
                |reg| reg.render(|_| Ok(()), |_| "waiting".to_owned()),
            );
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert!(sink.0.is_empty());
    Job::finish(&on_job, Ok(()));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec!["waiting"]]);
    Job::finish(&content_job, Ok(1));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[1], vec!["value 1"]);
}

#[test]
fn visible_content_attaches_before_its_existing_fallback_sibling() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let job = Rc::new(RefCell::new(Job::default()));
    let content = Rc::new(Cell::new(None));
    let fallback = Rc::new(Cell::new(None));
    runtime
        .install(|root| {
            let job = Rc::clone(&job);
            let value = root.memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&job))))));
            let content = Rc::clone(&content);
            let fallback = Rc::clone(&fallback);
            root.loading(
                |_| Ok(()),
                move |reg| {
                    content.set(Some(reg.owner()));
                    reg.render(move |cx| Ok(*cx.get(value)?), |n| format!("ready {n}"));
                },
                move |reg| {
                    fallback.set(Some(reg.owner()));
                    reg.render(|_| Ok(()), |_| "waiting".to_owned());
                },
            );
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    let content = content.get().unwrap();
    let fallback = fallback.get().unwrap();
    assert!(sink.0[0]
        .ops
        .iter()
        .all(|op| !matches!(op, FrameOp::Attach { owner, .. } if *owner == content)));

    Job::finish(&job, Ok(9));
    runtime.flush(&mut sink).unwrap();
    assert!(matches!(
        sink.0[1].ops.as_slice(),
        [
            FrameOp::Attach { owner, parent: Some(_), before: Some(before) },
            FrameOp::Patch(patched, patch),
            FrameOp::SetVisible(shown, true),
            FrameOp::SetVisible(hidden, false),
        ] if *owner == content && *before == fallback && *patched == content && patch == "ready 9"
          && *shown == content && *hidden == fallback
    ));
}

#[test]
fn recovering_errored_removes_fallback_descendants_before_their_parents() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let fallback = Rc::new(Cell::new(None));
    let child = Rc::new(Cell::new(None));
    let mut write = None;
    runtime
        .install(|root| {
            let (ready, set) = root.signal(false);
            write = Some(set);
            let fallback = Rc::clone(&fallback);
            let child = Rc::clone(&child);
            root.errored(
                move |reg| {
                    reg.render(
                        move |cx| {
                            if *cx.get(ready)? {
                                Ok(())
                            } else {
                                Err(signals::ReadError::Failed(failure("missing")))
                            }
                        },
                        |_| "ready".to_owned(),
                    );
                },
                move |reg, _| {
                    fallback.set(Some(reg.owner()));
                    let child = Rc::clone(&child);
                    reg.switch(
                        |_| Ok(()),
                        move |reg, _| {
                            child.set(Some(reg.owner()));
                            reg.render(|_| Ok(()), |_| "fallback child".to_owned());
                        },
                    );
                },
            );
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    let child = child.get().unwrap();
    let fallback = fallback.get().unwrap();
    let wrapper = sink.0[0]
        .ops
        .iter()
        .find_map(|op| match op {
            FrameOp::Attach { owner, parent, .. } if *owner == child => *parent,
            _ => None,
        })
        .unwrap();
    runtime.set(write.unwrap(), true).unwrap();
    runtime.flush(&mut sink).unwrap();
    let removed: Vec<_> = sink.0[1]
        .ops
        .iter()
        .filter_map(|op| match op {
            FrameOp::Remove(owner) => Some(*owner),
            _ => None,
        })
        .collect();
    assert_eq!(removed, vec![child, wrapper, fallback]);
}

#[test]
fn a_rearmed_slot_does_not_resume_control_of_its_collapsed_tail() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let first = Rc::new(RefCell::new(Job::default()));
    let second = Rc::new(RefCell::new(Job::default()));
    let mut write = None;
    runtime
        .install(|root| {
            let (reset, set) = root.signal(false);
            write = Some(set);
            let first = Rc::clone(&first);
            let second = Rc::clone(&second);
            root.reveal(RevealOrder::Sequential, true, |group| {
                group.loading(
                    move |cx| cx.get(reset).map(|_| ()),
                    move |content| {
                        let first = Rc::clone(&first);
                        let value = content.memo(move |cx| {
                            Ok(if *cx.get(reset)? {
                                Derive::Future(Box::pin(Manual(Rc::clone(&first))))
                            } else {
                                Derive::Value(1)
                            })
                        });
                        content.render(move |cx| Ok(*cx.get(value)?), |n| format!("first {n}"));
                    },
                    |fallback| fallback.render(|_| Ok(()), |_| "waiting first".to_owned()),
                );
                group.errored(
                    move |content| pending_slot(content, Rc::clone(&second), "second"),
                    |_, _| panic!("Pending must not open Errored"),
                );
                group.loading(
                    |_| Ok(()),
                    |content| content.render(|_| Ok(()), |_| "third".to_owned()),
                    |fallback| fallback.render(|_| Ok(()), |_| "waiting third".to_owned()),
                );
            });
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec!["first 1", "waiting second"]]);

    runtime.set(write.unwrap(), true).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[1], vec!["waiting first"]);

    Job::finish(&second, Ok(2));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[2], vec!["second 2", "third"]);
}

#[test]
fn disposing_a_waiting_reveal_slot_releases_its_ready_tail() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let first = Rc::new(RefCell::new(Job::default()));
    let fallback = Rc::new(Cell::new(None));
    runtime
        .install(|root| {
            let first = Rc::clone(&first);
            let fallback = Rc::clone(&fallback);
            root.reveal(RevealOrder::Sequential, true, |group| {
                group.loading(
                    |_| Ok(()),
                    move |content| {
                        let first = Rc::clone(&first);
                        let result = content
                            .memo(move |_| Ok(Derive::Future(Box::pin(Manual(Rc::clone(&first))))));
                        content.render(
                            move |cx| Ok(*cx.get(result)?),
                            |n: Rc<i32>| format!("first {n}"),
                        );
                    },
                    move |reg| {
                        fallback.set(Some(reg.owner()));
                        reg.render(|_| Ok(()), |_| "waiting first".to_owned());
                    },
                );
                group.loading(
                    |_| Ok(()),
                    |content| content.render(|_| Ok(()), |_| "ready second".to_owned()),
                    |fallback| fallback.render(|_| Ok(()), |_| "waiting second".to_owned()),
                );
            });
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec!["waiting first"]]);
    let fallback = fallback.get().unwrap();
    let slot = sink.0[0]
        .ops
        .iter()
        .find_map(|op| match op {
            FrameOp::Attach { owner, parent, .. } if *owner == fallback => *parent,
            _ => None,
        })
        .unwrap();
    runtime.dispose(slot).unwrap();
    assert_eq!(sink.0.len(), 1);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches()[1], vec!["ready second"]);
}

#[test]
fn a_failure_in_the_fallback_reaches_the_outer_error_boundary() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    runtime
        .install(|root| {
            root.errored(
                |outer| {
                    outer.errored(
                        |inner| {
                            inner.switch(
                                |_| Err::<(), _>(signals::ReadError::Failed(failure("switch key"))),
                                |_, _| panic!("failed switch must not build"),
                            )
                        },
                        |fallback, error| {
                            fallback.loading(
                                move |cx| {
                                    assert_eq!(cx.get(error)?.to_string(), "switch key");
                                    Err(signals::ReadError::Failed(failure("fallback on")))
                                },
                                |_| {},
                                |_| panic!("Failed must not open Loading"),
                            )
                        },
                    );
                },
                |fallback, error| {
                    fallback.render(
                        move |cx| Ok(cx.get(error)?.to_string()),
                        |n| format!("outer {n}"),
                    );
                },
            );
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec!["outer fallback on"]]);
}
