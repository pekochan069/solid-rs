use std::{
    cell::{Cell, RefCell},
    io,
    panic::{catch_unwind, AssertUnwindSafe},
    rc::Rc,
};

use signals::{
    Derive, Drive, Failure, FlushError, Frame, FrameOp, FrameSink, HandleError, ReadError, ReadRef,
    Register, Runtime,
};

#[derive(Default)]
struct Recorder<P> {
    frames: Vec<Frame<P>>,
}

impl<P> FrameSink<P> for Recorder<P> {
    fn publish(&mut self, frame: Frame<P>) -> Result<(), Failure> {
        self.frames.push(frame);
        Ok(())
    }
}

impl<P> Recorder<P> {
    fn patches(&self) -> Vec<Vec<&P>> {
        self.frames
            .iter()
            .map(|frame| {
                frame
                    .ops
                    .iter()
                    .filter_map(|op| match op {
                        FrameOp::Patch(_, patch) => Some(patch),
                        _ => None,
                    })
                    .collect()
            })
            .collect()
    }
}

#[test]
fn diamond_publishes_one_coherent_patch_after_batched_writes() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut handles = None;
    runtime
        .install(|root| {
            let (left, write_left) = root.signal(2);
            let (right, write_right) = root.signal(3);
            let sum = root.memo(move |cx| Ok(Derive::Value(*cx.get(left)? + *cx.get(right)?)));
            let product = root.memo(move |cx| Ok(Derive::Value(*cx.get(left)? * *cx.get(right)?)));
            let total = root.memo(move |cx| Ok(Derive::Value(*cx.get(sum)? + *cx.get(product)?)));
            root.render(
                move |cx| Ok((*cx.get(left)?, *cx.get(right)?, *cx.get(total)?)),
                |value| *value,
            );
            handles = Some((left, write_left, write_right, total));
        })
        .unwrap();
    let (left, write_left, write_right, total) = handles.unwrap();
    let mut sink = Recorder::default();
    assert!(matches!(
        runtime.committed(left),
        Err(ReadError::Pending(_))
    ));
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec![&(2, 3, 11)]]);

    runtime.set(write_left, 4).unwrap();
    runtime.update(write_left, |value| value + 1).unwrap();
    runtime.set(write_right, 7).unwrap();
    assert_eq!(*runtime.committed(left).unwrap(), 2);
    assert_eq!(*runtime.committed(total).unwrap(), 11);
    assert_eq!(sink.patches(), vec![vec![&(2, 3, 11)]]);

    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec![&(2, 3, 11)], vec![&(5, 7, 47)]]);
    assert_eq!(*runtime.committed(total).unwrap(), 47);
}

#[test]
fn manual_answer_tracks_the_new_question_after_a_selector_write() {
    let mut rt = Runtime::<()>::new(|| {}).unwrap();
    let mut handles = None;
    rt.install(|root| {
        let (selector, choose) = root.signal(true);
        let (a, set_a) = root.signal(10);
        let (b, set_b) = root.signal(20);
        let (value, set) = root.derived_signal(move |cx| {
            let source = if *cx.get(selector)? { a } else { b };
            Ok(Derive::Value(*cx.get(source)?))
        });
        handles = Some((choose, set_a, set_b, value, set));
    })
    .unwrap();
    let (choose, set_a, set_b, value, set) = handles.unwrap();
    let mut sink = Recorder::default();
    rt.flush(&mut sink).unwrap();
    rt.set(choose, false).unwrap();
    rt.set(set, 99).unwrap();
    rt.flush(&mut sink).unwrap();
    assert_eq!(*rt.committed(value).unwrap(), 99);
    rt.set(set_a, 11).unwrap();
    rt.flush(&mut sink).unwrap();
    assert_eq!(*rt.committed(value).unwrap(), 99);
    rt.set(set_b, 21).unwrap();
    rt.flush(&mut sink).unwrap();
    assert_eq!(*rt.committed(value).unwrap(), 21);
}

#[test]
fn an_equal_result_still_rewires_dynamic_dependencies() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut writes = None;
    let runs = Rc::new(Cell::new(0));
    runtime
        .install(|root| {
            let (choose_left, choose) = root.signal(true);
            let (left, write_left) = root.signal(10);
            let (right, write_right) = root.signal(10);
            let counter = Rc::clone(&runs);
            let selected = root.memo(move |cx| {
                counter.set(counter.get() + 1);
                let source = if *cx.get(choose_left)? { left } else { right };
                Ok(Derive::Value(*cx.get(source)?))
            });
            root.render(move |cx| Ok(*cx.get(selected)?), |value| *value);
            writes = Some((choose, write_left, write_right));
        })
        .unwrap();
    let (choose, write_left, write_right) = writes.unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    runtime.set(choose, false).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(runs.get(), 2);
    assert_eq!(sink.patches(), vec![vec![&10]]);

    runtime.set(write_left, 9).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(runs.get(), 2);
    runtime.set(write_right, 12).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(runs.get(), 3);
    assert_eq!(sink.patches(), vec![vec![&10], vec![&12]]);
}

#[test]
fn a_cycle_is_a_failed_answer_and_can_recover_when_an_input_changes() {
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    let mut handles = None;
    runtime
        .install(|root| {
            let edge = Rc::new(Cell::new(None::<ReadRef<i32>>));
            let (looping, toggle) = root.signal(true);
            let target = Rc::clone(&edge);
            let a = root.memo(move |cx| Ok(Derive::Value(*cx.get(target.get().unwrap())? + 1)));
            let b = root.memo(move |cx| {
                let value = if *cx.get(looping)? { *cx.get(a)? } else { 10 };
                Ok(Derive::Value(value))
            });
            edge.set(Some(b));
            handles = Some((a, b, toggle));
        })
        .unwrap();
    let (a, b, toggle) = handles.unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    for source in [a, b] {
        match runtime.committed(source) {
            Err(ReadError::Failed(error)) => assert_eq!(error.to_string(), "reactive cycle"),
            other => panic!("expected a failed cycle answer, got {other:?}"),
        }
    }

    runtime.set(toggle, false).unwrap();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(*runtime.committed(a).unwrap(), 11);
    assert_eq!(*runtime.committed(b).unwrap(), 10);
}

#[test]
fn foreign_and_retired_handles_cannot_read_or_mutate_reused_slots() {
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    let mut foreign = Runtime::<()>::new(|| {}).unwrap();
    let mut handles = None;
    let owner = runtime
        .install(|root| handles = Some(root.signal(String::from("old"))))
        .unwrap();
    let (old, write_old) = handles.unwrap();
    assert_eq!(
        foreign.set(write_old, String::from("wrong")),
        Err(HandleError::WrongRuntime)
    );
    assert_eq!(foreign.dispose(owner), Err(HandleError::WrongRuntime));
    assert!(matches!(
        foreign.committed(old),
        Err(ReadError::Handle(HandleError::WrongRuntime))
    ));
    runtime.set(write_old, String::from("queued")).unwrap();
    runtime.dispose(owner).unwrap();

    let replacement = runtime
        .install(|root| handles = Some(root.signal(String::from("current"))))
        .unwrap();
    let (current, _) = handles.unwrap();
    assert_ne!(owner, replacement);
    assert_eq!(runtime.dispose(owner), Err(HandleError::Disposed));
    assert_eq!(
        runtime.set(write_old, String::from("late")),
        Err(HandleError::Disposed)
    );
    assert!(matches!(
        runtime.committed(old),
        Err(ReadError::Handle(HandleError::Disposed))
    ));
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(runtime.committed(current).unwrap().as_str(), "current");
}

#[test]
fn disposal_releases_resources_in_reverse_order_without_a_frame() {
    struct ValueDrop(Rc<RefCell<Vec<u8>>>);

    impl PartialEq for ValueDrop {
        fn eq(&self, other: &Self) -> bool {
            Rc::ptr_eq(&self.0, &other.0)
        }
    }

    impl Drop for ValueDrop {
        fn drop(&mut self) {
            self.0.borrow_mut().push(2);
        }
    }

    let log = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    let owner = runtime
        .install(|root| {
            let first = Rc::clone(&log);
            root.on_cleanup(move || first.borrow_mut().push(1));
            root.signal(ValueDrop(Rc::clone(&log)));
            let last = Rc::clone(&log);
            root.on_cleanup(move || last.borrow_mut().push(3));
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    runtime.dispose(owner).unwrap();
    assert_eq!(*log.borrow(), vec![3, 2, 1]);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.frames.len(), 1);
}

#[test]
fn a_render_panic_is_rethrown_and_the_runtime_is_poisoned() {
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    let mut handle = None;
    runtime
        .install(|root| {
            let (source, _) = root.signal(1);
            root.render(|_| -> signals::Read<()> { panic!("render panic") }, |_| ());
            handle = Some(source);
        })
        .unwrap();
    let mut sink = Recorder::default();
    assert!(catch_unwind(AssertUnwindSafe(|| runtime.flush(&mut sink))).is_err());
    assert!(sink.frames.is_empty());
    assert!(matches!(
        runtime.committed(handle.unwrap()),
        Err(ReadError::Handle(HandleError::Poisoned))
    ));
    assert!(matches!(
        runtime.flush(&mut sink),
        Err(FlushError::Poisoned)
    ));
}

#[test]
fn a_rejected_frame_poisons_the_runtime_without_applying_effects() {
    struct Reject;

    impl FrameSink<()> for Reject {
        fn publish(&mut self, _: Frame<()>) -> Result<(), Failure> {
            Err(Rc::new(io::Error::other("frame rejected")))
        }
    }

    let applied = Rc::new(Cell::new(false));
    let mut runtime = Runtime::<()>::new(|| {}).unwrap();
    runtime
        .install(|root| {
            let result = Rc::clone(&applied);
            root.effect(
                |_| Ok(()),
                move |_, _| {
                    result.set(true);
                    Ok(())
                },
            );
        })
        .unwrap();
    match runtime.flush(&mut Reject) {
        Err(FlushError::Publish(error)) => assert_eq!(error.to_string(), "frame rejected"),
        other => panic!("expected publish error, got {other:?}"),
    }
    assert!(!applied.get());
    assert!(matches!(
        runtime.flush(&mut Recorder::default()),
        Err(FlushError::Poisoned)
    ));
}

#[test]
fn an_effect_proposes_its_write_for_the_next_tick() {
    let mut runtime = Runtime::new(|| {}).unwrap();
    let mut handle = None;
    runtime
        .install(|root| {
            let (input, _) = root.signal(7);
            let (output, write_output) = root.signal(0);
            root.render(move |cx| Ok(*cx.get(output)?), |value| *value);
            root.effect(
                move |cx| Ok(*cx.get(input)?),
                move |cx, value| {
                    cx.set(write_output, *value).unwrap();
                    Ok(())
                },
            );
            handle = Some(output);
        })
        .unwrap();
    let mut sink = Recorder::default();
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec![&0]]);
    assert_eq!(*runtime.committed(handle.unwrap()).unwrap(), 0);
    runtime.flush(&mut sink).unwrap();
    assert_eq!(sink.patches(), vec![vec![&0], vec![&7]]);
}
