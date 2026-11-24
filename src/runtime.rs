use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashSet},
    fmt,
    panic::{self, AssertUnwindSafe},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{arena::Arena, *};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

type Compute = Rc<dyn Fn(&mut Cx<'_>) -> Read<Value>>;
type Compare = fn(&Value, &Value) -> bool;
type Render<P> = Rc<dyn Fn(Value) -> P>;
type ApplyEffect = Box<dyn FnMut(&mut EffectCx<'_>, Value) -> JobResult<()>>;

#[derive(Clone)]
enum Answer {
    Value(Value),
    Failed(Failure),
}

impl Answer {
    fn read(&self) -> Read<Value> {
        match self {
            Self::Value(value) => Ok(Rc::clone(value)),
            Self::Failed(error) => Err(ReadError::Failed(Rc::clone(error))),
        }
    }

    fn same(&self, other: &Self, compare: Compare) -> bool {
        match (self, other) {
            (Self::Value(left), Self::Value(right)) => compare(left, right),
            (Self::Failed(left), Self::Failed(right)) => Rc::ptr_eq(left, right),
            _ => false,
        }
    }
}

struct Snapshot {
    answer: Answer,
    version: u64,
    reads: Vec<Dependency>,
}

enum Role<P> {
    Source,
    Memo,
    Render(Render<P>),
    Effect(Option<ApplyEffect>),
}

struct Node<P> {
    owner: Owner,
    ty: TypeId,
    compare: Compare,
    compute: Option<Compute>,
    role: Role<P>,
    committed: Option<Snapshot>,
    candidate: Option<Snapshot>,
    subscribers: HashSet<Key>,
    manual: Option<Value>,
    height: u32,
    queued: Option<u64>,
    running: bool,
}

impl<P> Node<P> {
    fn current(&self) -> Option<&Snapshot> {
        self.candidate.as_ref().or(self.committed.as_ref())
    }

    fn changed(&self) -> bool {
        self.candidate.as_ref().is_some_and(|candidate| {
            self.committed.as_ref().map(|s| s.version) != Some(candidate.version)
        })
    }
}

enum Resource {
    Node(Key),
    Cleanup(Box<dyn FnOnce()>),
}

struct OwnerState {
    children: Vec<Owner>,
    resources: Vec<Resource>,
}

enum Retired<P> {
    Node(Box<Node<P>>),
    Cleanup(Box<dyn FnOnce()>),
}

struct Store<P> {
    owners: Arena<OwnerState>,
    nodes: Arena<Node<P>>,
    roots: Vec<Owner>,
    attach: Vec<Owner>,
    dirty: BinaryHeap<Reverse<(u32, u64, Key)>>,
    next_enqueue: u64,
}

impl<P> Store<P> {
    fn new(id: u64) -> Self {
        Self {
            owners: Arena::new(id),
            nodes: Arena::new(id),
            roots: Vec::new(),
            attach: Vec::new(),
            dirty: BinaryHeap::new(),
            next_enqueue: 0,
        }
    }

    fn node(&self, key: Key) -> &Node<P> {
        self.nodes.get(key).expect("live node")
    }

    fn node_mut(&mut self, key: Key) -> &mut Node<P> {
        self.nodes.get_mut(key).expect("live node")
    }

    fn owner_mut(&mut self, owner: Owner) -> &mut OwnerState {
        self.owners.get_mut(owner.0).expect("live owner")
    }

    fn create_root(&mut self) -> Owner {
        let key = self.owners.insert(OwnerState {
            children: Vec::new(),
            resources: Vec::new(),
        });
        let owner = Owner(key, PhantomData);
        self.roots.push(owner);
        self.attach.push(owner);
        owner
    }

    fn schedule(&mut self, key: Key) {
        let node = self.node(key);
        if node.queued.is_some() || node.compute.is_none() {
            return;
        }
        let height = node.height;
        self.next_enqueue = self
            .next_enqueue
            .checked_add(1)
            .expect("queue revision exhausted");
        self.node_mut(key).queued = Some(self.next_enqueue);
        self.dirty.push(Reverse((height, self.next_enqueue, key)));
    }

    fn invalidate_subscribers(&mut self, source: Key) {
        let mut queue: Vec<_> = self.node(source).subscribers.iter().copied().collect();
        let mut seen = HashSet::new();
        while let Some(key) = queue.pop() {
            if let Some(node) = self.nodes.get(key) {
                if seen.insert(key) {
                    queue.extend(node.subscribers.iter().copied());
                    self.schedule(key);
                }
            }
        }
    }

    fn stage(&mut self, key: Key, answer: Answer, reads: Vec<Dependency>) -> bool {
        let node = self.node(key);
        let current = node.current();
        let previous_version = current.map_or(0, |s| s.version);
        let previous_answer = current.map(|snapshot| snapshot.answer.clone());
        let compare = node.compare;
        let changed = previous_answer.is_none_or(|previous| !answer.same(&previous, compare));
        let version = if changed {
            previous_version
                .checked_add(1)
                .expect("answer version exhausted")
        } else {
            previous_version
        };
        let node = self.node(key);
        if let Some(previous) = &node.candidate {
            let removed: Vec<_> = previous
                .reads
                .iter()
                .map(|dep| dep.source)
                .filter(|source| {
                    !reads.iter().any(|dep| dep.source == *source)
                        && !node
                            .committed
                            .as_ref()
                            .is_some_and(|s| s.reads.iter().any(|dep| dep.source == *source))
                })
                .collect();
            for source in removed {
                if let Some(node) = self.nodes.get_mut(source) {
                    node.subscribers.remove(&key);
                }
            }
        }
        for dep in &reads {
            if let Some(node) = self.nodes.get_mut(dep.source) {
                node.subscribers.insert(key);
            }
        }
        let previous = self.node_mut(key).candidate.replace(Snapshot {
            answer,
            version,
            reads,
        });
        drop(previous);
        changed
    }

    fn commit(&mut self, key: Key) -> Option<Snapshot> {
        let node = self.node_mut(key);
        let candidate = node.candidate.take().expect("prepared candidate");
        let previous = node.committed.replace(candidate);
        if let Some(previous) = &previous {
            for dep in &previous.reads {
                let snapshot = self.node(key).committed.as_ref().expect("committed answer");
                if !snapshot
                    .reads
                    .iter()
                    .any(|current| current.source == dep.source)
                {
                    if let Some(source) = self.nodes.get_mut(dep.source) {
                        source.subscribers.remove(&key);
                    }
                }
            }
        }
        previous
    }

    fn remove_tree(&mut self, owner: Owner, retired: &mut Vec<Retired<P>>) {
        let mut owners = Vec::new();
        let mut pending = vec![owner];
        while let Some(owner) = pending.pop() {
            if let Some(state) = self.owners.remove(owner.0) {
                pending.extend(state.children.iter().rev().copied());
                owners.push((owner, state));
            }
        }
        for (_, state) in owners.into_iter().rev() {
            for resource in state.resources.into_iter().rev() {
                match resource {
                    Resource::Node(key) => {
                        if self.nodes.get(key).is_some() {
                            self.invalidate_subscribers(key);
                            let node = self.nodes.remove(key).expect("live resource");
                            for dep in node
                                .committed
                                .iter()
                                .chain(&node.candidate)
                                .flat_map(|s| &s.reads)
                            {
                                if let Some(source) = self.nodes.get_mut(dep.source) {
                                    source.subscribers.remove(&key);
                                }
                            }
                            retired.push(Retired::Node(Box::new(node)));
                        }
                    }
                    Resource::Cleanup(cleanup) => retired.push(Retired::Cleanup(cleanup)),
                }
            }
        }
        self.roots.retain(|root| *root != owner);
    }
}

fn clean<P>(retired: Vec<Retired<P>>) {
    let mut failure = None;
    for resource in retired {
        let result = panic::catch_unwind(AssertUnwindSafe(move || match resource {
            Retired::Node(node) => drop(node),
            Retired::Cleanup(cleanup) => cleanup(),
        }));
        if let Err(payload) = result {
            if failure.is_none() {
                failure = Some(payload);
            }
        }
    }
    if let Some(payload) = failure {
        panic::resume_unwind(payload);
    }
}

pub struct Registration<P> {
    store: Store<P>,
    owner: Owner,
}

impl<P> Registration<P> {
    fn register<T: PartialEq + 'static>(
        &mut self,
        role: Role<P>,
        compute: Option<Compute>,
        initial: Option<T>,
    ) -> ReadRef<T> {
        let key = self.store.nodes.insert(Node {
            owner: self.owner,
            ty: TypeId::of::<T>(),
            compare: |left, right| left.downcast_ref::<T>() == right.downcast_ref::<T>(),
            compute,
            role,
            committed: None,
            candidate: initial.map(|value| Snapshot {
                answer: Answer::Value(Rc::new(value)),
                version: 1,
                reads: Vec::new(),
            }),
            subscribers: HashSet::new(),
            manual: None,
            height: 0,
            queued: None,
            running: false,
        });
        self.store
            .owner_mut(self.owner)
            .resources
            .push(Resource::Node(key));
        self.store.schedule(key);
        ReadRef(key, PhantomData)
    }
}

#[derive(Debug)]
struct Cycle;

impl fmt::Display for Cycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("reactive cycle")
    }
}

impl Error for Cycle {}

#[derive(Debug)]
struct AsyncUnavailable;

impl fmt::Display for AsyncUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("asynchronous computations are unavailable")
    }
}

impl Error for AsyncUnavailable {}

impl<P> Register<P> for Registration<P> {
    fn owner(&self) -> Owner {
        self.owner
    }

    fn signal<T: PartialEq + 'static>(&mut self, initial: T) -> (ReadRef<T>, WriteRef<T>) {
        let read = self.register(Role::Source, None, Some(initial));
        (read, WriteRef(read))
    }

    fn memo<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut Cx<'_>) -> Read<Derive<T>> + 'static,
    ) -> ReadRef<T> {
        self.register::<T>(
            Role::Memo,
            Some(Rc::new(move |cx| match compute(cx)? {
                Derive::Value(value) => Ok(Rc::new(value)),
                Derive::Future(_) | Derive::Stream(_) => {
                    Err(ReadError::Failed(Rc::new(AsyncUnavailable)))
                }
            })),
            None,
        )
    }

    fn derived_signal<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut Cx<'_>) -> Read<Derive<T>> + 'static,
    ) -> (ReadRef<T>, WriteRef<T>) {
        let read = self.memo(compute);
        (read, WriteRef(read))
    }

    fn render<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut Cx<'_>) -> Read<T> + 'static,
        apply: impl Fn(Rc<T>) -> P + 'static,
    ) {
        self.register::<T>(
            Role::Render(Rc::new(move |value| apply(downcast(value)))),
            Some(Rc::new(move |cx| Ok(Rc::new(compute(cx)?)))),
            None,
        );
    }

    fn effect<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut Cx<'_>) -> Read<T> + 'static,
        mut apply: impl FnMut(&mut EffectCx<'_>, Rc<T>) -> JobResult<()> + 'static,
    ) {
        self.register::<T>(
            Role::Effect(Some(Box::new(move |cx, value| apply(cx, downcast(value))))),
            Some(Rc::new(move |cx| Ok(Rc::new(compute(cx)?)))),
            None,
        );
    }

    fn on_cleanup(&mut self, cleanup: impl FnOnce() + 'static) {
        self.store
            .owner_mut(self.owner)
            .resources
            .push(Resource::Cleanup(Box::new(cleanup)));
    }
}

pub struct Runtime<P> {
    id: u64,
    store: Store<P>,
    writes: Vec<(Key, Value)>,
    wake_host: Box<dyn Fn() + Send + Sync>,
    wake_posted: bool,
    busy: bool,
    poisoned: bool,
    sequence: u64,
    local: PhantomData<Rc<()>>,
}

impl<P> Runtime<P> {
    pub fn new(wake_host: impl Fn() + Send + Sync + 'static) -> Result<Self, InitError> {
        let mut id = NEXT_RUNTIME_ID.load(Ordering::Relaxed);
        loop {
            let next = id.checked_add(1).ok_or(InitError::RuntimeIdsExhausted)?;
            match NEXT_RUNTIME_ID.compare_exchange_weak(
                id,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => id = current,
            }
        }
        Ok(Self {
            id,
            store: Store::new(id),
            writes: Vec::new(),
            wake_host: Box::new(wake_host),
            wake_posted: false,
            busy: false,
            poisoned: false,
            sequence: 0,
            local: PhantomData,
        })
    }

    fn guarded<T>(&mut self, action: impl FnOnce(&mut Self) -> T) -> T {
        match panic::catch_unwind(AssertUnwindSafe(|| action(self))) {
            Ok(result) => result,
            Err(payload) => {
                self.poisoned = true;
                self.busy = false;
                panic::resume_unwind(payload);
            }
        }
    }

    fn request_tick(&mut self) {
        if !self.wake_posted {
            self.wake_posted = true;
            self.guarded(|runtime| (runtime.wake_host)());
        }
    }

    fn check_key(&self, key: Key) -> Result<(), HandleError> {
        if self.poisoned {
            return Err(HandleError::Poisoned);
        }
        if key.runtime != self.id {
            return Err(HandleError::WrongRuntime);
        }
        Ok(())
    }

    fn checked_node(&self, key: Key, ty: TypeId) -> Result<&Node<P>, HandleError> {
        self.check_key(key)?;
        let node = self.store.nodes.get(key).ok_or(HandleError::Disposed)?;
        assert_eq!(node.ty, ty, "handle has a different type than its node");
        Ok(node)
    }

    fn pending(key: Key) -> ReadError {
        ReadError::Pending(PendingRead {
            attempt: key.generation,
            wait: u64::from(key.index),
        })
    }

    fn evaluate(&mut self, key: Key) -> Result<(), HandleError> {
        let node = self.store.node_mut(key);
        if node.queued.take().is_none() {
            return Ok(());
        }
        let Some(compute) = node.compute.clone() else {
            return Ok(());
        };
        node.running = true;
        let previous = node.current().map(|snapshot| snapshot.reads.clone());
        let manual = node.manual.take();
        let mut reads = Vec::new();
        if let Some(previous) = previous {
            let mut unchanged = true;
            for dep in previous {
                let mut version = 0;
                if let Some(source) = self.store.nodes.get(dep.source) {
                    if !source.running {
                        self.evaluate(dep.source)?;
                        version = self
                            .store
                            .node(dep.source)
                            .current()
                            .map_or(0, |s| s.version);
                    }
                }
                unchanged &= dep.version == version;
                if !unchanged && manual.is_none() {
                    break;
                }
                reads.push(Dependency {
                    source: dep.source,
                    version,
                });
            }
            if let Some(value) = manual {
                self.finish_evaluation(key, Answer::Value(value), reads);
                return Ok(());
            }
            if unchanged {
                self.store.node_mut(key).running = false;
                return Ok(());
            }
            reads.clear();
        }
        let result = compute(&mut Cx {
            graph: self,
            reads: &mut reads,
        });
        let answer = match (result, manual) {
            (Err(ReadError::Handle(error)), _) => {
                self.poisoned = true;
                return Err(error);
            }
            (_, Some(value)) | (Ok(value), _) => Answer::Value(value),
            (Err(ReadError::Failed(error)), _) => Answer::Failed(error),
            (Err(ReadError::Pending(_)), _) => {
                unreachable!("synchronous computation returned pending")
            }
        };
        if self.poisoned {
            return Err(HandleError::Poisoned);
        }
        self.finish_evaluation(key, answer, reads);
        Ok(())
    }

    fn finish_evaluation(&mut self, key: Key, answer: Answer, reads: Vec<Dependency>) {
        let height = reads
            .iter()
            .filter(|dep| dep.version != 0)
            .filter_map(|dep| self.store.nodes.get(dep.source))
            .map(|node| node.height.saturating_add(1))
            .max()
            .unwrap_or(0);
        let node = self.store.node_mut(key);
        node.height = node.height.max(height);
        node.running = false;
        self.store.stage(key, answer, reads);
    }

    fn advance(&mut self) -> Result<(), HandleError> {
        let writes: BTreeMap<_, _> = std::mem::take(&mut self.writes).into_iter().collect();
        for (key, value) in writes {
            if let Some(node) = self.store.nodes.get_mut(key) {
                if matches!(node.role, Role::Source) {
                    if self.store.stage(key, Answer::Value(value), Vec::new()) {
                        self.store.invalidate_subscribers(key);
                    }
                } else {
                    node.manual = Some(value);
                    self.store.schedule(key);
                    self.store.invalidate_subscribers(key);
                }
            }
        }
        while let Some(Reverse((_, revision, key))) = self.store.dirty.pop() {
            if self
                .store
                .nodes
                .get(key)
                .is_some_and(|node| node.queued == Some(revision))
            {
                self.evaluate(key)?;
            }
        }
        Ok(())
    }

    fn flush_tick(&mut self, sink: &mut impl FrameSink<P>) -> Result<(), FlushError> {
        self.wake_posted = false;
        self.advance().map_err(|_| FlushError::Poisoned)?;

        let mut ops = Vec::new();
        for &owner in &self.store.attach {
            if self.store.owners.get(owner.0).is_some() {
                ops.push(FrameOp::Attach {
                    owner,
                    parent: None,
                    before: None,
                });
            }
        }
        let mut staged = Vec::new();
        let mut effects = Vec::new();
        for key in self.store.nodes.keys() {
            let node = self.store.node(key);
            let Some(candidate) = &node.candidate else {
                continue;
            };
            staged.push(key);
            if node.changed() {
                match (&node.role, &candidate.answer) {
                    (Role::Render(apply), Answer::Value(value)) => {
                        let owner = node.owner;
                        let apply = Rc::clone(apply);
                        let value = Rc::clone(value);
                        ops.push(FrameOp::Patch(owner, apply(value)));
                    }
                    (Role::Render(_), Answer::Failed(_)) => return Err(FlushError::Poisoned),
                    (Role::Effect(_), Answer::Value(value)) => {
                        effects.push((key, Rc::clone(value)))
                    }
                    _ => {}
                }
            }
        }
        for &owner in &self.store.attach {
            if self.store.owners.get(owner.0).is_some() {
                ops.push(FrameOp::SetVisible(owner, true));
            }
        }

        let mut retired = Vec::with_capacity(staged.len());
        if !ops.is_empty() {
            let sequence = self
                .sequence
                .checked_add(1)
                .expect("frame sequence exhausted");
            sink.publish(Frame { sequence, ops })
                .map_err(FlushError::Publish)?;
            self.sequence = sequence;
        }

        self.store.attach.clear();
        for key in staged {
            retired.push(self.store.commit(key));
        }
        drop(retired);
        for (key, value) in effects {
            let Role::Effect(apply) = &mut self.store.node_mut(key).role else {
                unreachable!()
            };
            let mut callback = apply.take().expect("registered effect");
            let result = callback(&mut EffectCx { writes: self }, value);
            self.store.node_mut(key).role = Role::Effect(Some(callback));
            result.map_err(|_| FlushError::Poisoned)?;
        }
        Ok(())
    }
}

impl<P> ReadGraph for Runtime<P> {
    fn read(&mut self, key: Key, ty: TypeId, reads: &mut Vec<Dependency>) -> Read<Value> {
        let node = self.checked_node(key, ty)?;
        if node.running {
            reads.push(Dependency {
                source: key,
                version: 0,
            });
            return Err(ReadError::Failed(Rc::new(Cycle)));
        }
        self.evaluate(key)?;
        let snapshot = self.store.node(key).current();
        let version = snapshot.map_or(0, |s| s.version);
        if !reads.iter().any(|read| read.source == key) {
            reads.push(Dependency {
                source: key,
                version,
            });
        }
        snapshot.ok_or_else(|| Self::pending(key))?.answer.read()
    }
}

impl<P> WriteQueue for Runtime<P> {
    fn enqueue(&mut self, key: Key, ty: TypeId, value: Value) -> Result<(), HandleError> {
        self.checked_node(key, ty)?;
        self.writes.push((key, value));
        self.request_tick();
        Ok(())
    }
}

impl<P> Drive<P> for Runtime<P> {
    type Register = Registration<P>;

    fn install(&mut self, build: impl FnOnce(&mut Self::Register)) -> Result<Owner, HandleError> {
        if self.poisoned {
            return Err(HandleError::Poisoned);
        }
        self.guarded(|runtime| {
            let owner = runtime.store.create_root();
            let mut reg = Registration {
                store: std::mem::replace(&mut runtime.store, Store::new(runtime.id)),
                owner,
            };
            let result = panic::catch_unwind(AssertUnwindSafe(|| build(&mut reg)));
            runtime.store = reg.store;
            if let Err(payload) = result {
                let mut retired = Vec::new();
                runtime.store.remove_tree(owner, &mut retired);
                let _ = panic::catch_unwind(AssertUnwindSafe(|| clean(retired)));
                panic::resume_unwind(payload);
            }
            runtime.request_tick();
            Ok(owner)
        })
    }

    fn dispose(&mut self, owner: Owner) -> Result<(), HandleError> {
        self.check_key(owner.0)?;
        if self.store.owners.get(owner.0).is_none() {
            return Err(HandleError::Disposed);
        }
        self.guarded(|runtime| {
            let mut retired = Vec::new();
            runtime.store.remove_tree(owner, &mut retired);
            clean(retired);
            if !runtime.store.dirty.is_empty() {
                runtime.request_tick();
            }
        });
        Ok(())
    }

    fn set<T: 'static>(&mut self, target: WriteRef<T>, value: T) -> Result<(), HandleError> {
        self.enqueue(target.0 .0, TypeId::of::<T>(), Rc::new(value))
    }

    fn update<T: 'static>(
        &mut self,
        target: WriteRef<T>,
        update: impl FnOnce(&T) -> T,
    ) -> Read<()> {
        let key = target.0 .0;
        let node = self.checked_node(key, TypeId::of::<T>())?;
        let value = if let Some((_, queued)) =
            self.writes.iter().rev().find(|(queued, _)| *queued == key)
        {
            Rc::clone(queued)
        } else {
            node.current()
                .ok_or_else(|| Self::pending(key))?
                .answer
                .read()?
        };
        let value = downcast::<T>(value);
        let new = self.guarded(|_| update(&value));
        self.set(target, new)?;
        Ok(())
    }

    fn committed<T: 'static>(&self, source: ReadRef<T>) -> Read<Rc<T>> {
        let node = self.checked_node(source.0, TypeId::of::<T>())?;
        let snapshot = node
            .committed
            .as_ref()
            .ok_or_else(|| Self::pending(source.0))?;
        snapshot.answer.read().map(downcast)
    }

    fn flush(&mut self, sink: &mut impl FrameSink<P>) -> Result<(), FlushError> {
        if self.poisoned {
            return Err(FlushError::Poisoned);
        }
        if self.busy {
            return Err(FlushError::Busy);
        }
        self.busy = true;
        let result = self.guarded(|runtime| runtime.flush_tick(sink));
        self.busy = false;
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
}

impl<P> Drop for Runtime<P> {
    fn drop(&mut self) {
        let mut retired = Vec::new();
        while let Some(owner) = self.store.roots.pop() {
            self.store.remove_tree(owner, &mut retired);
        }
        if std::thread::panicking() {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| clean(retired)));
        } else {
            clean(retired);
        }
    }
}
