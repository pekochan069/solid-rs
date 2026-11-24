use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashSet},
    fmt,
    panic::{self, AssertUnwindSafe},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    arena::Arena,
    async_jobs::{Computed, EvaluationDomain, Flight, FlightId, Mailbox, Progress, Work},
    *,
};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

type Compute = Rc<dyn Fn(&mut Cx<'_>) -> Read<Computed>>;
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

enum Outcome {
    Answer(Answer),
    Pending {
        wait: PendingRead,
        retained: Option<Answer>,
    },
}

impl Outcome {
    fn answer(&self) -> Option<&Answer> {
        match self {
            Self::Answer(answer) => Some(answer),
            Self::Pending { retained, .. } => retained.as_ref(),
        }
    }

    fn wait(&self) -> Option<PendingRead> {
        match self {
            Self::Pending {
                wait,
                retained: None,
            } => Some(*wait),
            _ => None,
        }
    }

    fn read(&self) -> Read<Value> {
        match self {
            Self::Answer(answer)
            | Self::Pending {
                retained: Some(answer),
                ..
            } => answer.read(),
            Self::Pending {
                wait,
                retained: None,
            } => Err(ReadError::Pending(*wait)),
        }
    }

    fn same(&self, other: &Self, compare: Compare) -> bool {
        match (self.answer(), other.answer()) {
            (Some(left), Some(right)) => left.same(right, compare),
            (None, None) => self.wait() == other.wait(),
            _ => false,
        }
    }
}

struct Snapshot {
    outcome: Outcome,
    version: u64,
    question: u64,
    reads: Vec<Dependency>,
}

enum Role<P> {
    Source,
    Memo,
    Render(Render<P>),
    Effect(Option<ApplyEffect>),
}

enum Command {
    Set(Value),
    Refresh,
}

struct Node<P> {
    owner: Owner,
    ty: TypeId,
    compare: Compare,
    compute: Option<Compute>,
    role: Role<P>,
    initial: Option<Snapshot>,
    committed: Option<Snapshot>,
    candidate: Option<Snapshot>,
    subscribers: HashSet<Key>,
    command: Option<Command>,
    question: u64,
    flight: Option<Flight>,
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
    new_sources: Vec<Key>,
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
            new_sources: Vec::new(),
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

    fn stage(&mut self, key: Key, outcome: Outcome, reads: Vec<Dependency>) -> bool {
        let node = self.node(key);
        let current = node.current();
        let previous_version = current.map_or(0, |s| s.version);
        let question = node.question;
        let changed = current.is_none_or(|previous| !outcome.same(&previous.outcome, node.compare));
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
            outcome,
            version,
            question,
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
            initial: initial.map(|value| Snapshot {
                outcome: Outcome::Answer(Answer::Value(Rc::new(value))),
                version: 1,
                question: 0,
                reads: Vec::new(),
            }),
            committed: None,
            candidate: None,
            subscribers: HashSet::new(),
            command: None,
            question: 0,
            flight: None,
            height: 0,
            queued: None,
            running: false,
        });
        self.store
            .owner_mut(self.owner)
            .resources
            .push(Resource::Node(key));
        if self.store.node(key).initial.is_some() {
            self.store.new_sources.push(key);
        }
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
            Some(Rc::new(move |cx| compute(cx).map(Computed::erase))),
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
            Some(Rc::new(move |cx| {
                Ok(Computed::Value(Rc::new(compute(cx)?)))
            })),
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
            Some(Rc::new(move |cx| {
                Ok(Computed::Value(Rc::new(compute(cx)?)))
            })),
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
    inputs: Vec<(Key, Command)>,
    mailbox: Arc<Mailbox>,
    next_flight: u64,
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
            inputs: Vec::new(),
            mailbox: Mailbox::new(wake_host),
            next_flight: 0,
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
                self.mailbox.close();
                panic::resume_unwind(payload);
            }
        }
    }

    fn request_tick(&mut self) {
        self.guarded(|runtime| runtime.mailbox.request_tick(None));
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
            attempt: 0,
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
        let had_snapshot = previous.is_some();
        let command = node.command.take();
        let forced = command.is_some();
        let manual = match command {
            Some(Command::Set(value)) => Some(value),
            _ => None,
        };
        let mut reads = Vec::new();
        let mut new_question = previous.is_none();
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
            new_question = !unchanged;
            if unchanged && !forced {
                self.store.node_mut(key).running = false;
                return Ok(());
            }
            if manual.is_none() {
                reads.clear();
            }
        }
        let node = self.store.node_mut(key);
        if new_question {
            node.question = node
                .question
                .checked_add(1)
                .expect("question revision exhausted");
        }
        let retired = node.flight.take();
        drop(retired);
        if let Some(value) = manual.as_ref().filter(|_| had_snapshot) {
            self.finish_evaluation(key, Outcome::Answer(Answer::Value(Rc::clone(value))), reads);
            return Ok(());
        }
        let result = compute(&mut Cx {
            graph: self,
            reads: &mut reads,
        });
        let outcome = match (result, manual) {
            (Err(ReadError::Handle(error)), _) => {
                self.poisoned = true;
                return Err(error);
            }
            (_, Some(value)) | (Ok(Computed::Value(value)), _) => {
                Outcome::Answer(Answer::Value(value))
            }
            (Err(ReadError::Failed(error)), _) => Outcome::Answer(Answer::Failed(error)),
            (Ok(Computed::Job(work)), _) => self.start_flight(key, work),
            (Err(ReadError::Pending(wait)), _) => {
                if !reads.iter().any(|dep| {
                    self.store
                        .nodes
                        .get(dep.source)
                        .and_then(Node::current)
                        .is_some_and(|snapshot| {
                            snapshot.version == dep.version && snapshot.outcome.wait() == Some(wait)
                        })
                }) {
                    return Err(HandleError::Poisoned);
                }
                Outcome::Pending {
                    wait,
                    retained: None,
                }
            }
        };
        if self.poisoned {
            return Err(HandleError::Poisoned);
        }
        self.finish_evaluation(key, outcome, reads);
        Ok(())
    }

    fn finish_evaluation(&mut self, key: Key, outcome: Outcome, reads: Vec<Dependency>) -> bool {
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
        self.store.stage(key, outcome, reads)
    }

    fn start_flight(&mut self, key: Key, work: Work) -> Outcome {
        self.next_flight = self
            .next_flight
            .checked_add(1)
            .expect("flight generation exhausted");
        let node = self.store.node_mut(key);
        let id = FlightId {
            node: key,
            domain: EvaluationDomain::Main,
            question: node.question,
            generation: self.next_flight,
        };
        node.flight = Some(Flight::new(id, work));
        let progress = self.poll_flight(id).expect("new flight is ready to poll");
        if let Some(answer) = Self::poll_answer(progress) {
            return Outcome::Answer(answer);
        }
        let retained = self
            .store
            .node(key)
            .current()
            .filter(|snapshot| snapshot.question == id.question)
            .and_then(|snapshot| snapshot.outcome.answer())
            .cloned();
        Outcome::Pending {
            wait: PendingRead {
                attempt: self.id,
                wait: id.generation,
            },
            retained,
        }
    }

    fn poll_flight(&mut self, id: FlightId) -> Option<Progress> {
        let node = self.store.nodes.get_mut(id.node)?;
        let flight = node.flight.as_ref()?;
        if flight.id != id || id.question != node.question || !flight.can_poll() {
            return None;
        }
        let mut flight = node.flight.take().expect("validated flight");
        let waker = self.mailbox.waker(id);
        let progress = flight.poll(&mut Context::from_waker(&waker));
        if !matches!(progress, Progress::Done(_)) {
            self.store.node_mut(id.node).flight = Some(flight);
        }
        Some(progress)
    }

    fn poll_answer(progress: Progress) -> Option<Answer> {
        match progress {
            Progress::Item(value) | Progress::Done(Some(Ok(value))) => Some(Answer::Value(value)),
            Progress::Done(Some(Err(error))) => Some(Answer::Failed(error)),
            Progress::Pending | Progress::Done(None) => None,
        }
    }

    fn promote_inputs(&mut self) {
        for key in std::mem::take(&mut self.store.new_sources) {
            if let Some(node) = self.store.nodes.get_mut(key) {
                node.candidate = node.initial.take();
            }
        }
        let inputs: BTreeMap<_, _> = std::mem::take(&mut self.inputs).into_iter().collect();
        for (key, command) in inputs {
            if let Some(node) = self.store.nodes.get_mut(key) {
                if matches!(node.role, Role::Source) {
                    let Command::Set(value) = command else {
                        unreachable!("source refresh is a no-op")
                    };
                    if self
                        .store
                        .stage(key, Outcome::Answer(Answer::Value(value)), Vec::new())
                    {
                        self.store.invalidate_subscribers(key);
                    }
                } else {
                    node.command = Some(command);
                    self.store.schedule(key);
                    self.store.invalidate_subscribers(key);
                }
            }
        }
    }

    fn drain_dirty(&mut self) -> Result<(), HandleError> {
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
        let ready = self.mailbox.drain();
        self.promote_inputs();
        self.drain_dirty().map_err(|_| FlushError::Poisoned)?;
        for id in ready {
            let Some(answer) = self.poll_flight(id).and_then(Self::poll_answer) else {
                continue;
            };
            let reads = self
                .store
                .node(id.node)
                .current()
                .expect("live flight snapshot")
                .reads
                .clone();
            if self.store.stage(id.node, Outcome::Answer(answer), reads) {
                self.store.invalidate_subscribers(id.node);
                self.drain_dirty().map_err(|_| FlushError::Poisoned)?;
            }
        }

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
            let Outcome::Answer(answer) = &candidate.outcome else {
                continue;
            };
            staged.push(key);
            if node.changed() {
                match (&node.role, answer) {
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
        let mut resume = Vec::new();
        for key in staged {
            retired.push(self.store.commit(key));
            if let Some(id) = self
                .store
                .node_mut(key)
                .flight
                .as_mut()
                .and_then(Flight::resume_after_commit)
            {
                resume.push(id);
            }
        }
        drop(retired);
        for id in resume {
            self.mailbox.request_tick(Some(id));
        }
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
        snapshot.ok_or_else(|| Self::pending(key))?.outcome.read()
    }
}

impl<P> WriteQueue for Runtime<P> {
    fn enqueue(&mut self, key: Key, ty: TypeId, value: Value) -> Result<(), HandleError> {
        self.checked_node(key, ty)?;
        self.inputs.push((key, Command::Set(value)));
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
        let value = if let Some((_, Command::Set(value))) =
            self.inputs.iter().rev().find(|(queued, _)| *queued == key)
        {
            Rc::clone(value)
        } else {
            let snapshot = node
                .current()
                .or(node.initial.as_ref())
                .ok_or_else(|| Self::pending(key))?;
            match &snapshot.outcome {
                Outcome::Answer(answer) => answer.read()?,
                Outcome::Pending { wait, .. } => return Err(ReadError::Pending(*wait)),
            }
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
        snapshot.outcome.read().map(downcast)
    }

    fn latest<T: 'static>(&self, source: ReadRef<T>) -> Read<Rc<T>> {
        let node = self.checked_node(source.0, TypeId::of::<T>())?;
        if let Some(answer) = node
            .current()
            .and_then(|snapshot| snapshot.outcome.answer())
        {
            return answer.read().map(downcast);
        }
        if let Some(snapshot) = node.committed.as_ref() {
            return snapshot.outcome.read().map(downcast);
        }
        node.current()
            .ok_or_else(|| Self::pending(source.0))?
            .outcome
            .read()
            .map(downcast)
    }

    fn refresh<T: 'static>(&mut self, source: ReadRef<T>) -> Result<(), HandleError> {
        let node = self.checked_node(source.0, TypeId::of::<T>())?;
        if node.compute.is_some() {
            self.inputs.push((source.0, Command::Refresh));
            self.request_tick();
        }
        Ok(())
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
            self.mailbox.close();
        }
        result
    }
}

impl<P> Drop for Runtime<P> {
    fn drop(&mut self) {
        self.mailbox.close();
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
