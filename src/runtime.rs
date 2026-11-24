use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet},
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

mod ahead;
mod boundary;
mod frame_jobs;
use ahead::AheadState;
use boundary::{Boundary, BoundaryPlan, PublishGroup};
use frame_jobs::{FrameReads, FrameState};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

type Compute = Rc<dyn Fn(&mut Cx<'_>) -> Read<Computed>>;
type Compare = fn(&Value, &Value) -> bool;
type Render<P> = Rc<dyn Fn(Value) -> P>;
type ApplyEffect = Box<dyn FnMut(&mut EffectCx<'_>, Value) -> JobResult<()>>;
type BuildBranch<P> = Rc<dyn Fn(&mut Registration<P>, Value)>;

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

#[derive(Clone)]
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

#[derive(Clone)]
struct Snapshot {
    outcome: Outcome,
    version: u64,
    question: u64,
    reads: Vec<Dependency>,
    transactions: Vec<Transaction>,
    proposal: bool,
}

type Transaction = u64;

#[derive(Default)]
struct TransactionGroups {
    parents: BTreeMap<Transaction, Transaction>,
}

impl TransactionGroups {
    fn representative(&self, mut transaction: Transaction) -> Transaction {
        while let Some(&parent) = self.parents.get(&transaction) {
            transaction = parent;
        }
        transaction
    }

    fn join(&mut self, transactions: &[Transaction]) {
        let Some((&first, others)) = transactions.split_first() else {
            return;
        };
        for &other in others {
            let first = self.representative(first);
            let other = self.representative(other);
            if first != other {
                self.parents.insert(first.max(other), first.min(other));
            }
        }
    }

    fn accepts(&self, transaction: Transaction, transactions: &[Transaction]) -> bool {
        !transactions.is_empty()
            && transactions
                .iter()
                .all(|&id| self.representative(id) == transaction)
    }
}

enum Role<P> {
    Source,
    Memo,
    Render(Render<P>),
    Effect(Option<ApplyEffect>),
    Switch(Switch<P>),
    Loading,
}

impl<P> Role<P> {
    fn is_render_reader(&self) -> bool {
        matches!(self, Self::Render(_) | Self::Switch(_) | Self::Loading)
    }
}

struct Switch<P> {
    build: BuildBranch<P>,
    active: Option<Owner>,
    candidate: Option<(Owner, Value)>,
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
    candidate_changed: bool,
    subscribers: HashSet<Key>,
    command: Option<Command>,
    question: u64,
    next_version: u64,
    flight: Option<Flight>,
    height: u32,
    queued: Option<u64>,
    running: bool,
    applied: Option<Value>,
    effect_error: Option<EffectError>,
    affects: usize,
    ahead: Option<Box<AheadState>>,
}

struct EffectError {
    failure: Failure,
    question: u64,
    version: u64,
    transaction: Option<Transaction>,
}

impl EffectError {
    fn matches(&self, snapshot: &Snapshot) -> bool {
        (self.question, self.version) == (snapshot.question, snapshot.version)
    }
}

impl<P> Node<P> {
    fn current(&self) -> Option<&Snapshot> {
        self.candidate.as_ref().or(self.committed.as_ref())
    }

    fn pending_status(&self) -> bool {
        self.committed.as_ref().is_some_and(|committed| {
            self.affects > 0
                || self.candidate.as_ref().is_some_and(|candidate| {
                    (matches!(self.role, Role::Source) || candidate.question != committed.question)
                        && self.candidate_changed
                })
        })
    }
}

struct ReadView<'a> {
    current: Option<&'a Snapshot>,
    committed: Option<&'a Snapshot>,
    latest: Option<&'a Snapshot>,
    pending: Option<bool>,
}

impl<'a> ReadView<'a> {
    fn main<P>(node: &'a Node<P>, snapshot: Option<&'a Snapshot>) -> Self {
        let committed = node.committed.as_ref();
        let prospective_answer = snapshot.is_some_and(|s| matches!(s.outcome, Outcome::Answer(_)));
        Self {
            current: snapshot,
            committed,
            latest: snapshot,
            pending: if prospective_answer {
                Some(node.affects > 0)
            } else {
                committed.map(|s| {
                    node.affects > 0 || snapshot.is_some_and(|next| next.question != s.question)
                })
            },
        }
    }

    fn selected(&self, mode: ReadMode) -> Option<&Snapshot> {
        match mode {
            ReadMode::Get => self.current,
            ReadMode::Latest => self
                .latest
                .filter(|s| s.outcome.answer().is_some())
                .or(self.committed)
                .or(self.latest),
            ReadMode::Pending => None,
        }
    }

    fn observed(&self, mode: ReadMode) -> u64 {
        match mode {
            ReadMode::Pending => self
                .pending
                .map_or(0, |is_pending| if is_pending { 2 } else { 1 }),
            _ => self.selected(mode).map_or(0, |snapshot| snapshot.version),
        }
    }

    fn read<P>(&self, key: Key, mode: ReadMode) -> Read<Value> {
        if mode == ReadMode::Pending {
            return self
                .pending
                .map(|pending| Rc::new(pending) as Value)
                .ok_or_else(|| {
                    self.latest
                        .and_then(|snapshot| snapshot.outcome.wait())
                        .map_or_else(|| Runtime::<P>::pending(key), ReadError::Pending)
                });
        }
        self.selected(mode)
            .ok_or_else(|| Runtime::<P>::pending(key))?
            .outcome
            .read()
    }
}

fn record_read(
    reads: &mut Vec<Dependency>,
    source: Key,
    mode: ReadMode,
    version: u64,
    observed: u64,
) {
    if !reads
        .iter()
        .any(|dep| dep.source == source && dep.mode == mode)
    {
        reads.push(Dependency {
            source,
            version,
            mode,
            observed,
        });
    }
}

enum Resource {
    Node(Key),
    Affect(Key),
    Cleanup(Box<dyn FnOnce()>),
}

struct OwnerState {
    parent: Option<Owner>,
    children: Vec<Owner>,
    resources: Vec<Resource>,
    registration: Option<Transaction>,
    branch: Option<Key>,
    frame_only: bool,
    attached: bool,
    visible: bool,
    ordinal: u64,
}

enum Retired<P> {
    Node(Box<Node<P>>),
    Cleanup(Box<dyn FnOnce()>),
    Boundary(Boundary<P>),
}

struct Store<P> {
    owners: Arena<OwnerState>,
    nodes: Arena<Node<P>>,
    roots: Vec<Owner>,
    attach: Vec<Owner>,
    new_sources: Vec<Key>,
    dirty: BinaryHeap<Reverse<(u32, u64, Key)>>,
    next_enqueue: u64,
    next_transaction: Transaction,
    next_owner: u64,
    boundaries: BTreeMap<Key, Boundary<P>>,
    affects: Arena<(Owner, Key)>,
    affect_changes: Vec<(Key, bool)>,
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
            next_transaction: 0,
            next_owner: 0,
            boundaries: BTreeMap::new(),
            affects: Arena::new(id),
            affect_changes: Vec::new(),
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
        let transaction = self.new_transaction();
        self.create_owner(None, transaction, None)
    }

    fn create_owner(
        &mut self,
        parent: Option<Owner>,
        transaction: Transaction,
        branch: Option<Key>,
    ) -> Owner {
        self.next_owner = self
            .next_owner
            .checked_add(1)
            .expect("owner order exhausted");
        let frame_only = parent
            .and_then(|owner| self.owners.get(owner.0))
            .is_some_and(|state| state.frame_only);
        let key = self.owners.insert(OwnerState {
            parent,
            children: Vec::new(),
            resources: Vec::new(),
            registration: Some(transaction),
            branch,
            frame_only,
            attached: false,
            visible: true,
            ordinal: self.next_owner,
        });
        let owner = Owner(key, PhantomData);
        if let Some(parent) = parent {
            self.owner_mut(parent).children.push(owner);
        } else {
            self.roots.push(owner);
        }
        self.attach.push(owner);
        owner
    }

    fn replacing(&self, mut owner: Owner) -> Option<&Snapshot> {
        while let Some(state) = self.owners.get(owner.0) {
            if let Some(key) = state.branch {
                let node = self.node(key);
                let Role::Switch(switch) = &node.role else {
                    unreachable!("branch owner belongs to a switch")
                };
                if switch.active == Some(owner) && switch.candidate.is_some() {
                    return node.candidate.as_ref();
                }
            }
            owner = state.parent?;
        }
        None
    }

    fn owned_by(&self, mut owner: Owner, roots: &HashSet<Owner>) -> bool {
        loop {
            if roots.contains(&owner) {
                return true;
            }
            let Some(parent) = self.owners.get(owner.0).and_then(|state| state.parent) else {
                return false;
            };
            owner = parent;
        }
    }

    fn managed(&self, mut owner: Owner) -> bool {
        while let Some(state) = self.owners.get(owner.0) {
            if self.boundaries.contains_key(&owner.0) {
                return true;
            }
            let Some(parent) = state.parent else {
                break;
            };
            owner = parent;
        }
        false
    }

    fn visible(&self, mut owner: Owner) -> bool {
        while let Some(state) = self.owners.get(owner.0) {
            if !state.attached || !state.visible {
                return false;
            }
            let Some(parent) = state.parent else {
                return true;
            };
            owner = parent;
        }
        false
    }

    fn can_join_frame(&self, mut owner: Owner) -> bool {
        while let Some(state) = self.owners.get(owner.0) {
            if !state.visible && state.registration.is_none() {
                return false;
            }
            let Some(parent) = state.parent else {
                return true;
            };
            owner = parent;
        }
        false
    }

    fn attached_descendants(&self, owner: Owner, removal: &mut Vec<Owner>) {
        if let Some(state) = self.owners.get(owner.0) {
            if state.attached {
                for &child in &state.children {
                    self.attached_descendants(child, removal);
                }
                removal.push(owner);
            }
        }
    }

    fn new_transaction(&mut self) -> Transaction {
        self.next_transaction = self
            .next_transaction
            .checked_add(1)
            .expect("transaction ID exhausted");
        self.next_transaction
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

    fn stage(
        &mut self,
        key: Key,
        outcome: Outcome,
        reads: Vec<Dependency>,
        mut transactions: Vec<Transaction>,
        proposal: bool,
    ) -> bool {
        let node = self.node(key);
        let current = node.current();
        let question = node.question;
        let changed = current.is_none_or(|previous| {
            !outcome.same(&previous.outcome, node.compare)
                || (outcome.wait().is_some() && question != previous.question)
        });
        let version = if changed {
            node.next_version
                .checked_add(1)
                .expect("answer version exhausted")
        } else {
            current.expect("unchanged answer has a snapshot").version
        };
        for dep in &reads {
            if let Some(node) = self.nodes.get_mut(dep.source) {
                node.subscribers.insert(key);
            }
        }
        transactions.sort_unstable();
        transactions.dedup();
        let node = self.node_mut(key);
        node.next_version = node.next_version.max(version);
        node.candidate_changed = node
            .committed
            .as_ref()
            .is_none_or(|old| !outcome.same(&old.outcome, node.compare));
        let previous = node.candidate.replace(Snapshot {
            outcome,
            version,
            question,
            reads,
            transactions,
            proposal,
        });
        if let Some(previous) = &previous {
            self.prune_subscriptions(key, previous);
        }
        changed
    }

    fn commit(&mut self, key: Key) -> Option<Snapshot> {
        let candidate = self
            .node_mut(key)
            .candidate
            .take()
            .expect("prepared candidate");
        self.commit_frame(key, candidate, false)
    }

    fn discard_candidate(&mut self, key: Key) -> Option<(Snapshot, Option<Flight>)> {
        let node = self.node_mut(key);
        let candidate = node.candidate.take()?;
        let flight = (!candidate.transactions.is_empty())
            .then(|| node.flight.take())
            .flatten();
        self.prune_subscriptions(key, &candidate);
        Some((candidate, flight))
    }

    fn commit_frame(
        &mut self,
        key: Key,
        mut snapshot: Snapshot,
        candidate_changed: bool,
    ) -> Option<Snapshot> {
        snapshot.transactions.clear();
        for dep in &snapshot.reads {
            if let Some(source) = self.nodes.get_mut(dep.source) {
                source.subscribers.insert(key);
            }
        }
        let node = self.node_mut(key);
        node.next_version = node.next_version.max(snapshot.version);
        node.candidate_changed = candidate_changed;
        let previous = node.committed.replace(snapshot);
        if let Some(previous) = &previous {
            self.prune_subscriptions(key, previous);
        }
        previous
    }

    fn prune_subscriptions(&mut self, key: Key, previous: &Snapshot) {
        for dep in &previous.reads {
            let node = self.node(key);
            let still_read = node
                .committed
                .iter()
                .chain(&node.candidate)
                .any(|snapshot| {
                    snapshot
                        .reads
                        .iter()
                        .any(|current| current.source == dep.source)
                });
            if !still_read {
                if let Some(source) = self.nodes.get_mut(dep.source) {
                    source.subscribers.remove(&key);
                }
            }
        }
    }

    fn remove_tree(&mut self, owner: Owner, retired: &mut Vec<Retired<P>>) {
        if let Some(parent) = self.owners.get(owner.0).and_then(|owner| owner.parent) {
            self.owner_mut(parent)
                .children
                .retain(|child| *child != owner);
        }
        let mut owners = Vec::new();
        let mut pending = vec![owner];
        while let Some(owner) = pending.pop() {
            if let Some(state) = self.owners.remove(owner.0) {
                pending.extend(state.children.iter().rev().copied());
                owners.push((owner, state));
            }
        }
        for (owner, state) in owners.into_iter().rev() {
            if let Some(boundary) = self.boundaries.remove(&owner.0) {
                retired.push(Retired::Boundary(boundary));
            }
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
                    Resource::Affect(key) => {
                        if let Some((_, source)) = self.affects.remove(key) {
                            self.affect_changes.push((source, false));
                        }
                    }
                    Resource::Cleanup(cleanup) => retired.push(Retired::Cleanup(cleanup)),
                }
            }
        }
        self.roots.retain(|root| *root != owner);
        self.attach
            .retain(|owner| self.owners.get(owner.0).is_some());
    }
}

fn clean<P>(retired: Vec<Retired<P>>) {
    let mut failure = None;
    for resource in retired {
        let result = panic::catch_unwind(AssertUnwindSafe(move || match resource {
            Retired::Node(node) => drop(node),
            Retired::Cleanup(cleanup) => cleanup(),
            Retired::Boundary(boundary) => drop(boundary),
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
        self.register_with_compare(role, compute, initial, |left, right| {
            left.downcast_ref::<T>() == right.downcast_ref::<T>()
        })
    }

    fn register_with_compare<T: 'static>(
        &mut self,
        role: Role<P>,
        compute: Option<Compute>,
        initial: Option<T>,
        compare: Compare,
    ) -> ReadRef<T> {
        let key = self.store.nodes.insert(Node {
            owner: self.owner,
            ty: TypeId::of::<T>(),
            compare,
            compute,
            role,
            initial: initial.map(|value| Snapshot {
                outcome: Outcome::Answer(Answer::Value(Rc::new(value))),
                version: 1,
                question: 0,
                reads: Vec::new(),
                transactions: Vec::new(),
                proposal: false,
            }),
            committed: None,
            candidate: None,
            candidate_changed: false,
            subscribers: HashSet::new(),
            command: None,
            question: 0,
            next_version: 0,
            flight: None,
            height: 0,
            queued: None,
            running: false,
            applied: None,
            effect_error: None,
            affects: 0,
            ahead: None,
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

    fn ahead_memo<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut AheadCx<'_>) -> Read<Derive<T>> + 'static,
    ) -> ReadRef<T> {
        let read = self.register::<T>(
            Role::Memo,
            Some(Rc::new(move |cx| {
                compute(&mut AheadCx {
                    graph: cx.graph,
                    reads: cx.reads,
                })
                .map(Computed::erase)
            })),
            None,
        );
        self.store.node_mut(read.0).ahead = Some(Box::default());
        read
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

    fn render_ahead<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut AheadCx<'_>) -> Read<T> + 'static,
        apply: impl Fn(Rc<T>) -> P + 'static,
    ) {
        let read = self.register::<T>(
            Role::Render(Rc::new(move |value| apply(downcast(value)))),
            Some(Rc::new(move |cx| {
                compute(&mut AheadCx {
                    graph: cx.graph,
                    reads: cx.reads,
                })
                .map(|value| Computed::Value(Rc::new(value)))
            })),
            None,
        );
        self.store.node_mut(read.0).ahead = Some(Box::default());
    }

    fn on_cleanup(&mut self, cleanup: impl FnOnce() + 'static) {
        self.store
            .owner_mut(self.owner)
            .resources
            .push(Resource::Cleanup(Box::new(cleanup)));
    }

    fn switch<K: PartialEq + 'static>(
        &mut self,
        key: impl Fn(&mut Cx<'_>) -> Read<K> + 'static,
        build: impl Fn(&mut Self, Rc<K>) + 'static,
    ) {
        let parent = self.owner;
        let transaction = self
            .store
            .owner_mut(parent)
            .registration
            .expect("registering into an unpublished owner");
        self.owner = self.store.create_owner(Some(parent), transaction, None);
        self.register::<K>(
            Role::Switch(Switch {
                build: Rc::new(move |reg, value| build(reg, downcast(value))),
                active: None,
                candidate: None,
            }),
            Some(Rc::new(move |cx| Ok(Computed::Value(Rc::new(key(cx)?))))),
            None,
        );
        self.owner = parent;
    }

    fn loading(
        &mut self,
        on: impl Fn(&mut Cx<'_>) -> Read<()> + 'static,
        content: impl Fn(&mut Self) + 'static,
        fallback: impl Fn(&mut Self) + 'static,
    ) {
        self.register_loading(on, content, fallback);
    }

    fn errored(
        &mut self,
        content: impl Fn(&mut Self) + 'static,
        fallback: impl Fn(&mut Self, ReadRef<Failure>) + 'static,
    ) {
        self.register_errored(content, fallback);
    }

    fn reveal(&mut self, order: RevealOrder, collapse_tail: bool, build: impl FnOnce(&mut Self)) {
        self.register_reveal(order, collapse_tail, build);
    }
}

pub struct Runtime<P> {
    id: u64,
    store: Store<P>,
    inputs: Vec<(Key, Command)>,
    effect_errors: Vec<(Key, EffectError)>,
    layout: Option<Transaction>,
    mailbox: Arc<Mailbox>,
    next_flight: u64,
    frame_cache: BTreeMap<Transaction, FrameState>,
    frame_ready: HashSet<FlightId>,
    transaction: Transaction,
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
            effect_errors: Vec::new(),
            layout: None,
            mailbox: Mailbox::new(wake_host),
            next_flight: 0,
            frame_cache: BTreeMap::new(),
            frame_ready: HashSet::new(),
            transaction: 0,
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

    fn build_owner(&mut self, owner: Owner, build: impl FnOnce(&mut Registration<P>)) {
        let mut reg = Registration {
            store: std::mem::replace(&mut self.store, Store::new(self.id)),
            owner,
        };
        let result = panic::catch_unwind(AssertUnwindSafe(|| build(&mut reg)));
        self.store = reg.store;
        if let Err(payload) = result {
            let mut retired = Vec::new();
            self.store.remove_tree(owner, &mut retired);
            let _ = panic::catch_unwind(AssertUnwindSafe(|| clean(retired)));
            panic::resume_unwind(payload);
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
            attempt: 0,
            wait: u64::from(key.index),
        })
    }

    fn evaluate(&mut self, key: Key) -> Result<(), HandleError> {
        let owner = self.store.node(key).owner;
        if self
            .store
            .owners
            .get(owner.0)
            .is_some_and(|state| state.frame_only)
        {
            self.store.node_mut(key).queued = None;
            return Ok(());
        }
        let node = self.store.node_mut(key);
        if node.queued.take().is_none() {
            return Ok(());
        }
        let Some(compute) = node.compute.clone() else {
            return Ok(());
        };
        node.running = true;
        let previous = node.current().map(|snapshot| snapshot.reads.clone());
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
                if !unchanged {
                    break;
                }
                reads.push(Dependency {
                    version,
                    observed: version,
                    ..dep
                });
            }
            new_question = !unchanged;
            if unchanged && !forced {
                self.store.node_mut(key).running = false;
                return Ok(());
            }
            if new_question || manual.is_none() {
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
        if let Some(value) = manual.as_ref().filter(|_| !new_question) {
            self.finish_evaluation(
                key,
                Outcome::Answer(Answer::Value(Rc::clone(value))),
                reads,
                forced,
            );
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
        self.finish_evaluation(key, outcome, reads, forced);
        Ok(())
    }

    fn finish_evaluation(
        &mut self,
        key: Key,
        outcome: Outcome,
        reads: Vec<Dependency>,
        forced: bool,
    ) {
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
        let mut transactions: Vec<_> = reads
            .iter()
            .filter_map(|dep| {
                self.store
                    .nodes
                    .get(dep.source)?
                    .candidate
                    .as_ref()
                    .filter(|candidate| candidate.version == dep.version)
            })
            .flat_map(|candidate| candidate.transactions.iter().copied())
            .collect();
        let node = self.store.node(key);
        if let Some(transaction) = (!matches!(node.role, Role::Memo))
            .then(|| {
                self.store
                    .owners
                    .get(node.owner.0)
                    .and_then(|owner| owner.registration)
            })
            .flatten()
        {
            transactions.push(transaction);
        }
        if forced {
            if let Some(candidate) = &node.candidate {
                transactions.extend(&candidate.transactions);
            }
        }
        if forced {
            transactions.push(self.transaction);
        } else if transactions.is_empty() {
            transactions.push(self.store.new_transaction());
        }
        self.store.stage(key, outcome, reads, transactions, forced);
        self.refresh_switch(key);
    }

    fn refresh_switch(&mut self, key: Key) {
        let node = self.store.node(key);
        let Role::Switch(switch) = &node.role else {
            return;
        };
        let snapshot = node.candidate.as_ref().expect("staged switch key");
        let value = match &snapshot.outcome {
            Outcome::Answer(Answer::Value(value))
                if node.candidate_changed || switch.active.is_none() =>
            {
                Some(Rc::clone(value))
            }
            _ => None,
        };
        if let (Some((owner, previous)), Some(value)) = (&switch.candidate, &value) {
            if (node.compare)(previous, value) && self.store.owners.get(owner.0).is_some() {
                let transaction = self.store.owners.get(owner.0).unwrap().registration;
                self.store
                    .node_mut(key)
                    .candidate
                    .as_mut()
                    .unwrap()
                    .transactions
                    .extend(transaction);
                return;
            }
        }
        let parent = node.owner;
        let transaction = snapshot.transactions[0];
        let build = Rc::clone(&switch.build);
        let Role::Switch(switch) = &mut self.store.node_mut(key).role else {
            unreachable!()
        };
        if let Some((obsolete, _)) = switch.candidate.take() {
            let mut retired = Vec::new();
            self.store.remove_tree(obsolete, &mut retired);
            clean(retired);
        }
        if let Some(value) = value {
            let owner = self
                .store
                .create_owner(Some(parent), transaction, Some(key));
            self.build_owner(owner, |reg| build(reg, Rc::clone(&value)));
            let Role::Switch(switch) = &mut self.store.node_mut(key).role else {
                unreachable!()
            };
            switch.candidate = Some((owner, value));
            self.seed_sources();
        }
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
        if id.question != node.question {
            return None;
        }
        Flight::poll_slot(&mut node.flight, id, &self.mailbox)
    }

    fn poll_answer(progress: Progress) -> Option<Answer> {
        match progress {
            Progress::Item(value) | Progress::Done(Some(Ok(value))) => Some(Answer::Value(value)),
            Progress::Done(Some(Err(error))) => Some(Answer::Failed(error)),
            Progress::Pending | Progress::Done(None) => None,
        }
    }

    fn seed_sources(&mut self) {
        for key in std::mem::take(&mut self.store.new_sources) {
            if let Some(node) = self.store.nodes.get(key) {
                let transaction = self.store.owners.get(node.owner.0).unwrap().registration;
                let node = self.store.node_mut(key);
                let mut source = node.initial.take().expect("initial source");
                source.transactions.extend(transaction);
                node.next_version = source.version;
                node.candidate = Some(source);
            }
        }
    }

    fn promote_inputs(&mut self) {
        for (key, add) in std::mem::take(&mut self.store.affect_changes) {
            if let Some(node) = self.store.nodes.get_mut(key) {
                if add {
                    node.affects += 1;
                } else {
                    node.affects -= 1;
                }
            }
        }
        self.seed_sources();
        let inputs: BTreeMap<_, _> = std::mem::take(&mut self.inputs).into_iter().collect();
        for (key, command) in inputs {
            if let Some(node) = self.store.nodes.get_mut(key) {
                if matches!(node.role, Role::Source) {
                    let Command::Set(value) = command else {
                        unreachable!("source refresh is a no-op")
                    };
                    let outcome = Outcome::Answer(Answer::Value(value));
                    if node.candidate.is_none()
                        && node
                            .committed
                            .as_ref()
                            .is_some_and(|snapshot| outcome.same(&snapshot.outcome, node.compare))
                    {
                        continue;
                    }
                    let mut transactions = vec![self.transaction];
                    if let Some(candidate) = &node.candidate {
                        transactions.extend(&candidate.transactions);
                    }
                    if self
                        .store
                        .stage(key, outcome, Vec::new(), transactions, true)
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
        self.frame_ready.extend(
            ready
                .iter()
                .copied()
                .filter(|id| id.domain == EvaluationDomain::Frame),
        );
        for (key, mut error) in std::mem::take(&mut self.effect_errors) {
            if self.store.nodes.get(key).is_some() {
                error.transaction = Some(self.store.new_transaction());
                self.store.node_mut(key).effect_error = Some(error);
            }
        }
        self.transaction = self.store.new_transaction();
        self.promote_inputs();
        self.drain_dirty().map_err(|_| FlushError::Poisoned)?;
        for &id in ready
            .iter()
            .filter(|id| id.domain == EvaluationDomain::Main)
        {
            let Some(answer) = self.poll_flight(id).and_then(Self::poll_answer) else {
                continue;
            };
            let snapshot = self
                .store
                .node(id.node)
                .current()
                .expect("live flight snapshot");
            let reads = snapshot.reads.clone();
            let mut transactions = snapshot.transactions.clone();
            let proposal = snapshot.proposal;
            if transactions.is_empty() {
                transactions.push(self.store.new_transaction());
            }
            if self.store.stage(
                id.node,
                Outcome::Answer(answer),
                reads,
                transactions,
                proposal,
            ) {
                self.store.invalidate_subscribers(id.node);
                self.drain_dirty().map_err(|_| FlushError::Poisoned)?;
            }
        }

        self.publish_ahead(
            sink,
            ready
                .into_iter()
                .filter(|id| id.domain == EvaluationDomain::Ahead),
        )?;

        let mut projection_holds = HashSet::new();
        loop {
            let (groups, ready) = self.transaction_groups();
            self.reconcile_frames(&groups, &ready);
            let Some(transaction) = ready.into_iter().find(|id| !projection_holds.contains(id))
            else {
                if !self.store.affect_changes.is_empty() {
                    self.request_tick();
                }
                return Ok(());
            };
            match self.publish_group(sink, &groups, transaction)? {
                PublishGroup::Committed | PublishGroup::Staged => {
                    projection_holds.clear();
                    self.drain_dirty().map_err(|_| FlushError::Poisoned)?;
                }
                PublishGroup::Held => {
                    projection_holds.insert(transaction);
                }
            }
        }
    }

    fn transaction_groups(&self) -> (TransactionGroups, Vec<Transaction>) {
        let mut groups = TransactionGroups::default();
        let mut candidates = Vec::new();
        let mut ids = BTreeSet::new();
        for key in self.store.nodes.keys() {
            let node = self.store.node(key);
            if let Some(lane) = node.lane_candidate() {
                ids.extend(lane.transactions.iter().copied());
                groups.join(&lane.transactions);
            }
            ids.extend(
                node.effect_error
                    .as_ref()
                    .and_then(|error| error.transaction),
            );
            if let Some(candidate) = &node.candidate {
                ids.extend(candidate.transactions.iter().copied());
                let reader = node.role.is_render_reader();
                if reader || candidate.proposal {
                    let replacement = self.store.replacing(node.owner);
                    let joins = reader && self.store.can_join_frame(node.owner);
                    if candidate.proposal || (joins && replacement.is_none()) {
                        groups.join(&candidate.transactions);
                    }
                    if let Some(replacement) = replacement.filter(|_| joins) {
                        candidates.push((candidate, replacement));
                    }
                }
            }
        }
        ids.extend(
            self.store
                .attach
                .iter()
                .filter_map(|owner| self.store.owners.get(owner.0)?.registration),
        );
        ids.extend(self.layout);
        ids.extend(self.frame_cache.iter().filter_map(|(&id, state)| {
            (state.published
                && state
                    .flights
                    .values()
                    .any(|flight| self.frame_ready.contains(&flight.id)))
            .then_some(id)
        }));

        for (candidate, replacement) in candidates {
            groups.join(&Self::reader_transactions(&groups, candidate, replacement));
        }
        let mut ready: Vec<_> = ids
            .into_iter()
            .filter(|&id| groups.representative(id) == id)
            .collect();
        // Promote writes before validating a resumed stream against the committed frame.
        ready.sort_by_key(|id| {
            self.frame_cache
                .get(id)
                .is_some_and(|state| state.published)
        });
        (groups, ready)
    }

    fn reader_transactions(
        groups: &TransactionGroups,
        candidate: &Snapshot,
        replacement: &Snapshot,
    ) -> Vec<Transaction> {
        candidate
            .transactions
            .iter()
            .copied()
            .filter(|&id| {
                !replacement
                    .transactions
                    .iter()
                    .any(|&retiring| groups.representative(id) == groups.representative(retiring))
            })
            .collect()
    }

    fn publish_group(
        &mut self,
        sink: &mut impl FrameSink<P>,
        groups: &TransactionGroups,
        transaction: Transaction,
    ) -> Result<PublishGroup, FlushError> {
        let keys: Vec<_> = self.store.nodes.keys().collect();
        if keys.iter().any(|&key| {
            let node = self.store.node(key);
            node.role.is_render_reader()
                && self.store.visible(node.owner)
                && node.lane_candidate().is_some_and(|s| {
                    groups.accepts(transaction, &s.transactions) && s.outcome.wait().is_some()
                })
        }) {
            return Ok(PublishGroup::Held);
        }
        let mut switches = Vec::new();
        let mut retiring = HashSet::new();
        for &key in &keys {
            let node = self.store.node(key);
            if let (Role::Switch(switch), Some(snapshot)) = (&node.role, &node.candidate) {
                if switch.candidate.is_some() && groups.accepts(transaction, &snapshot.transactions)
                {
                    retiring.extend(switch.active);
                    switches.push((key, switch.active, None));
                }
            }
        }
        switches.retain(|(key, _, _)| !self.store.owned_by(self.store.node(*key).owner, &retiring));
        let mut projection = FrameReads {
            id: self.id,
            store: &self.store,
            groups,
            transaction,
            retiring,
            checked: HashSet::new(),
            running: HashSet::new(),
            cache: self.frame_cache.remove(&transaction).unwrap_or_default(),
            ready: &mut self.frame_ready,
            next_flight: &mut self.next_flight,
            mailbox: &self.mailbox,
            values: BTreeMap::new(),
        };
        for &key in &keys {
            let node = projection.store.node(key);
            let Role::Switch(switch) = &node.role else {
                continue;
            };
            if !projection.includes(node.owner) {
                continue;
            }
            projection.resolve(key).map_err(|_| FlushError::Poisoned)?;
            let desired =
                projection
                    .values
                    .get(&key)
                    .and_then(|snapshot| match &snapshot.outcome {
                        Outcome::Answer(Answer::Value(value))
                            if switch.active.is_none()
                                || node.committed.as_ref().is_none_or(|s| {
                                    !snapshot.outcome.same(&s.outcome, node.compare)
                                }) =>
                        {
                            Some(Rc::clone(value))
                        }
                        _ => None,
                    });
            match (projection.cache.branches.get(&key), desired.as_ref()) {
                (None, None) => {}
                (Some((owner, value)), Some(next)) if (node.compare)(value, next) => {
                    projection.retiring.extend(switch.active);
                    switches.push((key, switch.active, Some(*owner)));
                }
                _ => {
                    let parent = node.owner;
                    let build = Rc::clone(&switch.build);
                    let mut state = projection.suspend();
                    if let Some((obsolete, _)) = state.branches.remove(&key) {
                        let mut retired = Vec::new();
                        self.store.remove_tree(obsolete, &mut retired);
                        clean(retired);
                    }
                    if let Some(value) = desired {
                        let owner = self
                            .store
                            .create_owner(Some(parent), transaction, Some(key));
                        self.store.owner_mut(owner).frame_only = true;
                        self.build_owner(owner, |reg| build(reg, Rc::clone(&value)));
                        state.branches.insert(key, (owner, value));
                        self.seed_sources();
                    }
                    self.save_frame(transaction, state);
                    return Ok(PublishGroup::Staged);
                }
            }
        }
        for &key in &keys {
            let node = self.store.node(key);
            let new_branch = self
                .store
                .owners
                .get(node.owner.0)
                .is_some_and(|state| state.frame_only);
            if (node.role.is_render_reader() || matches!(node.role, Role::Effect(_)) || new_branch)
                && projection.includes(node.owner)
            {
                projection.resolve(key).map_err(|_| FlushError::Poisoned)?;
            }
        }
        let boundaries = BoundaryPlan::prepare(&projection);
        if boundaries.needs_staging() || boundaries.held() {
            let state = projection.finish();
            self.save_frame(transaction, state);
            if !boundaries.needs_staging() {
                return Ok(PublishGroup::Held);
            }
            boundaries.stage(self, transaction);
            return Ok(PublishGroup::Staged);
        }
        if boundaries.failed() {
            return Err(FlushError::Poisoned);
        }
        let boundary_removals = boundaries.removals();
        let mut visible = HashSet::new();
        let mut included = Vec::new();
        let mut traversal: Vec<_> = self
            .store
            .roots
            .iter()
            .rev()
            .copied()
            .map(|owner| (owner, true))
            .collect();
        while let Some((owner, parent_visible)) = traversal.pop() {
            if !projection.includes(owner) || boundary_removals.contains(&owner) {
                continue;
            }
            let state = self.store.owners.get(owner.0).expect("included owner");
            let showing = parent_visible && boundaries.local_visibility(owner, state);
            if showing {
                visible.insert(owner);
            }
            included.push(owner);
            traversal.extend(state.children.iter().rev().map(|&child| (child, showing)));
        }
        let mut retiring = projection.retiring;
        let mut projected = projection.values;
        retiring.extend(&boundary_removals);
        let mut continuing = FrameState {
            published: true,
            ..FrameState::default()
        };
        for (key, flight) in projection.cache.flights {
            if let Some(snapshot) = projected
                .get(&key)
                .filter(|_| !self.store.owned_by(self.store.node(key).owner, &retiring))
            {
                continuing.values.insert(key, snapshot.clone());
                continuing.flights.insert(key, flight);
            }
        }
        switches.retain(|(key, _, _)| !self.store.owned_by(self.store.node(*key).owner, &retiring));
        projected.retain(|key, snapshot| {
            let node = self.store.node(*key);
            !self.store.owned_by(node.owner, &retiring)
                && (matches!(snapshot.outcome, Outcome::Answer(_)) || node.committed.is_none())
        });
        for snapshot in projected.values() {
            for dep in &snapshot.reads {
                if let Some(source) = self.store.nodes.get_mut(dep.source) {
                    source.subscribers.reserve(1);
                }
            }
        }

        let attached: Vec<_> = included
            .iter()
            .copied()
            .filter(|&owner| {
                visible.contains(&owner)
                    && !self
                        .store
                        .owners
                        .get(owner.0)
                        .expect("included owner")
                        .attached
            })
            .collect();
        let mut ops = Vec::new();
        for &owner in &attached {
            let parent = self
                .store
                .owners
                .get(owner.0)
                .expect("registered owner")
                .parent;
            let siblings = parent.map_or(self.store.roots.as_slice(), |parent| {
                self.store
                    .owners
                    .get(parent.0)
                    .expect("live parent")
                    .children
                    .as_slice()
            });
            let before = siblings
                .iter()
                .copied()
                .skip_while(|&sibling| sibling != owner)
                .skip(1)
                .find(|&sibling| {
                    self.store
                        .owners
                        .get(sibling.0)
                        .is_some_and(|state| state.attached)
                        && !self.store.owned_by(sibling, &retiring)
                });
            ops.push(FrameOp::Attach {
                owner,
                parent,
                before,
            });
        }

        let mut staged = Vec::new();
        let mut staged_lane = Vec::new();
        let mut effects = Vec::new();
        for &key in &keys {
            let node = self.store.node(key);
            if self.store.owned_by(node.owner, &retiring) {
                continue;
            }
            let candidate = node
                .candidate
                .as_ref()
                .filter(|candidate| groups.accepts(transaction, &candidate.transactions));
            let lane = node
                .lane_candidate()
                .filter(|s| groups.accepts(transaction, &s.transactions));
            let main = projected.get(&key).or(candidate);
            let next = main.or(lane);
            let showing = visible.contains(&node.owner);
            let exposed = showing && !self.store.visible(node.owner);
            let Some(snapshot) = next.or_else(|| exposed.then_some(node.displayed()).flatten())
            else {
                continue;
            };
            let Outcome::Answer(answer) = &snapshot.outcome else {
                continue;
            };
            if main.is_none() && lane.is_some() {
                staged_lane.push(key);
            } else if candidate.is_some() && !projected.contains_key(&key) {
                staged.push(key);
            }
            if !showing {
                continue;
            }
            match (&node.role, answer) {
                (Role::Render(apply), Answer::Value(value))
                    if exposed
                        || node.displayed().is_none_or(|committed| {
                            !committed.outcome.same(&snapshot.outcome, node.compare)
                        }) =>
                {
                    ops.push(FrameOp::Patch(node.owner, apply(Rc::clone(value))));
                }
                (Role::Effect(_), Answer::Value(value))
                    if node
                        .applied
                        .as_ref()
                        .is_none_or(|last| !(node.compare)(last, value)) =>
                {
                    effects.push((key, Rc::clone(value)));
                }
                _ => {}
            }
        }

        for &owner in &attached {
            ops.push(FrameOp::SetVisible(owner, true));
        }
        for &owner in &included {
            let state = self.store.owners.get(owner.0).expect("included owner");
            let showing = boundaries.local_visibility(owner, state);
            if state.attached && state.visible != showing {
                ops.push(FrameOp::SetVisible(owner, showing));
            }
        }
        let mut removed = Vec::new();
        for (_, active, _) in &switches {
            if let Some(owner) = active {
                self.store.attached_descendants(*owner, &mut removed);
            }
        }
        for &owner in &boundary_removals {
            self.store.attached_descendants(owner, &mut removed);
        }
        ops.extend(removed.into_iter().map(FrameOp::Remove));
        let projected: Vec<_> = projected
            .into_iter()
            .map(|(key, snapshot)| {
                let node = self.store.node(key);
                let candidate_changed = node.candidate.as_ref().is_some_and(|candidate| {
                    !candidate.outcome.same(&snapshot.outcome, node.compare)
                });
                (key, snapshot, candidate_changed)
            })
            .collect();
        let mut replaced = Vec::with_capacity(staged.len() + projected.len() + staged_lane.len());
        let mut replaced_ahead = Vec::with_capacity(staged.len() + projected.len());
        let mut resume = Vec::with_capacity(staged.len());
        if !ops.is_empty() {
            let sequence = self
                .sequence
                .checked_add(1)
                .expect("frame sequence exhausted");
            if let Err(error) = sink.publish(Frame { sequence, ops }) {
                self.discard_unpublished();
                return Err(FlushError::Publish(error));
            }
            self.sequence = sequence;
        }

        for owner in attached {
            self.store.owner_mut(owner).attached = true;
        }
        for owner in included {
            let state = self.store.owner_mut(owner);
            state.registration = None;
            state.frame_only = false;
        }
        if self
            .layout
            .is_some_and(|id| groups.representative(id) == transaction)
        {
            self.layout = None;
        }
        boundaries.commit(&mut self.store);
        self.store.attach.retain(|owner| {
            self.store
                .owners
                .get(owner.0)
                .is_some_and(|owner| owner.registration.is_some())
        });
        for key in staged {
            replaced.push(self.store.commit(key));
            replaced_ahead.extend(self.adopt_main_ahead(key));
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
        for (key, snapshot, candidate_changed) in projected {
            replaced.push(self.store.commit_frame(key, snapshot, candidate_changed));
            replaced_ahead.extend(self.adopt_main_ahead(key));
        }
        for key in staged_lane {
            let (previous, flight) = self.commit_lane(key);
            replaced.push(previous);
            resume.extend(flight);
        }
        for &key in &keys {
            let node = self.store.node_mut(key);
            if let Some(candidate) = node.lane_candidate_mut() {
                candidate
                    .transactions
                    .retain(|&id| groups.representative(id) != transaction);
            }
            if let Some(candidate) = &mut node.candidate {
                candidate
                    .transactions
                    .retain(|&id| groups.representative(id) != transaction);
            }
            if let Some(error) = &mut node.effect_error {
                if error
                    .transaction
                    .is_some_and(|id| groups.representative(id) == transaction)
                {
                    error.transaction = None;
                }
                if error.transaction.is_none()
                    && node
                        .committed
                        .as_ref()
                        .is_some_and(|snapshot| !error.matches(snapshot))
                {
                    node.effect_error = None;
                }
            }
        }
        let mut retired = Vec::new();
        for (key, previous, projected) in switches {
            let Role::Switch(switch) = &mut self.store.node_mut(key).role else {
                unreachable!()
            };
            switch.active = projected.or_else(|| switch.candidate.take().map(|(owner, _)| owner));
            if let Some(owner) = previous {
                self.store.remove_tree(owner, &mut retired);
            }
        }
        for owner in boundary_removals {
            self.store.remove_tree(owner, &mut retired);
        }
        if !continuing.flights.is_empty() {
            resume.extend(
                continuing
                    .flights
                    .values_mut()
                    .filter_map(Flight::resume_after_commit),
            );
            let transaction = self.store.new_transaction();
            self.save_frame(transaction, continuing);
        }
        drop((replaced, replaced_ahead));
        clean(retired);
        for id in resume {
            self.mailbox.request_tick(Some(id));
        }
        for (key, value) in effects {
            let Role::Effect(apply) = &mut self.store.node_mut(key).role else {
                unreachable!()
            };
            let mut callback = apply.take().expect("registered effect");
            let result = callback(&mut EffectCx { writes: self }, Rc::clone(&value));
            let node = self.store.node_mut(key);
            node.role = Role::Effect(Some(callback));
            match result {
                Ok(()) => node.applied = Some(value),
                Err(failure) => {
                    let snapshot = node.committed.as_ref().expect("applied effect snapshot");
                    self.effect_errors.push((
                        key,
                        EffectError {
                            failure,
                            question: snapshot.question,
                            version: snapshot.version,
                            transaction: None,
                        },
                    ));
                    self.request_tick();
                }
            }
        }
        Ok(PublishGroup::Committed)
    }

    fn discard_unpublished(&mut self) {
        self.frame_cache.clear();
        self.frame_ready.clear();
        let mut retired = Vec::new();
        for owner in std::mem::take(&mut self.store.attach) {
            self.store.remove_tree(owner, &mut retired);
        }
        let keys: Vec<_> = self.store.nodes.keys().collect();
        for key in keys {
            if let Role::Switch(switch) = &mut self.store.node_mut(key).role {
                switch.candidate = None;
            }
            self.store.discard_candidate(key);
            if let Some(ahead) = &mut self.store.node_mut(key).ahead {
                ahead.discard();
            }
        }
        clean(retired);
    }
}

impl<P> ReadGraph for Runtime<P> {
    fn read(
        &mut self,
        key: Key,
        ty: TypeId,
        mode: ReadMode,
        reads: &mut Vec<Dependency>,
    ) -> Read<Value> {
        let node = self.checked_node(key, ty)?;
        if node.running {
            record_read(reads, key, mode, 0, 0);
            return Err(ReadError::Failed(Rc::new(Cycle)));
        }
        self.evaluate(key)?;
        let node = self.store.node(key);
        let snapshot = node.current();
        let view = ReadView::main(node, snapshot);
        record_read(
            reads,
            key,
            mode,
            snapshot.map_or(0, |s| s.version),
            view.observed(mode),
        );
        view.read::<P>(key, mode)
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
            runtime.build_owner(owner, build);
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
            let layout_changed = runtime.store.managed(owner);
            let mut retired = Vec::new();
            runtime.store.remove_tree(owner, &mut retired);
            runtime.prune_disposed_frames();
            clean(retired);
            if layout_changed {
                runtime.layout = Some(runtime.store.new_transaction());
            }
            // Displayed lanes may have read a removed node independently of the main graph.
            runtime.request_tick();
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
        let snapshot = node.displayed().ok_or_else(|| Self::pending(source.0))?;
        snapshot.outcome.read().map(downcast)
    }

    fn latest<T: 'static>(&self, source: ReadRef<T>) -> Read<Rc<T>> {
        let node = self.checked_node(source.0, TypeId::of::<T>())?;
        if let Some(ahead) = &node.ahead {
            if ahead.has_candidate() {
                return ahead
                    .view(node.affects)
                    .read::<P>(source.0, ReadMode::Latest)
                    .map(downcast);
            }
        }
        if let Some(answer) = node
            .candidate
            .as_ref()
            .and_then(|snapshot| snapshot.outcome.answer())
        {
            return answer.read().map(downcast);
        }
        if let Some(snapshot) = node.displayed() {
            return snapshot.outcome.read().map(downcast);
        }
        node.current()
            .ok_or_else(|| Self::pending(source.0))?
            .outcome
            .read()
            .map(downcast)
    }

    fn is_pending<T: 'static>(&self, source: ReadRef<T>) -> Result<bool, HandleError> {
        let node = self.checked_node(source.0, TypeId::of::<T>())?;
        Ok(node
            .ahead
            .as_ref()
            .filter(|ahead| ahead.has_candidate())
            .map_or_else(
                || node.pending_status(),
                |ahead| ahead.view(node.affects).pending.unwrap_or(false),
            ))
    }

    fn refresh<T: 'static>(&mut self, source: ReadRef<T>) -> Result<(), HandleError> {
        let node = self.checked_node(source.0, TypeId::of::<T>())?;
        if node.compute.is_some() {
            self.inputs.push((source.0, Command::Refresh));
            self.request_tick();
        }
        Ok(())
    }

    fn affects<T: 'static>(
        &mut self,
        owner: Owner,
        source: ReadRef<T>,
    ) -> Result<Affect, HandleError> {
        self.check_key(owner.0)?;
        if self.store.owners.get(owner.0).is_none() {
            return Err(HandleError::Disposed);
        }
        self.checked_node(source.0, TypeId::of::<T>())?;
        let key = self.store.affects.insert((owner, source.0));
        self.store
            .owner_mut(owner)
            .resources
            .push(Resource::Affect(key));
        self.store.affect_changes.push((source.0, true));
        self.request_tick();
        Ok(Affect(key, PhantomData))
    }

    fn end_affect(&mut self, token: Affect) -> Result<(), HandleError> {
        self.check_key(token.0)?;
        if let Some((owner, source)) = self.store.affects.remove(token.0) {
            self.store
                .owner_mut(owner)
                .resources
                .retain(|resource| !matches!(resource, Resource::Affect(key) if *key == token.0));
            self.store.affect_changes.push((source, false));
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
