use super::*;

type Build<P> = Rc<dyn Fn(&mut Registration<P>)>;
type BuildError<P> = Rc<dyn Fn(&mut Registration<P>, ReadRef<Failure>)>;

pub(super) struct Loading<P> {
    on: Key,
    content: Owner,
    fallback: Option<Owner>,
    build_fallback: Build<P>,
    shown: bool,
    released: bool,
}

pub(super) enum Boundary<P> {
    Loading(Loading<P>),
    Errored(Errored<P>),
    Reveal(Reveal),
}

pub(super) struct Reveal {
    order: RevealOrder,
    collapse_tail: bool,
    opened: bool,
    released: bool,
}

pub(super) struct Errored<P> {
    content: Owner,
    fallback: Option<ErrorFallback>,
    build_fallback: BuildError<P>,
}

#[derive(Clone, Copy)]
struct ErrorFallback {
    owner: Owner,
    failure: Key,
}

#[derive(Clone)]
enum FallbackChange {
    BuildLoading(Owner),
    BuildErrored(Owner, Failure),
    UpdateFailure(Key, Failure),
}

#[derive(Clone, Copy)]
struct Slot {
    owner: Owner,
    minimum: bool,
    complete: bool,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
enum Gate {
    #[default]
    Open,
    Fallback,
    Hidden,
}

type Gates = BTreeMap<Key, Gate>;

pub(super) enum PublishGroup {
    Committed,
    Staged,
    Held,
}

#[derive(Clone, Default)]
struct Issues {
    waiting: bool,
    failure: Option<Failure>,
}

impl Issues {
    fn extend(&mut self, other: Self) {
        self.waiting |= other.waiting;
        self.failure = self.failure.take().or(other.failure);
    }
}

#[derive(Clone, Default)]
pub(super) struct BoundaryPlan {
    issues: Issues,
    visible: BTreeMap<Key, bool>,
    loading: BTreeMap<Key, bool>,
    fallbacks: Vec<FallbackChange>,
    recovered: Vec<(Key, Owner)>,
    slots: Vec<Slot>,
    reveal: Vec<(Owner, bool, bool)>,
}

struct Layout<'a, 'store, P> {
    projection: &'a FrameReads<'store, P>,
    reveals: BTreeMap<(Key, bool, Gate), BoundaryPlan>,
}

impl BoundaryPlan {
    fn extend(&mut self, other: Self) {
        self.issues.extend(other.issues);
        self.visible.extend(other.visible);
        self.loading.extend(other.loading);
        self.fallbacks.extend(other.fallbacks);
        self.recovered.extend(other.recovered);
        self.slots.extend(other.slots);
        self.reveal.extend(other.reveal);
    }

    pub(super) fn prepare<P>(projection: &FrameReads<'_, P>) -> Self {
        let mut layout = Layout {
            projection,
            reveals: BTreeMap::new(),
        };
        let mut plan = Self::default();
        for &root in &projection.store.roots {
            plan.extend(Self::owner(&mut layout, root, false, &Gates::new()));
        }
        plan
    }

    fn owner<P>(layout: &mut Layout<'_, '_, P>, owner: Owner, rearm: bool, gates: &Gates) -> Self {
        let projection = layout.projection;
        let store = projection.store;
        if !projection.includes(owner) {
            return Self::default();
        }
        if let Some(boundary) = store.boundaries.get(&owner.0) {
            let gate = gates.get(&owner.0).copied().unwrap_or_default();
            if gate == Gate::Hidden {
                let mut plan = Self::default();
                plan.visible.insert(owner.0, false);
                return plan;
            }
            return match boundary {
                Boundary::Loading(boundary) => Self::loading(layout, owner, boundary, rearm, gate),
                Boundary::Errored(boundary) => Self::errored(layout, owner, boundary, rearm, gates),
                Boundary::Reveal(boundary) => Self::reveal(layout, owner, boundary, rearm, gate),
            };
        }
        let mut plan = Self::default();
        let state = store.owners.get(owner.0).expect("included owner");
        for resource in &state.resources {
            if let Resource::Node(key) = resource {
                let node = store.node(*key);
                if node.role.is_render_reader() {
                    plan.issues.extend(Self::issue(projection, *key));
                }
                if let Some(error) = &node.effect_error {
                    if error.transaction.is_none_or(|id| {
                        projection.groups.representative(id) == projection.transaction
                    }) && projection
                        .reader_snapshot(*key)
                        .is_some_and(|snapshot| error.matches(snapshot))
                    {
                        plan.issues.extend(Issues {
                            waiting: false,
                            failure: Some(Rc::clone(&error.failure)),
                        });
                    }
                }
            }
        }
        for &child in &state.children {
            plan.extend(Self::owner(layout, child, rearm, gates));
        }
        plan
    }

    fn issue<P>(projection: &FrameReads<'_, P>, key: Key) -> Issues {
        let node = projection.store.node(key);
        let in_group = |s: &&Snapshot| {
            projection
                .groups
                .accepts(projection.transaction, &s.transactions)
        };
        let snapshot = projection
            .values
            .get(&key)
            .or_else(|| node.candidate.as_ref().filter(in_group))
            .or_else(|| node.lane_candidate().filter(in_group))
            .or_else(|| node.ahead.as_ref().and_then(|_| node.displayed()))
            .or_else(|| projection.reader_snapshot(key));
        match snapshot.map(|snapshot| &snapshot.outcome) {
            Some(Outcome::Answer(Answer::Value(_))) => Issues::default(),
            Some(Outcome::Answer(Answer::Failed(error))) => Issues {
                waiting: false,
                failure: Some(Rc::clone(error)),
            },
            _ => Issues {
                waiting: true,
                failure: None,
            },
        }
    }

    fn loading<P>(
        layout: &mut Layout<'_, '_, P>,
        owner: Owner,
        boundary: &Loading<P>,
        rearm: bool,
        gate: Gate,
    ) -> Self {
        if gate == Gate::Fallback {
            return Self::loading_fallback(layout, owner, boundary, rearm);
        }
        let projection = layout.projection;
        let on = Self::issue(projection, boundary.on);
        if on.waiting || on.failure.is_some() {
            let mut plan = Self {
                issues: on,
                ..Self::default()
            };
            if !boundary.released {
                plan.slots.push(Slot {
                    owner,
                    minimum: false,
                    complete: false,
                });
            }
            return plan;
        }
        let current = projection
            .reader_snapshot(boundary.on)
            .expect("ready on pass");
        let previous = projection.store.node(boundary.on).committed.as_ref();
        let rearmed = previous.is_some_and(|snapshot| {
            snapshot.reads.len() != current.reads.len()
                || snapshot.reads.iter().any(|read| {
                    !current
                        .reads
                        .iter()
                        .any(|now| read.source == now.source && read.version == now.version)
                })
        });
        let shown = boundary.shown && !rearmed && !rearm;
        let content = Self::owner(layout, boundary.content, rearm, &Gates::new());
        let ready = !content.issues.waiting && content.issues.failure.is_none();
        let mut plan = if content.issues.waiting && content.issues.failure.is_none() && !shown {
            Self::loading_fallback(layout, owner, boundary, rearm)
        } else {
            let mut plan = content;
            plan.visible.insert(owner.0, true);
            plan.visible.insert(boundary.content.0, true);
            if let Some(fallback) = boundary.fallback {
                plan.visible.insert(fallback.0, false);
            }
            plan.loading.insert(owner.0, true);
            plan
        };
        plan.slots.clear();
        if !boundary.released {
            plan.slots.push(Slot {
                owner,
                minimum: ready,
                complete: ready,
            });
        }
        plan
    }

    fn loading_fallback<P>(
        layout: &mut Layout<'_, '_, P>,
        owner: Owner,
        boundary: &Loading<P>,
        rearm: bool,
    ) -> Self {
        let mut plan = Self::default();
        let projection = layout.projection;
        if let Some(fallback) = boundary
            .fallback
            .filter(|fallback| projection.store.owners.get(fallback.0).is_some())
        {
            if projection.includes(fallback) {
                plan = Self::owner(layout, fallback, rearm, &Gates::new());
                plan.visible.insert(fallback.0, true);
            } else {
                plan.issues.waiting = true;
            }
        } else {
            plan.fallbacks.push(FallbackChange::BuildLoading(owner));
        }
        plan.visible.insert(owner.0, true);
        plan.visible.insert(boundary.content.0, false);
        plan.loading.insert(owner.0, false);
        plan
    }

    fn errored<P>(
        layout: &mut Layout<'_, '_, P>,
        owner: Owner,
        boundary: &Errored<P>,
        rearm: bool,
        gates: &Gates,
    ) -> Self {
        let projection = layout.projection;
        let fallback = boundary
            .fallback
            .filter(|fallback| projection.store.owners.get(fallback.owner.0).is_some());
        let shown = fallback.is_some_and(|fallback| {
            projection
                .store
                .owners
                .get(fallback.owner.0)
                .is_some_and(|state| state.visible)
        });
        let content = Self::owner(layout, boundary.content, rearm || shown, gates);
        let failure = content.issues.failure.clone();
        if failure.is_none() && (!shown || !content.issues.waiting) {
            let mut plan = content;
            plan.visible.insert(boundary.content.0, true);
            if let Some(fallback) = fallback.filter(|fallback| projection.includes(fallback.owner))
            {
                plan.recovered.push((owner.0, fallback.owner));
            }
            return plan;
        }
        let mut plan = Self::default();
        if let Some(fallback) = fallback {
            if projection.includes(fallback.owner) {
                plan = Self::owner(layout, fallback.owner, rearm, gates);
                plan.visible.insert(fallback.owner.0, true);
                if let Some(error) = failure {
                    let current = projection
                        .reader_snapshot(fallback.failure)
                        .and_then(|snapshot| snapshot.outcome.answer())
                        .and_then(|answer| match answer {
                            Answer::Value(value) => value.downcast_ref::<Failure>(),
                            Answer::Failed(_) => None,
                        });
                    if current.is_none_or(|old| !Rc::ptr_eq(old, &error)) {
                        plan.fallbacks
                            .push(FallbackChange::UpdateFailure(fallback.failure, error));
                    }
                }
            } else {
                plan.issues.waiting = true;
            }
        } else if let Some(error) = failure {
            plan.fallbacks
                .push(FallbackChange::BuildErrored(owner, error));
        }
        plan.visible.insert(boundary.content.0, false);
        plan
    }

    fn reveal<P>(
        layout: &mut Layout<'_, '_, P>,
        owner: Owner,
        group: &Reveal,
        rearm: bool,
        gate: Gate,
    ) -> Self {
        let cache = (owner.0, rearm, gate);
        if let Some(plan) = layout.reveals.get(&cache) {
            return plan.clone();
        }
        let projection = layout.projection;
        let state = projection
            .store
            .owners
            .get(owner.0)
            .expect("included reveal");
        let mut scan = |gates: &Gates| {
            let mut plan = Self::default();
            for &child in &state.children {
                plan.extend(Self::owner(layout, child, rearm, gates));
            }
            plan
        };
        let mut plan = scan(&Gates::new());
        plan.slots.sort_by_key(|slot| {
            projection
                .store
                .owners
                .get(slot.owner.0)
                .expect("live slot")
                .ordinal
        });
        let slots = &plan.slots;
        let minimum = group.opened
            || slots.is_empty()
            || match group.order {
                RevealOrder::Sequential => slots[0].minimum,
                RevealOrder::Natural => slots.iter().any(|slot| slot.minimum),
                RevealOrder::Together => slots.iter().all(|slot| slot.minimum),
            };
        let complete = slots.iter().all(|slot| slot.complete);
        let mut gates = Gates::new();
        if gate == Gate::Fallback || (group.order == RevealOrder::Together && !minimum) {
            gates.extend(slots.iter().map(|slot| (slot.owner.0, Gate::Fallback)));
        } else if group.order == RevealOrder::Sequential {
            let tail = if group.collapse_tail {
                Gate::Hidden
            } else {
                Gate::Fallback
            };
            if let Some(first_waiting) = slots.iter().position(|slot| !slot.complete) {
                gates.extend(
                    slots[first_waiting + 1..]
                        .iter()
                        .map(|slot| (slot.owner.0, tail)),
                );
            }
        }
        if !gates.is_empty() {
            plan = scan(&gates);
        }
        plan.slots.clear();
        if !group.released {
            plan.slots.push(Slot {
                owner,
                minimum,
                complete,
            });
        }
        plan.visible.insert(owner.0, true);
        if gate == Gate::Open {
            plan.reveal.push((owner, minimum, complete));
        }
        layout.reveals.insert(cache, plan.clone());
        plan
    }

    pub(super) fn held(&self) -> bool {
        self.issues.waiting
    }

    pub(super) fn failed(&self) -> bool {
        self.issues.failure.is_some()
    }

    pub(super) fn removals(&self) -> Vec<Owner> {
        self.recovered.iter().map(|&(_, owner)| owner).collect()
    }

    pub(super) fn local_visibility(&self, owner: Owner, state: &OwnerState) -> bool {
        self.visible.get(&owner.0).copied().unwrap_or(state.visible)
    }

    pub(super) fn needs_staging(&self) -> bool {
        !self.fallbacks.is_empty()
    }

    pub(super) fn stage<P>(&self, runtime: &mut Runtime<P>, transaction: Transaction) {
        for change in &self.fallbacks {
            match change {
                &FallbackChange::BuildLoading(owner) => {
                    let Some(Boundary::Loading(boundary)) = runtime.store.boundaries.get(&owner.0)
                    else {
                        unreachable!("live loading boundary");
                    };
                    let build = Rc::clone(&boundary.build_fallback);
                    let fallback = runtime.store.create_owner(Some(owner), transaction, None);
                    runtime.store.owner_mut(fallback).visible = false;
                    runtime.build_owner(fallback, |reg| build(reg));
                    let Some(Boundary::Loading(boundary)) =
                        runtime.store.boundaries.get_mut(&owner.0)
                    else {
                        unreachable!("fallback has a live boundary")
                    };
                    boundary.fallback = Some(fallback);
                }
                FallbackChange::BuildErrored(owner, error) => {
                    let Some(Boundary::Errored(boundary)) = runtime.store.boundaries.get(&owner.0)
                    else {
                        unreachable!("live error boundary");
                    };
                    let build = Rc::clone(&boundary.build_fallback);
                    let fallback = runtime.store.create_owner(Some(*owner), transaction, None);
                    runtime.store.owner_mut(fallback).visible = false;
                    let mut failure = None;
                    runtime.build_owner(fallback, |reg| {
                        let read = reg.register_with_compare(
                            Role::Source,
                            None,
                            Some(Rc::clone(error)),
                            |left, right| {
                                Rc::ptr_eq(
                                    left.downcast_ref::<Failure>().expect("failure signal"),
                                    right.downcast_ref::<Failure>().expect("failure signal"),
                                )
                            },
                        );
                        failure = Some(read.0);
                        build(reg, read);
                    });
                    let Some(Boundary::Errored(boundary)) =
                        runtime.store.boundaries.get_mut(&owner.0)
                    else {
                        unreachable!("fallback has a live boundary")
                    };
                    boundary.fallback = Some(ErrorFallback {
                        owner: fallback,
                        failure: failure.expect("fallback error signal"),
                    });
                }
                FallbackChange::UpdateFailure(key, error) => {
                    let mut transactions = vec![transaction];
                    if let Some(previous) = &runtime.store.node(*key).candidate {
                        transactions.extend(&previous.transactions);
                    }
                    if runtime.store.stage(
                        *key,
                        Outcome::Answer(Answer::Value(Rc::new(Rc::clone(error)))),
                        Vec::new(),
                        transactions,
                        true,
                    ) {
                        runtime.store.invalidate_subscribers(*key);
                    }
                }
            }
        }
        runtime.seed_sources();
    }

    pub(super) fn commit<P>(self, store: &mut Store<P>) {
        for (key, visible) in self.visible {
            if let Some(owner) = store.owners.get_mut(key) {
                owner.visible = visible;
            }
        }
        for (key, showing) in self.loading {
            let Some(Boundary::Loading(boundary)) = store.boundaries.get(&key) else {
                continue;
            };
            let shown = showing && store.visible(boundary.content);
            let Some(Boundary::Loading(boundary)) = store.boundaries.get_mut(&key) else {
                unreachable!("live loading boundary")
            };
            boundary.shown = shown;
            boundary.released |= shown;
        }
        for (key, _) in self.recovered {
            let Some(Boundary::Errored(boundary)) = store.boundaries.get_mut(&key) else {
                unreachable!("live error boundary")
            };
            boundary.fallback = None;
        }
        for (owner, minimum, complete) in self.reveal {
            if !store.visible(owner) {
                continue;
            }
            let Some(Boundary::Reveal(group)) = store.boundaries.get_mut(&owner.0) else {
                unreachable!("live reveal group")
            };
            group.opened |= minimum;
            group.released |= complete;
        }
    }
}

impl<P> Registration<P> {
    pub(super) fn child(&mut self, build: impl FnOnce(&mut Self)) -> Owner {
        let parent = self.owner;
        let transaction = self
            .store
            .owner_mut(parent)
            .registration
            .expect("registering into an unpublished owner");
        let child = self.store.create_owner(Some(parent), transaction, None);
        self.owner = child;
        build(self);
        self.owner = parent;
        child
    }

    pub(super) fn register_loading(
        &mut self,
        on: impl Fn(&mut Cx<'_>) -> Read<()> + 'static,
        content: impl Fn(&mut Self) + 'static,
        fallback: impl Fn(&mut Self) + 'static,
    ) {
        self.child(|reg| {
            let on = reg.register::<()>(
                Role::Loading,
                Some(Rc::new(move |cx| {
                    on(cx).map(|()| Computed::Value(Rc::new(())))
                })),
                None,
            );
            let content = reg.child(content);
            reg.store.owner_mut(content).visible = false;
            reg.store.boundaries.insert(
                reg.owner.0,
                Boundary::Loading(Loading {
                    on: on.0,
                    content,
                    fallback: None,
                    build_fallback: Rc::new(fallback),
                    shown: false,
                    released: false,
                }),
            );
        });
    }

    pub(super) fn register_errored(
        &mut self,
        content: impl Fn(&mut Self) + 'static,
        fallback: impl Fn(&mut Self, ReadRef<Failure>) + 'static,
    ) {
        self.child(|reg| {
            let content = reg.child(content);
            reg.store.owner_mut(content).visible = false;
            reg.store.boundaries.insert(
                reg.owner.0,
                Boundary::Errored(Errored {
                    content,
                    fallback: None,
                    build_fallback: Rc::new(fallback),
                }),
            );
        });
    }

    pub(super) fn register_reveal(
        &mut self,
        order: RevealOrder,
        collapse_tail: bool,
        build: impl FnOnce(&mut Self),
    ) {
        self.child(|reg| {
            reg.store.boundaries.insert(
                reg.owner.0,
                Boundary::Reveal(Reveal {
                    order,
                    collapse_tail,
                    opened: false,
                    released: false,
                }),
            );
            build(reg);
        });
    }
}
