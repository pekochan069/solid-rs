use super::*;

#[derive(Default)]
pub(super) struct FrameState {
    pub(super) values: BTreeMap<Key, Snapshot>,
    pub(super) flights: BTreeMap<Key, Flight>,
    pub(super) branches: BTreeMap<Key, (Owner, Value)>,
    pub(super) published: bool,
}

pub(super) struct FrameReads<'a, P> {
    pub(super) id: u64,
    pub(super) store: &'a Store<P>,
    pub(super) groups: &'a TransactionGroups,
    pub(super) transaction: Transaction,
    pub(super) retiring: HashSet<Owner>,
    pub(super) checked: HashSet<Key>,
    pub(super) running: HashSet<Key>,
    pub(super) cache: FrameState,
    pub(super) mailbox: &'a Arc<Mailbox>,
    pub(super) next_flight: &'a mut u64,
    pub(super) ready: &'a mut HashSet<FlightId>,
    pub(super) values: BTreeMap<Key, Snapshot>,
}

impl<P> FrameReads<'_, P> {
    pub(super) fn suspend(mut self) -> FrameState {
        self.cache.values.extend(self.values);
        self.cache
            .flights
            .retain(|key, _| self.cache.values.contains_key(key));
        self.cache
    }

    pub(super) fn finish(mut self) -> FrameState {
        self.cache.values.clear();
        self.suspend()
    }

    pub(super) fn includes(&self, owner: Owner) -> bool {
        self.store.owners.get(owner.0).is_some_and(|state| {
            state
                .registration
                .is_none_or(|id| self.groups.representative(id) == self.transaction)
                && !self.store.owned_by(owner, &self.retiring)
        })
    }

    pub(super) fn reader_snapshot(&self, key: Key) -> Option<&Snapshot> {
        let node = self.store.node(key);
        self.values
            .get(&key)
            .or_else(|| {
                node.candidate.as_ref().filter(|snapshot| {
                    snapshot.transactions.is_empty()
                        || self
                            .groups
                            .accepts(self.transaction, &snapshot.transactions)
                })
            })
            .or(node.committed.as_ref())
    }

    fn snapshot(&self, key: Key) -> Option<&Snapshot> {
        let node = self.store.nodes.get(key)?;
        self.values
            .get(&key)
            .or_else(|| {
                node.candidate.as_ref().filter(|candidate| {
                    !self.store.owned_by(node.owner, &self.retiring)
                        && self
                            .groups
                            .accepts(self.transaction, &candidate.transactions)
                })
            })
            .or(node.committed.as_ref())
    }

    pub(super) fn resolve(&mut self, key: Key) -> Result<(), HandleError> {
        if !self.checked.insert(key) {
            return Ok(());
        }
        let Some(node) = self.store.nodes.get(key) else {
            return Ok(());
        };
        let Some(compute) = node.compute.clone() else {
            return Ok(());
        };
        let previous = node.committed.as_ref();
        if self.store.owned_by(node.owner, &self.retiring)
            || node.candidate.as_ref().is_some_and(|candidate| {
                self.groups
                    .accepts(self.transaction, &candidate.transactions)
            })
            || (previous.is_none()
                && !self
                    .store
                    .owners
                    .get(node.owner.0)
                    .is_some_and(|owner| owner.frame_only))
        {
            return Ok(());
        }
        self.running.insert(key);
        if let Some(mut snapshot) = self.cache.values.remove(&key) {
            if self.inputs_match(&snapshot.reads)? {
                let ready = self
                    .cache
                    .flights
                    .get(&key)
                    .map(|flight| flight.id)
                    .filter(|id| self.ready.remove(id));
                if let Some(answer) = ready.and_then(|id| self.poll(key, id)) {
                    let outcome = Outcome::Answer(answer);
                    if !snapshot.outcome.same(&outcome, node.compare) {
                        snapshot.version = node
                            .next_version
                            .checked_add(1)
                            .expect("answer version exhausted");
                    }
                    snapshot.outcome = outcome;
                }
                self.values.insert(key, snapshot);
                self.running.remove(&key);
                return Ok(());
            }
            self.cache.flights.remove(&key);
        }
        if match previous {
            Some(previous) => !self.inputs_match(&previous.reads)?,
            None => true,
        } {
            let mut reads = Vec::new();
            let result = compute(&mut Cx {
                graph: self,
                reads: &mut reads,
            });
            let outcome = match result {
                Ok(Computed::Value(value)) => Outcome::Answer(Answer::Value(value)),
                Err(ReadError::Failed(error)) => Outcome::Answer(Answer::Failed(error)),
                Err(ReadError::Handle(error)) => return Err(error),
                Err(ReadError::Pending(wait)) => {
                    if !reads.iter().any(|dep| {
                        self.snapshot(dep.source).map_or_else(
                            || matches!(Runtime::<P>::pending(dep.source), ReadError::Pending(token)
                                if dep.version == 0 && token == wait),
                            |snapshot| snapshot.version == dep.version && snapshot.outcome.wait() == Some(wait),
                        )
                    }) {
                        return Err(HandleError::Poisoned);
                    }
                    Outcome::Pending {
                        wait,
                        retained: None,
                    }
                }
                Ok(Computed::Job(work)) => self.start(key, work),
            };
            let version = previous
                .filter(|s| outcome.same(&s.outcome, node.compare))
                .map_or_else(
                    || {
                        node.next_version
                            .checked_add(1)
                            .expect("answer version exhausted")
                    },
                    |s| s.version,
                );
            self.values.insert(
                key,
                Snapshot {
                    outcome,
                    version,
                    question: previous.map_or(1, |s| s.question),
                    reads,
                    transactions: Vec::new(),
                    proposal: false,
                },
            );
        }
        self.running.remove(&key);
        Ok(())
    }

    fn inputs_match(&mut self, reads: &[Dependency]) -> Result<bool, HandleError> {
        for dep in reads {
            self.resolve(dep.source)?;
            let version = if self.running.contains(&dep.source) {
                0
            } else {
                self.snapshot(dep.source).map_or(0, |s| s.version)
            };
            if version != dep.version {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn start(&mut self, key: Key, work: Work) -> Outcome {
        *self.next_flight = self
            .next_flight
            .checked_add(1)
            .expect("flight generation exhausted");
        let id = FlightId {
            node: key,
            domain: EvaluationDomain::Frame,
            question: *self.next_flight,
            generation: *self.next_flight,
        };
        self.cache.flights.insert(key, Flight::new(id, work));
        self.poll(key, id).map_or(
            Outcome::Pending {
                wait: PendingRead {
                    attempt: self.id,
                    wait: id.generation,
                },
                retained: None,
            },
            Outcome::Answer,
        )
    }

    fn poll(&mut self, key: Key, id: FlightId) -> Option<Answer> {
        let mut slot = self.cache.flights.remove(&key);
        let progress = Flight::poll_slot(&mut slot, id, self.mailbox);
        if let Some(flight) = slot {
            self.cache.flights.insert(key, flight);
        }
        progress.and_then(Runtime::<P>::poll_answer)
    }
}

impl<P> ReadGraph for FrameReads<'_, P> {
    fn read(
        &mut self,
        key: Key,
        ty: TypeId,
        mode: ReadMode,
        reads: &mut Vec<Dependency>,
    ) -> Read<Value> {
        if key.runtime != self.id {
            return Err(ReadError::Handle(HandleError::WrongRuntime));
        }
        let node = self.store.nodes.get(key).ok_or(HandleError::Disposed)?;
        assert_eq!(node.ty, ty, "handle has a different type than its node");
        if self.running.contains(&key) {
            record_read(reads, key, mode, 0, 0);
            return Err(ReadError::Failed(Rc::new(Cycle)));
        }
        self.resolve(key)?;
        let snapshot = self.snapshot(key);
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

impl<P> Runtime<P> {
    pub(super) fn save_frame(&mut self, transaction: Transaction, state: FrameState) {
        for (&key, snapshot) in &state.values {
            if let Some(node) = self.store.nodes.get_mut(key) {
                node.next_version = node.next_version.max(snapshot.version);
            }
        }
        self.frame_cache.insert(transaction, state);
    }

    pub(super) fn prune_disposed_frames(&mut self) {
        self.frame_cache.retain(|_, state| {
            state.values.retain(|key, snapshot| {
                self.store.nodes.get(*key).is_some()
                    && (!state.published
                        || snapshot.reads.iter().all(|dep| {
                            self.store
                                .nodes
                                .get(dep.source)
                                .and_then(|node| node.committed.as_ref())
                                .map_or(0, |s| s.version)
                                == dep.version
                        }))
            });
            state
                .flights
                .retain(|key, _| state.values.contains_key(key));
            state
                .branches
                .retain(|_, (owner, _)| self.store.owners.get(owner.0).is_some());
            !state.values.is_empty() || !state.branches.is_empty()
        });
        self.frame_ready.retain(|id| {
            self.frame_cache.values().any(|state| {
                state
                    .flights
                    .get(&id.node)
                    .is_some_and(|flight| flight.id == *id)
            })
        });
    }

    pub(super) fn reconcile_frames(&mut self, groups: &TransactionGroups, active: &[Transaction]) {
        let mut retired = Vec::new();
        let obsolete: Vec<_> = self
            .frame_cache
            .iter()
            .filter_map(|(&id, state)| {
                (groups.representative(id) != id || (!state.published && !active.contains(&id)))
                    .then_some(id)
            })
            .collect();
        for id in obsolete {
            let mut state = self.frame_cache.remove(&id).expect("existing frame");
            let group = groups.representative(id);
            if !state.published && !active.contains(&group) {
                for (owner, _) in state.branches.into_values() {
                    self.store.remove_tree(owner, &mut retired);
                }
                continue;
            }
            let target = self.frame_cache.entry(group).or_default();
            target.published |= state.published;
            for (key, snapshot) in state.values {
                if let std::collections::btree_map::Entry::Vacant(entry) = target.values.entry(key)
                {
                    entry.insert(snapshot);
                    if let Some(flight) = state.flights.remove(&key) {
                        target.flights.insert(key, flight);
                    }
                }
            }
            for (key, (owner, value)) in state.branches {
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    target.branches.entry(key)
                {
                    entry.insert((owner, value));
                } else {
                    self.store.remove_tree(owner, &mut retired);
                }
            }
        }
        self.prune_disposed_frames();
        clean(retired);
    }
}
