use super::*;

#[derive(Default)]
pub(super) struct AheadState {
    // These snapshots belong to the displayed lane, independently of the main graph.
    committed: Option<Snapshot>,
    candidate: Option<Snapshot>,
    candidate_changed: bool,
    flight: Option<Flight>,
    question: u64,
}

impl AheadState {
    pub(super) fn has_candidate(&self) -> bool {
        self.candidate.is_some()
    }

    fn recovering(&self) -> bool {
        self.committed
            .as_ref()
            .is_some_and(|s| matches!(s.outcome, Outcome::Answer(Answer::Failed(_))))
    }

    fn current(&self) -> Option<&Snapshot> {
        self.candidate.as_ref().or(self.committed.as_ref())
    }

    pub(super) fn view(&self, affects: usize) -> ReadView<'_> {
        ReadView {
            current: self.current(),
            committed: self.committed.as_ref(),
            latest: self.current(),
            pending: self
                .committed
                .as_ref()
                .map(|_| affects > 0 || (self.has_candidate() && self.candidate_changed)),
        }
    }

    pub(super) fn discard(&mut self) {
        let retired = (self.flight.take(), self.candidate.take());
        drop(retired);
    }
}

impl<P> Node<P> {
    pub(super) fn lane_candidate(&self) -> Option<&Snapshot> {
        self.ahead.as_ref()?.candidate.as_ref()
    }

    pub(super) fn lane_candidate_mut(&mut self) -> Option<&mut Snapshot> {
        self.ahead.as_mut()?.candidate.as_mut()
    }

    pub(super) fn displayed(&self) -> Option<&Snapshot> {
        self.ahead
            .as_ref()
            .and_then(|ahead| ahead.committed.as_ref())
            .or(self.committed.as_ref())
    }
}

struct LaneReads<'a, P> {
    id: u64,
    store: &'a mut Store<P>,
    mailbox: &'a Arc<Mailbox>,
    next_flight: &'a mut u64,
    checked: HashSet<Key>,
    running: HashSet<Key>,
}

impl<P> LaneReads<'_, P> {
    fn view(node: &Node<P>) -> ReadView<'_> {
        if let Some(ahead) = &node.ahead {
            return ahead.view(node.affects);
        }
        let committed = node.committed.as_ref();
        ReadView {
            current: committed,
            committed,
            latest: node.current(),
            pending: committed.map(|_| node.pending_status()),
        }
    }

    fn resolve(&mut self, key: Key) -> Result<(), HandleError> {
        if self.running.contains(&key) || !self.checked.insert(key) {
            return Ok(());
        }
        let Some(node) = self.store.nodes.get(key) else {
            return Ok(());
        };
        let Some(ahead) = &node.ahead else {
            return Ok(());
        };
        if self
            .store
            .owners
            .get(node.owner.0)
            .is_none_or(|owner| owner.registration.is_some())
        {
            return Ok(());
        }
        let previous = ahead.current().map(|snapshot| snapshot.reads.clone());
        self.running.insert(key);
        let mut changed = previous.is_none();
        for dep in previous.unwrap_or_default() {
            self.resolve(dep.source)?;
            let observed = self
                .store
                .nodes
                .get(dep.source)
                .filter(|_| !self.running.contains(&dep.source))
                .map_or(0, |source| Self::view(source).observed(dep.mode));
            if dep.observed != observed {
                changed = true;
                break;
            }
        }
        if changed {
            let node = self.store.node_mut(key);
            let ahead = node.ahead.as_mut().expect("ahead state");
            ahead.question = ahead
                .question
                .checked_add(1)
                .expect("ahead question exhausted");
            let retired = ahead.flight.take();
            let compute = node.compute.clone().expect("ahead compute");
            drop(retired);
            let mut reads = Vec::new();
            let result = compute(&mut Cx {
                graph: self,
                reads: &mut reads,
            });
            let outcome = match result {
                Ok(Computed::Value(value)) => Outcome::Answer(Answer::Value(value)),
                Ok(Computed::Job(work)) => self.start(key, work),
                Err(ReadError::Failed(error)) => Outcome::Answer(Answer::Failed(error)),
                Err(ReadError::Handle(error)) => return Err(error),
                Err(ReadError::Pending(wait)) => {
                    if !reads.iter().any(|dep| {
                        self.store.nodes.get(dep.source).is_some_and(|source| {
                            matches!(Self::view(source).read::<P>(dep.source, dep.mode), Err(ReadError::Pending(token)) if token == wait)
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
            self.stage(key, outcome, reads);
        }
        self.running.remove(&key);
        Ok(())
    }

    fn stage(&mut self, key: Key, outcome: Outcome, reads: Vec<Dependency>) {
        let node = self.store.node_mut(key);
        let ahead = node.ahead.as_mut().expect("ahead state");
        let version = ahead
            .current()
            .filter(|previous| outcome.same(&previous.outcome, node.compare))
            .map_or_else(
                || {
                    node.next_version
                        .checked_add(1)
                        .expect("answer version exhausted")
                },
                |previous| previous.version,
            );
        node.next_version = node.next_version.max(version);
        ahead.candidate_changed = ahead
            .committed
            .as_ref()
            .is_none_or(|old| !outcome.same(&old.outcome, node.compare));
        let transactions = ahead
            .candidate
            .as_mut()
            .map(|s| std::mem::take(&mut s.transactions))
            .unwrap_or_default();
        let previous = ahead.candidate.replace(Snapshot {
            outcome,
            version,
            question: ahead.question,
            reads,
            transactions,
            proposal: false,
        });
        drop(previous);
    }

    fn start(&mut self, key: Key, work: Work) -> Outcome {
        *self.next_flight = self
            .next_flight
            .checked_add(1)
            .expect("flight generation exhausted");
        let ahead = self
            .store
            .node_mut(key)
            .ahead
            .as_mut()
            .expect("ahead state");
        let id = FlightId {
            node: key,
            domain: EvaluationDomain::Ahead,
            question: ahead.question,
            generation: *self.next_flight,
        };
        ahead.flight = Some(Flight::new(id, work));
        self.poll(id)
            .and_then(Runtime::<P>::poll_answer)
            .map_or_else(
                || Outcome::Pending {
                    wait: PendingRead {
                        attempt: self.id,
                        wait: id.generation,
                    },
                    retained: None,
                },
                Outcome::Answer,
            )
    }

    fn poll(&mut self, id: FlightId) -> Option<Progress> {
        let ahead = self.store.nodes.get_mut(id.node)?.ahead.as_mut()?;
        Flight::poll_slot(&mut ahead.flight, id, self.mailbox)
    }

    fn advance(&mut self, ready: impl Iterator<Item = FlightId>) -> Result<(), HandleError> {
        for id in ready {
            self.resolve(id.node)?;
            let Some(answer) = self.poll(id).and_then(Runtime::<P>::poll_answer) else {
                continue;
            };
            let current = self
                .store
                .node(id.node)
                .ahead
                .as_ref()
                .and_then(|ahead| ahead.current())
                .expect("live ahead flight");
            self.stage(id.node, Outcome::Answer(answer), current.reads.clone());
            self.checked.clear();
        }
        Ok(())
    }
}

impl<P> ReadGraph for LaneReads<'_, P> {
    fn read(
        &mut self,
        key: Key,
        ty: TypeId,
        mode: ReadMode,
        reads: &mut Vec<Dependency>,
    ) -> Read<Value> {
        if key.runtime != self.id {
            return Err(HandleError::WrongRuntime.into());
        }
        let node = self.store.nodes.get(key).ok_or(HandleError::Disposed)?;
        assert_eq!(node.ty, ty, "handle has a different type than its node");
        if self.running.contains(&key) {
            record_read(reads, key, mode, 0, 0);
            return Err(ReadError::Failed(Rc::new(Cycle)));
        }
        self.resolve(key)?;
        let view = Self::view(self.store.node(key));
        let observed = view.observed(mode);
        record_read(reads, key, mode, observed, observed);
        view.read::<P>(key, mode)
    }
}

impl<P> Runtime<P> {
    pub(super) fn commit_lane(&mut self, key: Key) -> (Option<Snapshot>, Option<FlightId>) {
        let ahead = self
            .store
            .node_mut(key)
            .ahead
            .as_mut()
            .expect("ahead state");
        let mut snapshot = ahead.candidate.take().expect("ahead publication candidate");
        snapshot.transactions.clear();
        let previous = ahead.committed.replace(snapshot);
        let resume = ahead.flight.as_mut().and_then(Flight::resume_after_commit);
        (previous, resume)
    }

    pub(super) fn adopt_main_ahead(&mut self, key: Key) -> Option<AheadState> {
        let node = self.store.node_mut(key);
        let ahead = node.ahead.as_deref_mut()?;
        let next = AheadState {
            committed: node.committed.clone(),
            question: ahead.question,
            ..AheadState::default()
        };
        Some(std::mem::replace(ahead, next))
    }

    pub(super) fn publish_ahead(
        &mut self,
        sink: &mut impl FrameSink<P>,
        ready: impl Iterator<Item = FlightId>,
    ) -> Result<(), FlushError> {
        let visible: Vec<_> = self
            .store
            .nodes
            .keys()
            .filter(|&key| {
                let node = self.store.node(key);
                node.ahead
                    .as_ref()
                    .is_some_and(|ahead| self.store.visible(node.owner) || ahead.recovering())
            })
            .collect();
        let mut lane = LaneReads {
            id: self.id,
            store: &mut self.store,
            mailbox: &self.mailbox,
            next_flight: &mut self.next_flight,
            checked: HashSet::new(),
            running: HashSet::new(),
        };
        lane.advance(ready).map_err(|_| FlushError::Poisoned)?;
        for &key in &visible {
            lane.resolve(key).map_err(|_| FlushError::Poisoned)?;
        }

        // Shared `latest` inputs and ahead-to-ahead reads make one publication unit.
        let mut groups = TransactionGroups::default();
        let mut latest = BTreeMap::new();
        let mut members: BTreeSet<_> = visible.iter().copied().collect();
        let mut visit = visible;
        while let Some(key) = visit.pop() {
            let node = self.store.node(key);
            let ahead = node.ahead.as_ref().expect("ahead state");
            let id = u64::from(key.index);
            for dep in ahead
                .committed
                .iter()
                .chain(&ahead.candidate)
                .flat_map(|s| &s.reads)
            {
                if self
                    .store
                    .nodes
                    .get(dep.source)
                    .is_some_and(|node| node.ahead.is_some())
                {
                    groups.join(&[id, u64::from(dep.source.index)]);
                    if members.insert(dep.source) {
                        visit.push(dep.source);
                    }
                }
                if dep.mode == ReadMode::Latest {
                    if let Some(other) = latest.insert(dep.source, id) {
                        groups.join(&[id, other]);
                    }
                }
            }
        }
        let mut publications = BTreeMap::<_, Vec<_>>::new();
        for key in members {
            publications
                .entry(groups.representative(u64::from(key.index)))
                .or_default()
                .push(key);
        }
        for keys in publications.values() {
            // Boundary changes need the same frame as every display sharing the lane.
            let mut transactions: Vec<_> = keys
                .iter()
                .filter_map(|&key| self.store.node(key).lane_candidate())
                .flat_map(|s| &s.transactions)
                .copied()
                .collect();
            let boundary_change = keys.iter().any(|&key| {
                let node = self.store.node(key);
                node.role.is_render_reader()
                    && node.lane_candidate().is_some_and(|s| {
                        matches!(s.outcome, Outcome::Answer(Answer::Failed(_)))
                            || node.ahead.as_ref().is_some_and(|ahead| ahead.recovering())
                    })
            });
            let blocked = keys.iter().any(|&key| {
                let node = self.store.node(key);
                node.role.is_render_reader()
                    && self.store.visible(node.owner)
                    && node
                        .ahead
                        .as_ref()
                        .and_then(|ahead| ahead.current())
                        .is_some_and(|candidate| candidate.outcome.wait().is_some())
            });
            if !transactions.is_empty() || (boundary_change && !blocked) {
                if transactions.is_empty() {
                    transactions.push(self.store.new_transaction());
                }
                transactions.sort_unstable();
                transactions.dedup();
                for &key in keys {
                    let ahead = self
                        .store
                        .node_mut(key)
                        .ahead
                        .as_mut()
                        .expect("ahead state");
                    if let Some(candidate) = &mut ahead.candidate {
                        candidate.transactions.clone_from(&transactions);
                    }
                }
                continue;
            }
            if blocked {
                continue;
            }
            let mut ops = Vec::new();
            let mut staged = Vec::new();
            for &key in keys {
                let node = self.store.node(key);
                let Some(snapshot) = node
                    .ahead
                    .as_ref()
                    .and_then(|ahead| ahead.candidate.as_ref())
                else {
                    continue;
                };
                let Outcome::Answer(answer) = &snapshot.outcome else {
                    continue;
                };
                if let Role::Render(apply) = &node.role {
                    if !self.store.visible(node.owner) {
                        continue;
                    }
                    if let Answer::Value(value) = answer {
                        if node.displayed().is_none_or(|previous| {
                            !snapshot.outcome.same(&previous.outcome, node.compare)
                        }) {
                            ops.push(FrameOp::Patch(node.owner, apply(Rc::clone(value))));
                        }
                    }
                }
                staged.push(key);
            }
            let mut replaced = Vec::with_capacity(staged.len());
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
            for key in staged {
                let (previous, flight) = self.commit_lane(key);
                replaced.push(previous);
                resume.extend(flight);
            }
            drop(replaced);
            for id in resume {
                self.mailbox.request_tick(Some(id));
            }
        }
        Ok(())
    }
}
