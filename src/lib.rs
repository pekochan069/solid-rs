mod arena;
mod async_jobs;
mod runtime;

use std::{
    any::{Any, TypeId},
    error::Error,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

pub use runtime::{Registration, Runtime};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Key {
    runtime: u64,
    index: u32,
    generation: u64,
}

type LocalInvariant<T> = PhantomData<(fn(T) -> T, Rc<()>)>;

pub struct ReadRef<T>(Key, LocalInvariant<T>);
pub struct WriteRef<T>(ReadRef<T>);

impl<T> Copy for ReadRef<T> {}
impl<T> Clone for ReadRef<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for WriteRef<T> {}
impl<T> Clone for WriteRef<T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Owner(Key, PhantomData<Rc<()>>);

pub type Failure = Rc<dyn Error>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingRead {
    attempt: u64,
    wait: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandleError {
    WrongRuntime,
    Disposed,
    Poisoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitError {
    RuntimeIdsExhausted,
}

#[derive(Clone, Debug)]
pub enum ReadError {
    Pending(PendingRead),
    Failed(Failure),
    Handle(HandleError),
}

impl From<HandleError> for ReadError {
    fn from(error: HandleError) -> Self {
        Self::Handle(error)
    }
}

pub type Read<T> = Result<T, ReadError>;
pub type JobResult<T> = Result<T, Failure>;
pub type LocalFuture<T> = Pin<Box<dyn Future<Output = JobResult<T>>>>;
pub type LocalStream<T> = Pin<Box<dyn Stream<Item = JobResult<T>>>>;

pub trait Stream {
    type Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
}

pub enum Derive<T> {
    Value(T),
    Future(LocalFuture<T>),
    Stream(LocalStream<T>),
}

type Value = Rc<dyn Any>;

#[derive(Clone, Copy)]
struct Dependency {
    source: Key,
    version: u64,
}

trait ReadGraph {
    fn read(&mut self, key: Key, ty: TypeId, reads: &mut Vec<Dependency>) -> Read<Value>;
}

pub struct Cx<'a> {
    graph: &'a mut dyn ReadGraph,
    reads: &'a mut Vec<Dependency>,
}

impl Cx<'_> {
    pub fn get<T: 'static>(&mut self, source: ReadRef<T>) -> Read<Rc<T>> {
        self.graph
            .read(source.0, TypeId::of::<T>(), self.reads)
            .map(downcast)
    }
}

pub struct EffectCx<'a> {
    writes: &'a mut dyn WriteQueue,
}

trait WriteQueue {
    fn enqueue(&mut self, key: Key, ty: TypeId, value: Value) -> Result<(), HandleError>;
}

impl EffectCx<'_> {
    pub fn set<T: 'static>(&mut self, target: WriteRef<T>, value: T) -> Result<(), HandleError> {
        self.writes
            .enqueue(target.0 .0, TypeId::of::<T>(), Rc::new(value))
    }
}

fn downcast<T: 'static>(value: Value) -> Rc<T> {
    value.downcast().expect("validated signal value type")
}

#[derive(Debug)]
pub enum FrameOp<P> {
    Attach {
        owner: Owner,
        parent: Option<Owner>,
        before: Option<Owner>,
    },
    SetVisible(Owner, bool),
    Patch(Owner, P),
    Remove(Owner),
}

#[derive(Debug)]
pub struct Frame<P> {
    pub sequence: u64,
    pub ops: Vec<FrameOp<P>>,
}

pub trait FrameSink<P> {
    fn publish(&mut self, frame: Frame<P>) -> Result<(), Failure>;
}

#[derive(Clone, Debug)]
pub enum FlushError {
    Busy,
    Poisoned,
    Publish(Failure),
}

pub trait Register<P>: Sized {
    fn owner(&self) -> Owner;

    fn signal<T: PartialEq + 'static>(&mut self, initial: T) -> (ReadRef<T>, WriteRef<T>);

    fn memo<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut Cx<'_>) -> Read<Derive<T>> + 'static,
    ) -> ReadRef<T>;

    fn derived_signal<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut Cx<'_>) -> Read<Derive<T>> + 'static,
    ) -> (ReadRef<T>, WriteRef<T>);

    fn render<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut Cx<'_>) -> Read<T> + 'static,
        apply: impl Fn(Rc<T>) -> P + 'static,
    );

    fn effect<T: PartialEq + 'static>(
        &mut self,
        compute: impl Fn(&mut Cx<'_>) -> Read<T> + 'static,
        apply: impl FnMut(&mut EffectCx<'_>, Rc<T>) -> JobResult<()> + 'static,
    );

    fn on_cleanup(&mut self, cleanup: impl FnOnce() + 'static);

    fn switch<K: PartialEq + 'static>(
        &mut self,
        key: impl Fn(&mut Cx<'_>) -> Read<K> + 'static,
        build: impl Fn(&mut Self, Rc<K>) + 'static,
    );
}

pub trait Drive<P> {
    type Register: Register<P>;

    fn install(&mut self, build: impl FnOnce(&mut Self::Register)) -> Result<Owner, HandleError>;

    fn dispose(&mut self, owner: Owner) -> Result<(), HandleError>;

    fn set<T: 'static>(&mut self, target: WriteRef<T>, value: T) -> Result<(), HandleError>;

    fn update<T: 'static>(&mut self, target: WriteRef<T>, update: impl FnOnce(&T) -> T)
        -> Read<()>;

    fn committed<T: 'static>(&self, source: ReadRef<T>) -> Read<Rc<T>>;

    fn latest<T: 'static>(&self, source: ReadRef<T>) -> Read<Rc<T>>;

    fn refresh<T: 'static>(&mut self, source: ReadRef<T>) -> Result<(), HandleError>;

    fn flush(&mut self, sink: &mut impl FrameSink<P>) -> Result<(), FlushError>;
}
