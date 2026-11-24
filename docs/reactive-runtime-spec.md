# Rust 반응형 런타임 구현 명세

## 범위

`signals`는 그래프, 소유권, 비동기 폴링, 프레임 스케줄러를 제공하는 `std` 전용 크레이트입니다. 이후 GPU UI 크레이트가 `signals`에 의존하며, 이벤트 수신과 위젯 트리의 변경, 레이아웃, GPU 프레임 제출을 맡습니다. CPU 렌더러 없이도 빈 호스트와 패치 기록기로 런타임을 구동할 수 있어야 합니다.

첫 구현은 가변 시그널, 메모, 쓰기 가능한 파생 시그널, Future와 스트림, L2 보류, 분할 이펙트, Loading·Errored·Reveal, `latest`·`is_pending`·`refresh`·`affects`를 포함합니다. 디스플레이 선행 계산에는 자체 공개 단위를 둡니다. 낙관적 쓰기·롤백, `action`, 스토어와 프로젝션은 다음 단계에서 같은 프레임 불변식 위에 추가합니다. `createDeferred`의 명시적 지연과 SSR·하이드레이션은 별도 기능으로 둡니다.

런타임은 생성 스레드에서만 그래프를 조작하고 `Future`를 폴링합니다. 반응형 읽기는 `Result<T, ReadError>`를 반환하므로 계산 클로저에서 `?`로 전파할 수 있습니다. 나머지 API 이름과 호스트 실패 처리, 스트림 종료 정책은 구현을 시작할 수 있도록 이 문서에서 정한 첫 구현의 기본값입니다.

## 용어

| 용어 | 의미 |
| --- | --- |
| 소유자 | 노드, 하위 소유자, 실행 중인 작업과 정리 함수를 거느리는 수명 단위입니다. |
| 틱 | 한 번의 `flush`가 가져온 입력 묶음입니다. |
| 프레임 | 한 번에 공개되는 트리 변경과 해당 변경이 읽은 그래프 상태입니다. 화면 기록기에서 프레임의 모든 변경을 한 항목으로 검사할 수 있습니다. |
| 트랜잭션 | 함께 공개해야 할 쓰기, 후보 결과와 프레임을 추적하는 스케줄러 단위입니다. |
| 보류 | 프레임이 필요한 현재 질문의 답을 기다리는 트랜잭션 상태입니다. |
| 질문 | 특정 논리 노드를 특정 입력 버전과 공개 문맥에서 계산하라는 요청입니다. 재시도에도 같은 질문을 쓸 수 있으며, 비동기 작업의 실행 세대는 매번 달라집니다. |
| 프레임 리더 | 커밋된 화면에 붙어 있거나 그 화면을 대체할 후보에 속한 렌더 계산입니다. |
| 선행 레인 | `latest`나 `is_pending`을 읽는 표시와 그 파생 결과를 함께 공개하는 단위입니다. |

## 공개 인터페이스

아래의 트레이트 선언은 메서드 시그니처를 컴파일할 수 있게 적은 것입니다. 구현 시 `Runtime<P>`가 `Drive<P>`를, 등록 문맥이 `Register<P>`를 구현합니다. `P`는 호스트가 해석하는 패치 타입입니다. `ReadRef<T>`와 `WriteRef<T>`는 `T: Copy` 없이 복사할 수 있으며, 읽은 값은 `Rc<T>`로 소유합니다. 런타임이 저장소를 빌린 상태로 사용자 코드에 값을 건네는 경로를 막습니다.

```rust,no_run
use std::{
    error::Error, future::Future, marker::PhantomData, pin::Pin, rc::Rc,
    task::{Context, Poll},
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Key {
    runtime: u64,
    index: u32,
    generation: u64,
}

pub struct ReadRef<T>(Key, PhantomData<(fn(T) -> T, Rc<()>)>);
pub struct WriteRef<T>(ReadRef<T>);
impl<T> Copy for ReadRef<T> {}
impl<T> Clone for ReadRef<T> { fn clone(&self) -> Self { *self } }
impl<T> Copy for WriteRef<T> {}
impl<T> Clone for WriteRef<T> { fn clone(&self) -> Self { *self } }

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Owner(Key, PhantomData<Rc<()>>);
#[derive(Clone, Copy)]
pub struct Affect(Key, PhantomData<Rc<()>>);

pub type Failure = Rc<dyn Error>;
pub struct PendingRead { attempt: u64, wait: u64 }
pub enum HandleError { WrongRuntime, Disposed, Poisoned }
pub enum InitError { RuntimeIdsExhausted }
pub enum ReadError {
    Pending(PendingRead),
    Failed(Failure),
    Handle(HandleError),
}
pub type Read<T> = Result<T, ReadError>;
pub type JobResult<T> = Result<T, Failure>;
pub type LocalFuture<T> = Pin<Box<dyn Future<Output = JobResult<T>>>>;
pub type LocalStream<T> = Pin<Box<dyn Stream<Item = JobResult<T>>>>;
pub trait Stream {
    type Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
}
pub enum Derive<T> { Value(T), Future(LocalFuture<T>), Stream(LocalStream<T>) }

# pub struct Cx<'a>(PhantomData<&'a mut ()>);
# pub struct AheadCx<'a>(PhantomData<&'a mut ()>);
# pub struct EffectCx<'a>(PhantomData<&'a mut ()>);
# impl Cx<'_> {
#     pub fn get<T: 'static>(&mut self, _source: ReadRef<T>) -> Read<Rc<T>> { unimplemented!() }
# }
# impl AheadCx<'_> {
#     pub fn get<T: 'static>(&mut self, _source: ReadRef<T>) -> Read<Rc<T>> { unimplemented!() }
#     pub fn latest<T: 'static>(&mut self, _source: ReadRef<T>) -> Read<Rc<T>> { unimplemented!() }
#     pub fn is_pending<T: 'static>(&mut self, _source: ReadRef<T>) -> Read<bool> { unimplemented!() }
# }
# impl EffectCx<'_> {
#     pub fn set<T: 'static>(&mut self, _target: WriteRef<T>, _value: T) -> Result<(), HandleError> { unimplemented!() }
# }
pub enum RevealOrder { Sequential, Together, Natural }
pub enum FrameOp<P> {
    Attach { owner: Owner, parent: Option<Owner>, before: Option<Owner> },
    SetVisible(Owner, bool),
    Patch(Owner, P),
    Remove(Owner),
}
pub struct Frame<P> {
    pub sequence: u64,
    pub ops: Vec<FrameOp<P>>,
}
pub trait FrameSink<P> {
    fn publish(&mut self, frame: Frame<P>) -> Result<(), Failure>;
}
pub enum FlushError { Busy, Poisoned, Publish(Failure) }

pub trait Register<P>: Sized {
    fn owner(&self) -> Owner;
    fn signal<T: PartialEq + 'static>(&mut self, initial: T) -> (ReadRef<T>, WriteRef<T>);
    fn memo<T: PartialEq + 'static>(
        &mut self, compute: impl Fn(&mut Cx<'_>) -> Read<Derive<T>> + 'static,
    ) -> ReadRef<T>;
    fn derived_signal<T: PartialEq + 'static>(
        &mut self, compute: impl Fn(&mut Cx<'_>) -> Read<Derive<T>> + 'static,
    ) -> (ReadRef<T>, WriteRef<T>);
    fn ahead_memo<T: PartialEq + 'static>(
        &mut self, compute: impl Fn(&mut AheadCx<'_>) -> Read<Derive<T>> + 'static,
    ) -> ReadRef<T>;
    fn render<T: PartialEq + 'static>(
        &mut self, compute: impl Fn(&mut Cx<'_>) -> Read<T> + 'static,
        apply: impl Fn(Rc<T>) -> P + 'static,
    );
    fn render_ahead<T: PartialEq + 'static>(
        &mut self, compute: impl Fn(&mut AheadCx<'_>) -> Read<T> + 'static,
        apply: impl Fn(Rc<T>) -> P + 'static,
    );
    fn effect<T: PartialEq + 'static>(
        &mut self, compute: impl Fn(&mut Cx<'_>) -> Read<T> + 'static,
        apply: impl FnMut(&mut EffectCx<'_>, Rc<T>) -> JobResult<()> + 'static,
    );
    fn on_cleanup(&mut self, cleanup: impl FnOnce() + 'static);
    fn switch<K: PartialEq + 'static>(
        &mut self, key: impl Fn(&mut Cx<'_>) -> Read<K> + 'static,
        build: impl Fn(&mut Self, Rc<K>) + 'static,
    );
    fn loading(
        &mut self, on: impl Fn(&mut Cx<'_>) -> Read<()> + 'static,
        content: impl Fn(&mut Self) + 'static, fallback: impl Fn(&mut Self) + 'static,
    );
    fn errored(
        &mut self, content: impl Fn(&mut Self) + 'static,
        fallback: impl Fn(&mut Self, ReadRef<Failure>) + 'static,
    );
    fn reveal(
        &mut self, order: RevealOrder, collapse_tail: bool,
        build: impl FnOnce(&mut Self),
    );
}
pub trait Drive<P> {
    type Register: Register<P>;
    fn install(&mut self, build: impl FnOnce(&mut Self::Register)) -> Result<Owner, HandleError>;
    fn dispose(&mut self, owner: Owner) -> Result<(), HandleError>;
    fn set<T: 'static>(&mut self, target: WriteRef<T>, value: T) -> Result<(), HandleError>;
    fn update<T: 'static>(
        &mut self, target: WriteRef<T>, update: impl FnOnce(&T) -> T,
    ) -> Read<()>;
    fn committed<T: 'static>(&self, source: ReadRef<T>) -> Read<Rc<T>>;
    fn latest<T: 'static>(&self, source: ReadRef<T>) -> Read<Rc<T>>;
    fn is_pending<T: 'static>(&self, source: ReadRef<T>) -> Result<bool, HandleError>;
    fn refresh<T: 'static>(&mut self, source: ReadRef<T>) -> Result<(), HandleError>;
    fn affects<T: 'static>(
        &mut self, owner: Owner, source: ReadRef<T>,
    ) -> Result<Affect, HandleError>;
    fn end_affect(&mut self, token: Affect) -> Result<(), HandleError>;
    fn flush(&mut self, sink: &mut impl FrameSink<P>) -> Result<(), FlushError>;
}

fn install_panel(
    rt: &mut impl Drive<String>,
    load_title: impl Fn(Rc<u32>) -> LocalFuture<String> + 'static,
) -> Result<Owner, HandleError> {
    rt.install(move |root| {
        let (id, _) = root.signal(1_u32);
        let title = root.memo(move |cx| {
            let id = cx.get(id)?;
            Ok(Derive::Future(load_title(id)))
        });
        root.reveal(RevealOrder::Sequential, true, |group| {
            group.errored(
                move |error| error.loading(
                    move |cx| { cx.get(id)?; Ok(()) },
                    move |ready| ready.render(
                        move |cx| Ok(cx.get(title)?.to_string()),
                        |text| text.to_string(),
                    ),
                    |fallback| fallback.render(|_| Ok("Loading".to_owned()), |s| s.to_string()),
                ),
                |fallback, error| fallback.render(
                    move |cx| Ok(cx.get(error)?.to_string()),
                    |s| s.to_string(),
                ),
            );
        });
        root.render_ahead(
            move |cx| Ok((*cx.get(id)?, *cx.latest(id)?, cx.is_pending(id)?)),
            |state| format!("{state:?}"),
        );
    })
}
```

생성자는 `Runtime::new(wake_host: impl Fn() + Send + Sync + 'static) -> Result<Self, InitError>`로 둡니다. `install`은 루트 소유자에 등록한 작업을 큐에 넣고 소유자를 반환합니다. 동기 등록 중 같은 소유자 안에서 핸들을 전달할 수 있으며, 화면에 붙을 시점과 공개 읽기의 초기값은 첫 커밋이 결정합니다. `switch`는 키가 바뀌면 분기를 별도 후보 소유자에서 만듭니다. 같은 키를 다시 읽으면 기존 분기를 유지합니다. 앱은 `ReadRef`를 추적 없이 읽으려면 `committed`를 호출합니다.

## 저장소와 소유권

- 서로 다른 런타임의 핸들은 단조 증가하는 런타임 ID로 구별합니다. 전역 원자 카운터는 재사용 없이 ID를 발급하고 소진 시 생성에 실패합니다. 세대형 아레나는 `Vec<Slot>`과 빈 슬롯 인덱스 목록을 사용합니다. 제거 시 세대를 증가시키고 세대가 소진된 슬롯은 재사용 대상에서 뺍니다. 외부 핸들에는 런타임 ID·슬롯 인덱스·세대가 들어갑니다. 형식 매개변수는 불변이므로 하위 타입 관계가 있어도 다른 형식의 슬롯 핸들로 바꿀 수 없습니다.
- 노드 값과 실패는 아레나에서 형식을 지워 `Rc<dyn Any>` 및 `Rc<dyn Error>`로 저장합니다. 검증한 형식에만 안전한 `Rc::downcast`를 사용합니다. 슬롯에는 원본 형식, 종류, 소유자, 버전, 의존 간선이 있습니다. 등록 함수만 형식이 붙은 핸들을 만들 수 있습니다.
- 소유자마다 자식과 제거할 자원을 등록 순서대로 기록합니다. 트리를 폐기할 때 먼저 폐기 대상 전체를 죽은 것으로 표시하고, 간선과 대기 토큰을 제거하며 작업 세대를 무효화합니다. 이후 자식을 역순으로 재귀 폐기하고 각 소유자의 작업과 정리 함수를 역순으로 해제합니다. 콜백과 `Drop`은 저장소 대여를 끝낸 뒤 실행합니다.
- 분기를 교체할 때 기존 소유자는 교체 프레임이 성공할 때까지 살아 있습니다. 후보가 선점되면 후보만 폐기합니다. 기존 소유자는 후보가 생겼다는 이유로 정리하지 않습니다. 두 소유자 트리의 노드와 작업을 다른 세대로 유지하고, 숨김이나 교체 대기의 UI에 대해 적용을 중지하거나 기존 프레임 문맥으로 한정합니다. 이미 폐기한 슬롯의 큐 항목은 재검증에서 탈락합니다.

## 그래프 계산과 읽기

노드에는 커밋된 답 `committed: Option<Answer>`, 해당 노드의 주 프레임에 속하는 후보 `candidate: Option<Candidate>`를 둡니다. `Answer`는 값 또는 실패입니다. 후보에는 트랜잭션 ID와 입력 버전, 질문, 결과 또는 대기 토큰이 있습니다. 쓰기 큐는 별도입니다. 선행 레인은 자체 공개 스냅숏과 후보를 보관합니다. 둘을 주 프레임의 후보 슬롯에 덮어쓰면 재계산 시 서로의 질문을 취소하게 됩니다.

1. 쓰기나 답의 상태가 바뀌면 구독자 방향으로 더러운 노드를 표시합니다. 더러운 노드는 `BinaryHeap<Reverse<(height, enqueue_sequence, node_id, revision)>>`에서 낮은 높이부터 꺼냅니다. 키가 낡은 엔트리는 건너뜁니다. 초기 구현에서는 구독자가 없는 메모도 이 큐로 계산해 `latest`에서 지연 계산의 차이가 나지 않게 합니다.
2. 계산 패스에서 상위 노드를 읽을 때 그 문맥의 최신 버전까지 pull합니다. 실행 중인 문맥별 노드 스택에 같은 엔트리가 있으면 현재 후보를 순환 오류로 끝냅니다. 당기는 과정에서 높이는 현재 상위 높이의 최댓값에 1을 더한 값 이상으로 올립니다. 새로운 높이와 리비전으로 큐를 갱신합니다. 순서 정확성은 pull과 버전 검사에 의존하므로 높이는 같은 패스 안에서 낮출 필요가 없습니다.
3. 읽기 결과를 반환하기 전에 의존 간선을 기록합니다. `?`로 종료하면 실제로 읽었던 접두부와 대기 토큰을 후보에 기록합니다. 활성 프레임의 간선과 후보 간선을 따로 유지합니다. 후보가 사라지면 후보 간선만 치우고, 값이나 실패의 커밋과 함께 간선을 교체합니다. 값이 동등해 화면 패치를 생략하더라도 새 패스의 읽기와 앞으로의 무효화를 설치합니다. 화면에 적용한 패스의 간선은 화면이 교체될 때까지 따로 유지합니다.
4. 시그널과 메모는 `PartialEq`로 값의 동일성을 판정합니다. 같은 입력 및 문맥에서 동등한 값은 다시 알리지 않습니다. 상태 전이, 오류 회복 및 질문 교체는 값 비교와 별도로 알립니다. `update`는 동일 대상의 과거 제안, 현재 트랜잭션 후보, 커밋된 값 순으로 사용 가능한 값을 찾아 한 번의 새 제안을 만듭니다. 후보의 오류나 무응답도 이 순서에서 발견한 상태이며 오래된 정상값으로 덮어서 계산하지 않습니다.
5. 쓰기 가능한 파생 시그널에 직접 쓴 값은 그 질문의 제안입니다. 쓰기가 틱에 들어오면 같은 문맥의 작업을 취소해 사용자 값이 우선합니다. 다음 입력 변경에서 수동 값을 해제하고 계산을 다시 실행합니다. 같은 틱에 새 입력과 직접 쓰기가 있으면 직접 쓰기가 새 질문의 답입니다. `refresh`는 명시적으로 새 작업을 시작하므로 수동 답과의 경쟁을 큐의 마지막 명령 순서로 결정합니다.

읽기 실패의 출처와 상태 전이는 별도입니다. `Pending`은 해당 문맥에서 읽을 답이 없을 때만 발생하며, `Failed`는 그 문맥의 가장 최근 답이 오류일 때 발생합니다. `ReadError::Handle`은 오용이므로 Errored의 데이터 오류로 다루지 않습니다. 계산 바깥에서 핸들 만료나 다른 런타임은 즉시 돌려줍니다. 계산에서 빠져나간 핸들 오용은 후보를 중단하고 런타임을 오류 상태로 만듭니다.

Rust에서는 `match`로 읽기 오류를 받아 자기 대체값을 의도적으로 반환할 수 있습니다. 그 패스가 값을 반환하면 렌더 리더는 해당 읽기로 보류를 만들지 않지만, 그래프는 실패한 읽기를 구독하고 상태가 변할 때 패스를 다시 실행합니다. `?`로 전파했으면 반대로 경계 또는 스케줄러가 처리합니다. 서로 다른 작업의 오래된 Pending 토큰은 재사용할 수 없습니다. 이 규칙을 지키려면 Solid D-42의 상태 전달 최적화처럼 사용자 패스 자체를 건너뛰면 안 됩니다.

## 비동기 작업과 호스트 깨우기

- 계산 클로저가 먼저 시그널을 읽고 값을 복제한 뒤 `Derive::Future` 또는 `Derive::Stream`을 반환합니다. Future나 스트림의 `poll`에는 추적 문맥을 건네지 않습니다. 두 작업 모두 런타임에 소유되며 `Send`가 필요 없습니다. std 채널을 쓰는 호스트 어댑터라면 외부 스레드는 자체 I/O 결과와 깨우기만 담당하고 그래프는 소유 스레드에 남습니다.
- 작업 ID는 `(node_key, evaluation_domain, question_revision, flight_generation)`입니다. 처음 등록한 작업을 소유 스레드에서 한 번 폴링합니다. `Pending`이면 다음 깨우기까지 멈춥니다. Future 응답은 값과 오류 모두 같은 스테이징 경로를 거칩니다. 입력 선점, 새 작업, 폐기 시 먼저 세대를 무효화하고 살아 있는 박스를 꺼내서 즉시 `Drop`합니다. 따로 실행한 I/O를 중단하려면 어댑터가 취소 신호를 구현해야 합니다. 늦게 온 메시지와 깨우기는 세대가 맞을 때만 수용합니다.
- `Wake`를 구현하는 `Arc<TaskWake>`에는 작업 ID와 공유 수신함만 넣습니다. 수신함은 `Mutex` 안에 준비 큐, 중복 제거 집합, `posted`, `closed`를 함께 둡니다. 작업이 `wake`할 때 닫힌 수신함이면 반환합니다. 열린 수신함에서는 ID를 큐에 넣고 `posted`가 거짓이면 참으로 바꿉니다. 참으로 바꾼 호출이 락을 놓은 뒤 호스트 이벤트를 게시합니다. 입력 쓰기 및 새 작업에도 같은 호스트 깨우기 경로를 사용합니다.
- 호스트는 이벤트 수신 후 `flush`를 호출합니다. 틱 시작에 같은 락으로 큐·중복 제거 집합을 분리하고 `posted`를 거짓으로 바꿉니다. 이에 앞선 깨우기는 분리한 큐에 남고, 이후 깨우기는 새 이벤트를 게시합니다. 직접 호출한 `flush` 때문에 늦게 도착한 빈 이벤트도 허용합니다. 호스트 콜백은 연결이 살아 있는 동안 이벤트를 빠짐없이 게시하고, 콜백 안에서 재진입하지 않습니다. 런타임을 닫을 때 같은 락으로 닫힘을 표시합니다. 이미 락 밖으로 나간 호스트 게시가 늦게 도착할 수 있으므로 연결을 해제한 호스트는 이를 무시합니다.
- 스트림은 한 작업에서 틱당 최대 한 항목을 받아 순서대로 처리합니다. 스테이징한 항목이 해당 질문의 답으로 커밋되었거나 폐기될 때까지 그 스트림을 또 폴링하지 않습니다. 커밋 후 다시 큐에 넣어 다음 항목을 요청합니다. 첫 항목 뒤의 `Pending` 동안 마지막 답을 유지하고, 첫 항목 전 EOF는 `EmptyStream` 실패로 끝냅니다. 한 항목 이상을 낸 뒤 EOF이면 마지막 답을 유지합니다. 오류 항목은 그 스트림의 최종 답이며, 수락한 즉시 작업을 종료합니다.

## 틱과 보류

`flush`는 다른 틱에 재진입하면 `Busy`를 반환합니다. 소유 스레드는 입력과 준비 큐를 고정하고 다음 순서로 처리합니다.

1. 이번 틱에 들어온 쓰기를 대상과 큐 순서로 합성해 트랜잭션의 제안으로 승격합니다. 준비 큐의 각 작업은 폴링 직전에 해당 문맥에서 전이 의존성까지 pull하고, 현재 입력 버전과 작업 ID가 일치하는 경우에만 폴링합니다. 입력이 바뀐 작업은 먼저 무효화해 `Drop`한 뒤 새 질문을 평가합니다. 수락한 답만 스테이징합니다. 서로 엮이지 않은 과거 보류는 유지합니다. 틱 이후의 입력과 이펙트 적용에서 발생한 새 쓰기는 다음 틱으로 넘어갑니다.
2. 메모는 읽는 입력 버전과 후보의 출처를 기록합니다. 주 프레임 후보의 값을 읽은 패스는 그 트랜잭션에 속합니다. 트랜잭션 후보를 읽지 않은 신규 메모는 자신이 만들어졌다는 이유만으로 가입하지 않습니다. 후보값으로 만든 분기는 같은 트랜잭션에서만 붙을 수 있습니다.
3. 렌더 리더를 후보 프레임에서 검증합니다. 새 분기의 첫 리더도 포함합니다. 비동기 작업이 살아 있다는 사실만으로 보류하지 않습니다. 표시해야 하는 렌더 리더가 `Pending`을 전파했고 경계가 흡수하지 않았을 때만 `(reader, question, transaction)` 대기 항목을 만듭니다. 답이 없으면 기존 프레임을 유지합니다. 한 트랜잭션에서 리더가 후보를 검사하는 패스와 기존 화면을 갱신하는 패스를 구분합니다. 기존 화면 패스는 영향받지 않은 새 값과 기존 프레임에서 커밋된 입력을 사용합니다.
4. 보류 중인 입력을 기존 프레임에서 커밋된 버전으로 읽었다면 `(reader, transaction, committed_version)`을 기록합니다. 보류가 풀릴 때 해당 리더를 다시 계산해서 이전 값으로 만든 패치를 그대로 적용하지 않습니다. 같은 패스가 후보값을 읽어 가입한 경우에는 후보 의존성을 별도로 기록합니다. 두 기록은 서로를 대체하지 않습니다.
5. 같은 슬롯에 채택된 쓰기 제안이 있거나 같은 표시 리더의 현재 질문을 함께 결정하는 트랜잭션은 하나로 합칩니다. 보류된 쓰기가 없는 슬롯에서 틱 내의 최종 쓰기가 커밋값과 같으면 제안을 만들거나 작업을 재시작하지 않습니다. 보류된 쓰기에 다시 쓰는 틱은 기존 제안과 값이 같아도 그 트랜잭션에 가입합니다. 새 틱의 후보 값이 이전 후보를 선점하면 마지막 쓰기를 사용합니다. 병합하면 대표 ID에 후보·리더·작업을 다시 연결하고, 입력 버전이 달라진 질문만 폐기해 재계산합니다. 공통 소유자만으로 합치지 않습니다. 의존 관계가 없는 틱은 기존 보류 중에도 커밋할 수 있습니다.
6. 리더가 재계산에서 해당 질문을 읽지 않거나, 폐기되거나, 커밋으로 숨겨지면 참가를 해제합니다. 동일한 후보 프레임에서 교체가 결정된 리더는 그 교체의 보류를 만들지 않습니다. 교체가 아직 보류 중인 경우 다른 트랜잭션에서는 기존 리더가 여전히 표시 중입니다. Loading이 잡는 대기는 경계에 귀속하고, 같은 질문을 읽는 외부 리더의 대기는 유지합니다.
7. 대기가 없는 단위는 질문·의존 버전과 경계 상태를 마지막으로 검증한 뒤 공개합니다. 질문이 바뀌면 이전 답을 버리고 새 답을 기다립니다. 이 과정에서 생긴 합침, 대기 해제, 경계 변경이 안정되면 더 이상 보류를 추가하지 않는 클로저만 패치로 변환합니다. 같은 스케줄러 단위에 속한 패치와 그래프 커밋은 한 프레임입니다.

프레임이 같은 값으로 마감돼도 응답 상태, 대기 참조와 소유권 전이는 처리합니다. 예를 들어 보류 중인 실패는 기다림을 끝내지만 같은 트랜잭션의 다른 리더가 기다리고 있다면 실패 자체도 공개할 수 없습니다. 독립 트랜잭션은 조정 시점의 시작 순서로 검사하며, 준비되지 않은 항목이 준비된 항목의 커밋을 막지 않습니다.

## 선행 읽기와 다시 요청하기

선행 계산은 `ahead_memo`나 `render_ahead`로 등록합니다. 이 명세는 Solid의 실행 중 모드 추론을 명시적 등록으로 옮깁니다. 일반 `Cx::get`은 해당 프레임의 문맥을 따릅니다. 선행 계산은 공개 단위에 따라 다음 두 문맥을 사용합니다. 한 번의 패스가 읽는 스냅숏을 고정해 호출 순서와 관계없이 같은 결과를 반환합니다.

| 선행 클로저의 실행 문맥 | `AheadCx::get` | `AheadCx::latest`, `AheadCx::is_pending` |
| --- | --- | --- |
| 선행 레인 계산 | 일반 노드는 커밋 스냅숏, 같은 레인의 선행 파생 노드는 레인 후보의 값·실패·대기 | 이번 공개 틱까지 승격된 답과 질문 상태 |
| 주 프레임의 공개 검증 | 마지막 커밋 위에 해당 프레임의 후보만 얹은 스냅숏 | 같은 투영을 기준으로 이번 프레임에 공개할 답을 반영한 상태 |

두 실행 문맥은 입력 버전, 읽기 간선과 구독을 따로 기록합니다. 주 프레임이 방금 만든 노드에 의존하는 첫 렌더는 주 프레임 문맥으로 계산합니다. 이 공개 검증에서 같은 프레임이 첫 답을 제공할 수 있다면 `is_pending`은 공개 직후의 상태인 거짓을 반환합니다. 별도 선행 레인에서 그 첫 커밋을 기다리는 패스는 주 프레임의 대기 항목에 넣지 않습니다.

- `latest`는 현재 질문의 답 중 승인된 후보가 있으면 값이나 실패를 반환하고, 답이 아직 없으면 그 노드의 마지막 답을 반환합니다. 값이 한 번도 없고 오류 답도 없으면 `Pending`입니다. 쓰기 큐와 선점된 작업은 어느 쪽의 답에도 포함되지 않습니다.
- `is_pending`은 먼저 첫 공개 여부를 확인합니다. 커밋된 답이 없으면 추적 호출은 커밋까지 `Pending`이고, 추적 없는 호출은 거짓입니다. 최종 실패도 커밋된 답에 해당합니다. 이후 살아 있는 `affects` 선언이 하나라도 있거나, 새 질문에 답이 없거나, 새 질문의 변경된 답이 도착했지만 아직 보류 중이면 참입니다. 새 답이 커밋되면 그 질문의 미완료 판정은 끝납니다. `affects` 선언은 개별 해제까지 유지합니다.
- `refresh`는 대상 계산을 같은 질문으로 다시 실행하도록 큐에 넣습니다. 입력이 그대로이며 이전 답을 표시 중이면 작업이 진행 중일 때와 답을 스테이징했을 때 모두 보류 판정을 바꾸지 않습니다. 독립적인 새 질문이나 `affects` 선언이 있으면 그 조건을 적용합니다. 원시 시그널의 `refresh`는 수행할 계산이 없으므로 성공만 반환합니다. `affects`는 특정 노드가 현재 값과 다른 답을 준비할 것임을 선언합니다. 소유자와 토큰으로 등록 개수를 관리하고, 명시적 완료·취소 또는 소유자 폐기 때 하나씩 해제합니다. 이미 종료한 같은 런타임의 토큰은 세대가 달라졌어도 성공으로 응답하며, 새 토큰의 개수에 손대지 않습니다. 등록·해제는 다음 틱에서 보입니다.
- 선행 레인은 `latest`를 공유하거나 같은 선행 화면 리더에서 합쳐지는 파생 출력을 함께 공개합니다. 그 레인이 스스로 시작한 비동기 결과가 대기 중이면 레인의 이전 표시를 유지합니다. 단순 상태 표시가 상위 보류를 연장하지 않습니다. 주 프레임 자체가 준비되려면 그 변경에서 파생된 표시가 모두 준비되어야 하므로, 여전히 화면이 소비하는 선행 파생의 대기는 상위 프레임의 마지막 검증에서도 검사합니다. 화면에서 빠지거나 Loading이 흡수하면 그 제약을 해제합니다.
- 선행 레인의 응답이 먼저 오면 해당 레인만 공개합니다. 주 프레임에서는 별도로 자신의 후보 입력으로 계산합니다. 같은 노드가 두 문맥에서 요청되면 두 질문의 ID와 작업을 독립적으로 갖고 서로의 답으로 대체하지 않습니다. 주 프레임이 자기 질문에 필요한 답을 모두 확보한 경우, 교체할 화면의 선점된 선행 응답을 더 기다리지 않습니다. 선행 상태만 보고 시작한 새 분기는 보통 프레임 문맥으로 등록합니다. 그 분기가 보류한 값을 실제로 읽으면 해당 트랜잭션에 가입합니다.

## 적용과 경계

### 계산과 적용

렌더 이펙트의 계산은 읽기와 출력 값 생성만 수행합니다. 비교를 통과한 결과는 `apply`에서 `P`로 변환합니다. 렌더 `apply`는 외부 상태를 변경하지 않는 패치 생성 함수입니다. 런타임은 붙이기, 표시·숨기기, 패치, 제거의 순서를 정해 프레임 하나로 `FrameSink::publish`에 건넵니다. 제거는 자식부터, 붙이기는 부모부터 하며 처음 붙는 노드의 초기 패치는 최초 가시화 전에 처리합니다. `Attach`는 소유자와 그 부모, 같은 부모 아래에서 삽입할 위치의 다음 형제를 전달합니다. 다음 형제가 없으면 맨 뒤에 삽입합니다. 런타임은 형제가 이미 존재하거나 먼저 붙도록 패치 순서를 정합니다.

호스트는 한 런타임의 모든 `flush`에 같은 그래프와 연결된 패치 수신기를 전달합니다. 수신기의 표시 상태가 소유자 ID와 패치 이력을 보관합니다. 표시 대상 전체를 교체할 때는 새 런타임에서 트리를 처음부터 등록합니다. 호스트는 다음 트리나 커맨드 목록을 먼저 준비하고 한 번에 노출하며, `publish` 동안 입력 이벤트나 런타임 관측 콜백을 실행하지 않습니다. `publish`가 오류를 돌려주면 호스트는 이전 화면을 유지하고 런타임도 후보의 값·간선·소유권을 커밋하지 않습니다. 실패한 런타임에서 이후 호출은 `Poisoned`로 끝납니다. `dispose`와 런타임 소멸 시의 정리는 곧바로 실행하며 패치를 발행하지 않습니다. 마지막 화면을 제거할 책임은 호스트에 있습니다.

런타임은 `publish` 호출 전에 그래프 커밋에 필요한 검사와 할당을 마칩니다. 공개에 성공하면 저장해 둔 상태를 교체해 커밋을 끝낸 뒤, 이전 값의 `Drop`과 소유자 정리, 사용자 이펙트의 `apply`를 실행합니다. 퇴장하는 숨은 하위 트리와 폐기된 후보의 사용자 이펙트는 실행하지 않습니다.

일반 사용자 이펙트는 렌더 리더로 집계하지 않습니다. 보류 후보에서 계산한 값은 그 프레임이 커밋될 때 적용합니다. 적용은 `EffectCx`로 다음 틱 쓰기를 제안할 수 있습니다. 사용자 이펙트의 계산 오류와 적용 오류는 각각 처리합니다. 계산에서 전파된 오류는 진단 후 해당 실행을 생략합니다. 적용 실패는 이미 공개한 프레임을 되돌릴 수 없으므로 다음 틱에서 가까운 Errored에 전달합니다. 처리할 경계가 없는 렌더 오류 및 적용 오류는 런타임을 오류 상태로 전환합니다. Rust panic은 앱 데이터 오류 표현에 사용하지 않습니다. `panic=unwind`에서는 호스트로 다시 전파하면서 런타임을 오류 상태로 표시하고, 다음 호출에서 기존 후보를 재실행하지 않습니다.

### Loading과 Errored

- Loading은 콘텐츠와 대체 화면에 별도 자식 소유자를 둡니다. 콘텐츠를 한 번도 커밋하지 않은 경계는 대기 상태를 받을 수 있습니다. 가장 가까운 수용 가능한 경계 아래에서 해당 렌더 리더의 대기는 경계에 귀속됩니다. 경계는 콘텐츠를 전부 붙일 수 있을 때 공개하거나, 콘텐츠 대기가 있다면 경계를 활성화한 프레임에 대체 화면을 공개합니다. 경계 자체가 보류한 프레임에 속했다면 대체 화면도 그 프레임을 따릅니다.
- 콘텐츠를 한 번 공개한 Loading은 다음 요청 동안 이전 콘텐츠를 유지하며 Pending을 바깥으로 전달합니다. `on` 패스가 읽은 의존성이 변경되면 다시 수용 상태로 돌아가며, 이때 대체 화면 전환은 `on`을 유발한 프레임에 속합니다. `on`은 반환값 비교 없이 추적한 읽기의 버전으로 판단합니다. `on`이 먼저 Pending이면 이전 경계 상태를 유지하고 상위에서 기다립니다.
- Errored는 가장 가까운 하위 계산에서 전파한 `Failed`를 수용합니다. Pending은 Loading이나 트랜잭션이 처리합니다. 대체 화면이 이미 열려 있다면 커밋될 오류를 대체 화면이 구독한 실패 시그널에 담아 그 화면의 상태를 유지합니다. 정상 콘텐츠가 커밋될 때 대체 화면을 폐기합니다. 재요청이 Pending이면 기존 대체 화면을 유지합니다. 마지막 값과 같은 값으로 회복해도 실패에서 값으로 상태가 전환되므로 콘텐츠를 복구합니다. 가려진 콘텐츠를 복원하면 그 하위 Loading이 다시 수용 상태에서 시작합니다. 대체 화면 자체의 읽기는 해당 경계의 바깥에서 처리합니다.
- 두 경계 모두 가려진 콘텐츠의 상태와 비동기 작업을 유지합니다. 가려진 하위의 변경은 준비 상태를 계산할 수 있지만 현재 화면의 보류 참가자에서는 제외합니다. 새 화면으로 공개될 때만 준비된 렌더 패치를 적용합니다. 가려진 상태에 예약한 사용자 이펙트는 최신 커밋 가능한 결과 하나만 남기고 표시 후 재검증합니다.

### Reveal

Reveal은 자기가 직접 거느리는 Loading과 중첩 Reveal을 등록 순서대로 제어합니다. 일반 소유자와 Errored는 슬롯이 아니며 별도 Reveal 조상을 만들지 않습니다. `Sequential`은 준비한 선두 슬롯부터 열고 첫 미준비 슬롯을 경계로 삼습니다. 그 뒤는 대체 화면을 표시하거나 `collapse_tail`일 때 숨깁니다. 경계가 중첩 그룹이면 그 그룹 자체의 공개 규칙을 적용합니다. `Natural`은 준비한 슬롯을 독립적으로 엽니다.

`Together`는 각 슬롯의 최소 준비 상태를 검사한 뒤 그룹의 게이트를 한 프레임에서 엽니다. 일반 슬롯은 콘텐츠 준비가 최소 조건입니다. 중첩 그룹은 `Sequential`의 첫 슬롯, `Natural`의 슬롯 하나, `Together`의 모든 슬롯이 최소 조건을 충족해야 합니다. 빈 그룹은 준비 상태입니다. 열린 중첩 그룹은 자기 규칙을 계속 적용합니다. 슬롯이 콘텐츠를 커밋하면 부모의 제어에서 벗어납니다. 중첩 그룹은 열린 뒤 자기 슬롯이 준비되었을 때 제어에서 벗어납니다. 이후 자신의 Loading을 다시 활성화해도 이미 공개한 형제나 새로 등록할 그룹의 순서를 되돌리지 않습니다. 폐기한 슬롯은 준비 검사에서 즉시 제거합니다.

## 검증 계획

테스트에서 채널과 수동 Future를 연결해 완료·깨우기·폐기 순서를 직접 제어합니다. 각 틱에서 `FrameSink`에 들어온 전체 표시 스냅숏, 값과 상태, 폴링 횟수와 정리 로그를 비교합니다. 타이머 대기와 실제 창은 필요 없습니다.

| Solid 규칙 | 첫 구현의 관찰 가능한 테스트 |
| --- | --- |
| F-1 | 다이아몬드 그래프에 두 입력을 같은 틱에서 쓰고 어느 패치에도 옛 파생값과 새 원본이 섞이지 않는지 검사합니다. |
| F-2 | A 화면에서 B, C로 연속 전환합니다. B가 늦게 완료되어도 A를 유지하고, C 작업 완료 후 C 입력과 렌더가 한 프레임에 나옵니다. 새 분기의 첫 비동기 읽기도 같은 판정을 적용합니다. |
| F-3 | 같은 Pending을 읽던 가시 리더 두 개 중 하나를 제거한 동안 보류하고, 나머지가 더 이상 읽지 않으면 보류를 해제합니다. 후보의 제거가 보류된 리더는 다른 프레임에서 계속 참가합니다. |
| F-4 | 준비되지 않은 초기 Loading과 독립적인 시그널을 같은 틱에서 갱신해 대체 화면과 독립 변경이 그 프레임에 나오는지 검사합니다. 같은 Pending을 경계 밖에서도 읽는 변형에서 외부 보류가 유지됩니다. |
| F-5 | 첫 콘텐츠 공개 후 재요청하면 이전 콘텐츠를 보입니다. `on` 의존성 변경이 공개된 프레임에서만 대체 화면으로 돌아갑니다. |
| F-6 | 순수 `latest` 표시를 먼저 공개합니다. 그 표시를 비동기로 파생한 값도 같은 레인에서 읽으면 둘 다 그 레인의 답이 준비될 때 함께 공개합니다. 부모와 선행 레인의 서로 다른 질문에 각기 다른 답을 보내고 어느 프레임에도 다른 쪽 답이 혼입되지 않는지 검사합니다. 예제에 첫 폴링이 `Pending`인 `load_title`을 전달하면 `id`의 이전 커밋 없이도 Loading 대체 화면과 선행 표시가 첫 프레임에 함께 나옵니다. |
| F-7 | 보류 입력으로 만든 메모와 분기의 부착 패치, 같은 프레임의 재활성화 패치가 보류 중에는 없고 커밋에 함께 있는지 검사합니다. |
| F-8 | Solid가 철회한 번호입니다. 독립 요구사항을 배정하지 않습니다. |
| F-9 | 첫 요청의 추적 없는 판정은 거짓이고 추적 읽기는 Pending입니다. 조용한 갱신의 답을 별도 보류에 스테이징해도 판정은 거짓입니다. 새 질문의 변경된 답은 보류 중에 참입니다. 중복 등록한 `affects`는 답을 커밋해도 유지되며, 개별 해제 후 마지막 선언이 끝날 때 거짓입니다. 선행 패스는 일반 값과 판정을 어느 순서로 읽어도 같은 쌍을 받습니다. |
| F-10 | 쓰기 두 건을 큐에 넣고 `flush` 이전의 일반 읽기, `latest`, `is_pending` 및 UI에 변화가 없는지 검사합니다. `update`는 앞서 넣은 제안과 합성합니다. 두 쓰기가 커밋값으로 돌아오면 새 요청이 생기지 않으며, 보류 중인 값을 다시 쓰는 틱은 기존 보류에 가입합니다. |
| F-11 | 낙관적 쓰기 단계에서 추정·권위 답 불일치, 동일 답, 늦게 완료한 이전 질문, 되돌리기와 원래 쓰기의 동시 공개를 검사합니다. |
| F-12 | Pending은 Errored를 열지 않습니다. 보류된 실패는 답으로 계산하지만 다른 대기가 끝날 때 함께 공개합니다. 이전 값과 같은 값의 복구도 표시합니다. Rust의 직접 `match`와 `?` 분기의 구독 및 경계 동작을 각각 검사합니다. |

추가 회귀 사례로 동적 의존성 제거와 스테이징·커밋 간선 버전, 순환 계산, 다른 런타임 핸들, 슬롯 재사용 전후의 깨우기, 폴링 중 자체 깨우기, 수신함 분리 직전·직후의 교차 스레드 깨우기, 중첩 소유자의 역순 정리, 스트림 EOF와 늦은 항목, 보류 중 후보 폐기, Reveal의 중첩 최소 준비 및 공개 슬롯 재활성화, 호스트 공개 실패를 고정합니다. 이전 작업의 깨우기와 전이 입력을 바꾸는 쓰기가 같은 틱에 들어오면 이전 작업의 추가 폴링 횟수는 0회여야 합니다.

| 순서 | 구현 단위와 완료 조건 |
| --- | --- |
| 1 | 아레나·소유권·동기 그래프를 헤드리스 호스트에 연결합니다. 동기 다이아몬드, 동적 의존성, 슬롯 세대, 역순 정리와 틱 전 쓰기 비가시성이 통과합니다. |
| 2 | 로컬 폴링과 세대 필터, 입력 질문을 도입합니다. 교차 스레드 깨우기, 선점, Future·스트림 종료·실패, 빈 호스트 구동이 통과합니다. |
| 3 | 후보 슬롯과 주 프레임 보류, 그래프·패치 공개를 묶습니다. F-1~F-3·F-7·F-10과 실패한 호스트 공개를 헤드리스 프레임 기록에서 확인합니다. |
| 4 | 경계와 Reveal, 상태 판정 및 선행 레인을 연결합니다. 적용한 Rust 의미론을 포함해 F-4~F-6·F-9·F-12, Reveal 중첩과 오류 복구를 확인합니다. |
| 다음 단계 | 작업 수명과 질문 버전 규칙을 재사용해 낙관적 갱신, `action`, 스토어·프로젝션을 붙입니다. F-11의 추정·권위·롤백 기록과 첫 구현의 행을 함께 실행합니다. |

지금 검증할 수 있는 범위는 이 문서의 Rust 타입과 호출 예제입니다. 구현 후에는 각 행을 실제 런타임의 테스트로 실행합니다.

```powershell
rustdoc +stable --edition=2021 --test docs/reactive-runtime-spec.md
```

## 근거

Solid.js는 로컬 체크아웃 `fcaa05104574e2700b1b8b667c0734c428812c7b`의 `@solidjs/signals@2.0.3`을 기준으로 삼았습니다. 비교 기준은 `packages/signals/docs/RULES-FUNDAMENTALS.md`, `RULES-DERIVED.md`의 D-3·D-9·D-10·D-22~D-25·D-38·D-41·D-42, `SCENARIOS.md`, `packages/signals/src/reveal.ts`입니다. 현 소스에는 후보값 읽기의 `REACTIVE_STAGED_READ`와 기존 프레임 읽기의 `REACTIVE_FRAME_READ`가 둘 다 있습니다. 위 명세는 둘을 별도 기록으로 표현합니다. [프로젝트 리서치 보고서](../research/2026-11-23-solid-2-async-signals/report.md)
