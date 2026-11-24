# Connecting Fine-Grained Reactive Signals to a Retained UI Tree, Events, Focus, and Widgets in Rust

## What I searched

I looked for the boundary between a signal subscription and a native node mutation, rules for retaining child identity, and the runtime work that follows a patch or input event. I checked published releases separately from repository `main` and project-hosted documentation.

| Query | What it returned |
|---|---|
| `Floem latest docs crates virtual_stack dyn_stack style transition event phase focus` | Floem’s `VirtualStack` documentation. |
| `Masonry Xilem docs.rs latest widget arena pointer capture focus request layout animation` | Masonry pass, widget, and invalidation docs, plus Xilem’s crate docs. |
| `GPUI crates latest Zed Element DispatchTree mouse hitbox focus list animation` | Zed’s dispatch tree source and list implementation leads. |
| `Solid 2 universal renderer createRenderer For Index beta xilem leptos tachys dioxus blitz` | An older npm README for Solid Universal alongside Xilem and Blitz leads. |
| `Floem GitHub view_storage dyn_stack Key Scope create_effect style transition floem latest releases` | Floem version and implementation leads, with stronger matches in later searches. |
| `GPUI 0.2.2 docs.rs Element immediate mode rendered every frame FocusHandle tab index animation` | Released GPUI window source and tab-stop and input examples. |
| `Leptos tachys Renderer Render rebuild reactive signal For keyed source docs.rs` | Tachys `Render` implementations and Leptos’s keyed-list documentation. |
| `Dioxus native Blitz dioxus-native MutationWriter real_dom hit testing Stylo source GitHub` | First-party Blitz mutation-writer source and secondary architecture summaries. |
| `site:github.com/solidjs/solid Solid 2 For keyed by index universal renderer 2.0.3` | Solid release listings, a 2.0 RC announcement, and the older 1.x renderer README. |
| `site:docs.rs/floem/0.2.0 dyn_stack dynamic dyn_view ViewId view_storage Style Taffy` | Floem `ViewId`, `ViewStorage`, and `dyn_stack` documentation and source. |
| `site:github.com/lapce/floem event routing capture z_index view tree style dyn_stack` | No usable first-party routing match in that batch. |
| `site:linebender.org masonry vision xilem Raph Levien retained reactive tree` | No usable architecture match in that batch. |
| `Floem source dyn_stack apply_diff Scope dyn_container label ViewId style request_layout 0.2.0` | Released list diff and style-pass source. |
| `site:docs.rs/masonry_core hit_test clip inverse transform children reverse find_widget_at request_pointer_capture` | Masonry clip and capture context leads. |
| `site:docs.rs/gpui Element lifecycle recreated list AnimationExt DispatchPhase` | Released GPUI dispatch and list source, plus animation results from a fork. |
| `site:docs.rs/tachys renderer DomRenderer reactive_function RenderEffect keyed either` | Tachys renderer module and reactive and keyed `Render` implementations. |
| `Solid 2 For keyed repeat show control flow v2.solidjs.com keyed false` | Official `Show` docs and a mirror of the control-flow RFC. I fetched the RFC from the release commit. |
| `lapce floem box tree event phase pointer capture z index transform hit testing event routing` | Project-hosted Floem docs for the box tree and event phases. |
| `Masonry find_widget_at_pos paint order xilem tree_arena WidgetArena source` | No useful exact source match. I fetched the released `Widget` trait directly. |
| `Raph Levien Xilem architecture reactive UI Rust Masonry vision raphlinus linebender` | Xilem’s current README, Linebender’s project overview, and talk leads. I fetched Levien’s architecture article directly. |

Pages opened or inspected include the Floem, Masonry, Xilem, GPUI, Tachys, Dioxus, and `blitz-dom` package pages. I also opened the [Floem repository](https://github.com/lapce/floem), [Xilem releases](https://github.com/linebender/xilem/releases), and the implementation and specification pages cited below. The npm registry endpoints and old Solid control-flow URLs failed to load. The older npm README displayed `2.0.0-rc.0`. Solid’s repository listed `2.0.3` releases for both `solid-js` and `@solidjs/universal`, and I could fetch the renderer from their published commit.

## 1. Bridging signals to retained nodes

### Takeaway

Floem shows the create-once contract directly. Solid Universal supplies a compact host mutation interface and still reconciles changing arrays of host nodes. Xilem and Dioxus show how a retained tree can receive localized mutations from a diffing front end, while GPUI retains entity and element state across rendering work.

### Cited findings

- Published versions visible on November 24 were Floem `0.2.0` (2024-11-14), Masonry and Xilem `0.4.0` (2025-10-29), and Zed’s GPUI `0.2.2` (2025-10-22). Solid’s releases list `solid-js` and `@solidjs/universal` `2.0.3` on November 19, 2026, at commit `68406c2`. [Floem](https://docs.rs/crate/floem/latest), [Masonry](https://docs.rs/crate/masonry/latest), [Xilem](https://docs.rs/crate/xilem/latest), [GPUI](https://docs.rs/crate/gpui/latest), [Solid releases](https://github.com/solidjs/solid/releases), accessed 2026-11-24, Direct.
- Floem’s `ViewStorage` is thread-local. A `SlotMap` allocates `ViewId`s, secondary maps store widget objects and state, and `Vec<ViewId>` records sibling order alongside parent links. The derived label creates its ID and initial text once. Its updater sends later text values to that same ID. [View storage](https://docs.rs/crate/floem/latest/source/src/view_storage.rs), [Label source](https://docs.rs/crate/floem/latest/source/src/views/label.rs), `0.2.0`, accessed 2026-11-24, Direct.
- Floem `dyn_stack` tracks the item iterator and key function, emits insert, move, and remove operations, and gives each constructed row a child scope. Removal deletes the view and disposes its scope. `dyn_container` replaces a single child and disposes the previous scope, with code to wait for configured exit animations. Floem also has a retained `Display::None` state exposed by `ViewId`. [Dynamic stack](https://docs.rs/crate/floem/latest/source/src/views/dyn_stack.rs), [Dynamic container](https://docs.rs/crate/floem/latest/source/src/views/dyn_container.rs), [ViewId](https://docs.rs/floem/0.2.0/floem/struct.ViewId.html), `0.2.0`, accessed 2026-11-24, Direct.
- Solid Universal’s `RendererOptions<NodeType>` provides creation, text replacement, property assignment, anchored insertion and removal, and parent/first-child/next-sibling access. Reactive inserts and spreads run through render effects. `reconcileArrays` moves, inserts, and removes existing host nodes in a dynamic child range. The Universal package is a separate package in Solid 2. [Renderer source](https://github.com/solidjs/solid/blob/68406c2/packages/universal/src/universal.ts), [Universal README](https://github.com/solidjs/solid/blob/68406c2/packages/universal/README.md), `2.0.3`, accessed 2026-11-24, Direct.
- Solid 2’s `<For>` uses item identity by default and exposes a reactive index. `keyed={false}` retains by position and exposes the item as an accessor, replacing the old `<Index>` pattern. A key function allows both the item and its index to update through accessors while the row survives. Default `<Show>` retains its child across truthy values. Keyed `<Show>` remounts on identity change. [For](https://docs.solidjs.com/reference/solid-js/components-jsx/for/), [Show](https://docs.solidjs.com/reference/solid-js/components-jsx/show/), [Solid 2 migration guide](https://github.com/solidjs/solid/blob/68406c2/documentation/solid-2.0/MIGRATION.md), Solid 2 and guide at `2.0.3`, accessed 2026-11-24, Direct.
- Xilem’s `View` exposes `build`, `rebuild(prev, ..., element, ...)`, `teardown`, and `message`. Its documentation says the view tree “is retained only long enough to dispatch messages and then serve as a reference for diffing for the next view tree.” Masonry owns the retained widget tree, with child ordering from `children_ids()` and external mutation through `RenderRoot::edit_root_widget`. The architecture file identifies its `tree_arena` dependency. [Xilem `View`](https://docs.rs/xilem_core/0.4.0/xilem_core/trait.View.html), [Masonry passes](https://docs.rs/masonry/0.4.0/masonry/doc/internals_01_pass_system/index.html), [Masonry architecture](https://github.com/linebender/xilem/blob/main/masonry/ARCHITECTURE.md), `0.4.0` and undated `main` architecture file, accessed 2026-11-24, Direct.
- Tachys builds and rebuilds retained `Render::State`. Reactive functions use `RenderEffectState`, and keyed iteration is exposed separately. The `Rndr` alias resolves to `Dom`. Its docs say the earlier renderer-generic view types were removed before Leptos 0.7.0 after compile-time and linker problems on larger applications. This limits what the existing abstraction offers a native backend. [Render](https://docs.rs/leptos/latest/leptos/tachys/view/trait.Render.html), [Rndr](https://docs.rs/tachys/latest/tachys/renderer/type.Rndr.html), Leptos `0.8.23` and Tachys `0.2.19`, accessed 2026-11-24, Direct.
- Dioxus Native has a mutation boundary between its `VirtualDom` and Blitz’s document. The writer takes Dioxus element IDs and applies attribute and tree operations through Blitz’s document mutator. Blitz separates DOM, style, layout and event handling from painting and the window shell. Its current README lists Stylo, Taffy and Parley for style, box layout and text layout. [Dioxus VirtualDom](https://docs.rs/dioxus-core/latest/dioxus_core/struct.VirtualDom.html), [Blitz mutation writer](https://github.com/DioxusLabs/blitz/blob/main/packages/dioxus-native-dom/src/mutation_writer.rs), [Blitz package architecture](https://github.com/DioxusLabs/blitz#readme), Dioxus `0.7.10` and undated Blitz `main`, accessed 2026-11-24, Direct.
- GPUI describes itself as hybrid immediate and retained mode. An entity owns application state and a `Render` implementation produces elements. `Element` goes through layout request, prepaint, and paint. An optional ID contributes to a global ID used to recover element-local state across frames. [GPUI crate docs](https://docs.rs/gpui/0.2.2/gpui/index.html), [Element](https://docs.rs/gpui/0.2.2/gpui/trait.Element.html), `0.2.2`, accessed 2026-11-24, Direct.

### Inferences

- A native backend could keep an ordered retained tree and scope lifetime separate from each property binding. Floem’s view-ID updater and Solid’s host operations suggest that a binding can patch a property directly while a structural binding performs local reconciliation. Both implementations still need an operation that moves an existing child while preserving its identity.
- Floem’s row builder receives `T` at creation. Solid’s custom-key mode shows why a row keyed by a database ID may also need a separately reactive value. Otherwise, a same-key replacement can preserve the native row while leaving its display bound to the old value.

### Gaps

- I did not establish Floem’s documented behavior for duplicate keys or test same-key replacement values. The checked signature and list-diff source establish the distinction between creation and movement.

## 2. Hit-testing, events, focus, scrolling, and text input

### Takeaway

The examples separate hit-testing from event dispatch. Geometry determines which visual target was hit. Tree relationships, pointer capture and keyboard focus then determine who receives the event. Scrolling and text input add state that outlives an individual input event.

### Cited findings

- Masonry chooses the last overlapping eligible child in `children_ids()` order. Its hit-test docs say, “No widget is returned if `pos` is outside the widget’s clip path.” Its compose pass assigns transforms. Pointer events go to the hit widget or pointer-capture owner and bubble to ancestors. Focusable widgets can be reached with Tab. `accepts_text_input` is a separate opt-in that starts an IME session on focus. [Widget trait](https://docs.rs/masonry_core/0.4.0/masonry_core/core/trait.Widget.html), [Masonry passes](https://docs.rs/masonry/0.4.0/masonry/doc/internals_01_pass_system/index.html), `0.4.0`, accessed 2026-11-24, Direct.
- Floem’s project-hosted docs describe a box tree in which several visual rectangles can map to one `ViewId`, such as the content and scrollbar rectangles of a scroll view. Hit-testing uses visual IDs and painting follows the box tree’s z-order. Event capture runs down the dispatch path, with target and bubble handlers on the return path. The project-hosted source also includes pointer-ID capture/release messages. These pages display `0.2.0` but describe APIs newer than the cited crates.io `0.2.0` sources. [ElementId](https://lapce.dev/floem/floem/struct.ElementId.html), [View event contract](https://lapce.dev/floem/floem/view/trait.View.html), [Capture message source](https://lapce.dev/floem/src/floem/message.rs.html), project-hosted snapshot, accessed 2026-11-24, Direct for that snapshot.
- GPUI tests hitbox bounds intersected with their content masks. Mouse capture listeners run back-to-front and bubble listeners front-to-back. Keyboard capture runs from root toward the focused element, and keyboard bubble reverses that path. `FocusHandle` carries `tab_index` and `tab_stop`. `focus_next` and `focus_prev` consult the last rendered frame’s tab stops. [Window source](https://docs.rs/crate/gpui/0.2.2/source/src/window.rs), `0.2.2`, accessed 2026-11-24, Direct.
- Pointer Events Level 3 defines capture per pointer ID, with the capture target taking the place of normal hit-testing until release. CSS 2.2 defines painting order within stacking contexts. Together these give precise references for two distinct interaction decisions. [Pointer Events Level 3](https://www.w3.org/TR/pointerevents3/#pointer-capture), W3C Recommendation 2026-06-30, [CSS 2.2 stacking contexts](https://www.w3.org/TR/CSS22/zindex.html), accessed 2026-11-24, Direct for the web specifications.
- Floem `VirtualStack` lazily loads rows. GPUI’s variable-height list keeps size and scroll state on the caller’s view and measures an overdraw region. Its source documents a contract for off-screen height changes. The caller uses `splice` or `reset` to invalidate cached measurements. GPUI also provides `UniformList` for equal-height rows. [Floem `VirtualStack`](https://docs.rs/floem/0.2.0/floem/views/struct.VirtualStack.html), `0.2.0`, [GPUI list source](https://docs.rs/gpui/0.2.2/src/gpui/elements/list.rs.html), `0.2.2`, accessed 2026-11-24, Direct.
- Masonry’s `TextInput` wraps a `TextArea`. GPUI’s `EntityInputHandler` exposes selected and marked ranges, text replacement and composition methods, and bounds for a text range. Those interfaces expose state and geometry needed by the platform IME. [Masonry `TextInput`](https://docs.rs/masonry/0.4.0/masonry/widgets/struct.TextInput.html), `0.4.0`, [GPUI `EntityInputHandler`](https://docs.rs/gpui/latest/gpui/trait.EntityInputHandler.html), `0.2.2`, accessed 2026-11-24, Direct.

### Inferences

- Floem’s visual-to-logical mapping and GPUI’s clipped hitboxes suggest using the same committed geometry for painting and picking. A transform-aware picker would convert the point to candidate-local coordinates and respect ancestor clips and local stacking order before returning the logical event path. Pointer capture can override the resulting target independently of capture-phase listeners.
- Virtualization introduces a focus policy. When the focused row leaves the realized range, the toolkit needs a defined way to keep it realized, realize it on demand, or transfer focus. Floem’s lazy rows and GPUI’s separate list and tab-stop state expose the boundary where that policy matters.

### Gaps

- I did not verify browser-equivalent stacking contexts in any native widget framework, nor the treatment of singular transforms, nested clip paths, or captured targets removed during dispatch across all toolkits.
- I did not establish a cross-toolkit behavior for nested wheel scrolling, focus restoration after hiding or removing a subtree, or IME composition during row removal. The cited APIs establish routing and state contracts, with platform behavior still to check.

## 3. Rust construction APIs, style invalidation, and animation

### Takeaway

Floem demonstrates create-once construction with regular Rust functions, fluent traits and tracked closures. Masonry exposes independent work requests for widget mutations. GPUI shows a frame-request animation adapter layered onto element rendering.

### Cited findings

- Floem `0.2.0` constructs an ordinary `impl IntoView` with functions, tuple children and fluent `.style(...)` and `.action(...)` calls. A closure passed to `label` reads the signal at the binding. Current `main` shows constructor spellings such as `Stack::horizontal`, `Button::new` and `Label::derived`. [Released Floem README](https://docs.rs/crate/floem/latest), `0.2.0`, [Floem repository README](https://github.com/lapce/floem#readme), undated `main`, accessed 2026-11-24, Direct.
- Masonry’s widget contexts expose `request_paint_only`, `request_layout`, `request_accessibility_update`, and `request_anim_frame`. The widget guide describes cached paint output, and the pass docs say passes can skip work until their invalidation flags are set. Floem’s style pass converts resolved style to Taffy style and requests layout when that value changes. [Implementing a Masonry widget](https://docs.rs/masonry/latest/masonry/doc/doc_02_implementing_widget/index.html), `0.4.0`, [Floem style pass](https://docs.rs/floem/0.2.0/src/floem/context.rs.html), `0.2.0`, accessed 2026-11-24, Direct.
- Floem supports transitions on interpolable style properties and keyframe animations. The style pass schedules more work while a transition needs another frame. Masonry’s animation pass visits animated widgets, and an animation tick alone does not invalidate paint. GPUI’s `AnimationExt` uses retained timing state, applies an eased value to the element, and requests another frame while animation continues. [Floem release README](https://docs.rs/crate/floem/latest), [Floem style pass](https://docs.rs/floem/0.2.0/src/floem/context.rs.html), `0.2.0`, [Masonry passes](https://docs.rs/masonry/latest/masonry/doc/internals_01_pass_system/index.html), [Masonry `Widget`](https://docs.rs/masonry_core/0.4.0/masonry_core/core/trait.Widget.html), `0.4.0`, [GPUI animation source](https://docs.rs/gpui/0.2.2/src/gpui/elements/animation.rs.html), `0.2.2`, accessed 2026-11-24, Direct.

### Inferences

- Floem’s fluent API suggests that typed functions and explicit reactive closures can define the construction and binding semantics before a macro is introduced. Solid’s compiled frontend offers an example of syntax lowering to a small renderer contract. Tachys’s experience suggests measuring Rust compile time before carrying the renderer type through every view type.
- Masonry’s invalidation APIs and Floem’s Taffy-style comparison suggest that property setters can declare which work they invalidate. An animated color can request paint, while a width change requests layout and a transform change may also require updated hit geometry. The frame clock can supply time while each resulting property mutation uses its regular invalidation path.

### Gaps

- I found no comparative measurements of builder versus macro ergonomics, compiler cost, or runtime performance for otherwise equivalent native APIs.
- The checked sources establish scheduling and invalidation mechanisms. They do not establish compositor-only animation, reduced-motion parity, or behavior under sustained high-refresh input across the toolkits.

## Leads

- Levien’s original architecture article traces view construction, stored child IDs, and widget-to-view message paths. It can help evaluate which identity and dispatch concepts carry over to a signal-driven front end. [Xilem: an architecture for UI in Rust](https://raphlinus.github.io/rust/gui/2022/05/07/ui-architecture.html), 2022-05-07, accessed 2026-11-24, Direct for the original design.
- Masonry’s architecture describes a test harness for simulated interactions and screenshots. A follow-up could use those entry points to check retained identity through list moves, capture across drag and removal, and focus cleanup when a subtree disappears. [Masonry architecture](https://github.com/linebender/xilem/blob/main/masonry/ARCHITECTURE.md), undated `main`, accessed 2026-11-24, Direct for harness capabilities.