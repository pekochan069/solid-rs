# Accessibility, Testing/DevTools, and Practical Pitfalls in Rust GPU GUI Libraries

## What I searched

Research cutoff and access date were 2026-11-24. Release records were checked separately from development-branch source. The queries below are reproduced as sent.

| Query | What it returned |
|---|---|
| `AccessKit accesskit_winit latest release TreeUpdate focus adapter docs.rs` | AccessKit project overview and winit adapter API. |
| `Masonry masonry_testing Xilem TestHarness snapshot Vello CPU Linebender` | Linebender architecture and a secondary report pointing to the GPU-to-CPU harness change. |
| `GPUI Zed VisualTestContext headless test inspector screenshot` | Zed’s GPUI testing example. |
| `Iced tester headless screenshot egui kittest Floem TestHarness` | Egui harness API and snapshot material. |
| `wgpu backend troubleshooting DX12 Vulkan Metal software adapter SwiftShader WARP llvmpipe` | Rerun troubleshooting and software-adapter examples. Several results concerned other WebGPU implementations. |
| `wgpu create_surface_safe unsafe pipeline cache compilation DX12 Vulkan Metal documentation` | Wgpu device API and pipeline-cache documentation. |
| `Zed GPUI wgpu Vulkan Linux atlas driver text IME maintainer` | A Zed report of an atlas panic after device loss. |
| `iced egui Floem Rust GUI compile time generics IME maintainer` | An Iced maintainer interview and secondary ecosystem surveys. |
| `Iced 0.14 tester Simulator Headless tiny skia iced_test selector debug devtools` | Published `iced_test` source metadata and a test example. |
| `egui kittest snapshot SwiftShader egui dev tools inspection UI debug` | Egui’s harness README and changelog. The fetched README contained no SwiftShader reference. |
| `Floem TestHarness headless Layout event inspector capture GitHub floem` | Indirect Floem references. The narrower search below found the harness. |
| `Masonry Xilem masonry_testing latest crates.io release CPU harness Linebender` | Project architecture and secondary references to `0.4.0`. |
| `site:zed.dev Zed linux Vulkan troubleshooting wgpu driver IME Windows shader atlas` | Official Zed installation, platform, and troubleshooting docs. |
| `site:github.com gfx-rs wgpu shader compilation slow startup pipeline cache SwiftShader testing` | A Vello maintainer’s Android pipeline-cache experiment. |
| `site:github.com linebender xilem IME AccessKit text accessibility Parley issue` | A Masonry text-area change involving layout and accessibility. |
| `site:github.com iced-rs iced compile times generics compilation renderer IME` | No primary measurement that isolated Iced’s generic compilation cost. |
| `Floem test headless inspector launch src inspector floem GitHub lapce` | Floem’s project-hosted `floem_test` source, README, and inspector example. |
| `GPUI Zed element inspector debug inspect test async dispatcher simulated time GPUI` | GPUI inspector and testing source locations. |
| `AccessKit retrofitting egui Zed accessibility Matt Campbell rich text platform AccessKit` | AccessKit project documentation and Campbell’s conference talk. |
| `wgpu maintainers tracing debugging PIX RenderDoc WGPU_BACKEND headless software testing SwiftShader` | Wgpu’s debugging wiki and mirrors. |
| `site:github.com zed gpui IME Windows TSF composition text input pull request` | A Zed Windows IME report and downstream cases. |
| `site:github.com iced slow compile time generic renderer monomorphization hecrj iced` | No usable primary measurement. |
| `site:raphlinus.github.io Xilem compile time Rust generic monomorphization performance` | Leads on Xilem’s type design. |
| `site:github.com zed egui etagere texture atlas fragmentation WebGPU glyph maintainer` | Atlas-related leads without a verified fragmentation measurement. |
| `Raph Levien Xilem Rust GUI compile time type erasure AnyView monomorphization` | Levien’s architecture article and Xilem’s published documentation. |
| `Iced creator compilation times Rust iced tiny skia debug time hecrj devtools time travel comet` | The Iced Comet repository. |
| `AccessKit Matt Campbell accessibility retrofit ui toolkit screen reader focus text Zed` | Xilem and AccessKit design leads without a quantified retrofit. |
| `Zed GPUI atlas fragmentation device loss fix Zed wgpu atlas` | GPUI wgpu context source, the atlas recovery report, and commit mirrors. |

Opened the AccessKit release records, schema and adapter APIs, and project README. Opened Masonry’s released test docs, tagged test source, development-branch harness and accessibility pass, and the Vello CPU release record. Opened the Iced test and renderer source, GPUI example and test/inspector source, Egui snapshot and accessibility docs, and Floem’s project-hosted test source and README. Opened wgpu’s release, instance, surface, adapter and cache docs, its debugging wiki, the SwiftShader, Mesa and Microsoft rasterizer docs, and Zed’s Linux docs and incident reports. Some versioned docs.rs URLs and Floem raw source URLs failed to open. The accessible project-hosted pages are cited below.

## How does AccessKit integrate with a retained GUI tree?

### Takeaway

A toolkit supplies an initial semantic tree, then pushes changes keyed by stable node IDs. The platform adapter presents that tree to assistive technology and routes requests back to the toolkit. Focus, bounds, and actions are part of the toolkit’s semantic state.

### Cited findings

- The latest versions located were `accesskit` **0.25.2** and `accesskit_winit` **0.34.2**, both published 2026-10-25. The winit adapter declares `accesskit ^0.25.2` and `winit ^0.30.5`. [AccessKit release record](https://docs.rs/crate/accesskit/latest), [AccessKit winit release record](https://docs.rs/crate/accesskit_winit/latest), accessed 2026-11-24, Direct.
- `TreeUpdate` is an atomic update against a specific prior tree. Each supplied `(NodeId, Node)` replaces that node’s record in full. For insertions, supply the child and update the parent’s child list. For removals, omit the subtree and update the parent’s child list. Initialization requires tree metadata. “The latest focus state must be provided with every tree update, even if the focus state didn’t change in a given update.” Use the root when no specific node has keyboard focus. [TreeUpdate fields](https://docs.rs/accesskit/0.25.2/accesskit/struct.TreeUpdate.html), 0.25.2, accessed 2026-11-24, Direct.
- Masonry’s accessibility pass maps `WidgetId` to `NodeId`, skips clean branches and stashed children, and derives role, border-box bounds, and transform from widget state. It applies the window scale at the root and calls the widget’s `accessibility` hook to complete its node. AccessKit nodes can advertise actions. An `ActionRequest` supplies an action, target tree, target node, and optional data. [Masonry accessibility pass](https://github.com/linebender/xilem/blob/main/masonry_core/src/passes/accessibility.rs), development branch, [AccessKit Node](https://docs.rs/accesskit/0.25.2/accesskit/struct.Node.html), [ActionRequest](https://docs.rs/accesskit/latest/accesskit/struct.ActionRequest.html), 0.25.2, accessed 2026-11-24, Direct.
- Create `accesskit_winit::Adapter` before showing the window and forward each window event to `process_event` before application handling. An event-loop proxy delivers requests asynchronously and may require a placeholder tree until the first full update. Mixed handlers can provide the initial tree synchronously while queuing actions and deactivation. The activation callback’s thread is platform-dependent. `update_if_active` only invokes its closure after tree initialization. [Adapter lifecycle](https://docs.rs/accesskit_winit/latest/accesskit_winit/struct.Adapter.html), [Adapter window events](https://docs.rs/accesskit_winit/latest/accesskit_winit/enum.WindowEvent.html), 0.34.2, accessed 2026-11-24, Direct.
- Desktop adapters expose AppKit `NSAccessibility` on macOS, the integration point for VoiceOver, UI Automation on Windows, and AT-SPI via D-Bus on Unix. The project reports rough adapter feature parity and support for single- and multiline text input. Its README still lists rich text and hypertext as gaps and says adapters implement only part of the schema. [AccessKit platform adapters](https://github.com/AccessKit/accesskit#platform-adapters), undated project README, accessed 2026-11-24, Direct for the adapter APIs and stated coverage.

### Inferences

- Stable widget identity, geometry after layout, and focus from the input system appear to be good sources for a retained toolkit’s accessibility pass. An action can then enter the same state transition as equivalent keyboard or pointer input. The AccessKit update contract and Masonry pass support that design. [TreeUpdate](https://docs.rs/accesskit/0.25.2/accesskit/struct.TreeUpdate.html), [Masonry accessibility pass](https://github.com/linebender/xilem/blob/main/masonry_core/src/passes/accessibility.rs).

### Gaps

- The adapter and schema docs establish what the library exposes. They leave live VoiceOver, UIA client, and AT-SPI behavior for particular widgets and actions unverified. The searches also yielded no measured estimate of accessibility retrofit effort.

## How do the projects test UI and inspect it during development?

### Takeaway

The five projects provide ways to exercise interactions without a normal application window. Their test coverage differs. Masonry, Iced, and Egui document image snapshots. GPUI’s examples exercise the element tree and action delivery. Floem’s harness demonstrates windowless interaction tests, and its application inspector exposes layout.

### Cited findings

- **Masonry/Xilem.** The latest published `masonry_testing` found was **0.4.0**, released 2025-10-29. It hosts a widget or a widget subtree, injects events, controls time, and compares PNGs through `assert_render_snapshot!`. The tagged screenshot path asks Vello’s wgpu `RenderContext` for an adapter without a surface. The development branch instead instantiates `VelloCpuRenderer` from `imaging_vello_cpu` and exposes a consumer accessibility tree alongside image output. The standalone `vello_cpu` release record lists **0.4.0**, dated 2026-11-11. That version does not identify the renderer used by the published Masonry harness. [Published test docs](https://docs.rs/crate/masonry_testing/latest), [Tagged GPU harness](https://github.com/linebender/xilem/blob/v0.4.0/masonry_testing/src/harness.rs), [Development CPU harness](https://github.com/linebender/xilem/blob/main/masonry_testing/src/harness.rs), [Vello CPU release record](https://docs.rs/crate/vello_cpu/latest), accessed 2026-11-24, Direct.
- **GPUI.** The testing example uses `#[gpui::test]`, `TestAppContext`, and `VisualTestContext`. It describes drawing after updates so a test can dispatch an action through the focused element and assert the result. The test context supplies a test platform and dispatcher, with simulated clipboard and dialogs. GPUI’s inspector source gates element inspection metadata on the `inspector` feature or debug assertions. [GPUI testing example](https://github.com/zed-industries/zed/blob/main/crates/gpui/examples/testing.rs), [Test context](https://github.com/zed-industries/zed/blob/main/crates/gpui/src/app/test_context.rs), [Inspector source](https://github.com/zed-industries/zed/blob/main/crates/gpui/src/inspector.rs), development branch, accessed 2026-11-24, Direct.
- **Iced.** `iced_test` **0.14.0** exposes a programmatically inspectable `Simulator`. The counter example selects a button, clicks it, and checks the emitted message. The API also documents simulated key input, typing, and image snapshots. [Iced test docs](https://docs.rs/iced_test/latest/iced_test/), [Tagged simulator source](https://github.com/iced-rs/iced/blob/0.14.0/test/src/simulator.rs), 0.14.0, accessed 2026-11-24, Direct.
- **Egui.** Published `egui_kittest` **0.37.0** dates to 2026-10-15. Its development README shows querying and driving widgets via AccessKit, asserting a checkbox’s semantic toggle state, and testing accessible names. The documented wgpu snapshot path uses `wgpu` and `snapshot` features, reference images, and configurable pixel comparison thresholds. Egui also documents an accessibility-tree inspector for its demo. [Release record](https://docs.rs/crate/egui_kittest/latest), 0.37.0, [Harness README](https://github.com/emilk/egui/blob/main/crates/egui_kittest/README.md), [Accessibility docs](https://github.com/emilk/egui/blob/main/docs/accessibility.md), development branch, accessed 2026-11-24, Direct.
- **Floem.** The project-hosted `floem_test` source shows `HeadlessHarness::new_with_size`, click and z-order tests, and helpers for pointer capture and scrolling. Its development README documents an element inspector for layout and a Tiny Skia fallback when a GPU is unavailable. The published `floem` record shows **0.2.0**, from 2024-11-14. The headless test evidence comes from project-hosted source docs. [Floem test source](https://lapce.dev/floem/src/floem_test/lib.rs.html), undated development docs, [Floem README](https://github.com/lapce/floem/blob/main/README.md), development branch, [Floem release record](https://docs.rs/crate/floem/latest), accessed 2026-11-24, Direct.
- **Software adapters.** Wgpu documents `force_fallback_adapter` as a filter that requests only a fallback adapter, generally a software implementation already on the system. SwiftShader provides a CPU Vulkan implementation, LLVMpipe is Mesa’s Gallium software rasterizer, and WARP provides Direct3D software rasterization. [Wgpu adapter options](https://docs.rs/wgpu/latest/wgpu/type.RequestAdapterOptions.html), 30.0.2, [SwiftShader](https://github.com/google/SwiftShader), undated project README, [Mesa LLVMpipe](https://docs.mesa3d.org/drivers/llvmpipe.html), rolling docs, [Microsoft WARP guide](https://learn.microsoft.com/en-us/windows/win32/direct3darticles/directx-warp), rolling docs, accessed 2026-11-24, Direct.

### Inferences

- A semantic test can prove which node was exposed and how it responded. A CPU-rendered snapshot can catch a visual regression in that renderer. An offscreen wgpu test with a software adapter can exercise the selected graphics API and readback path. Native window behavior and real driver or assistive-technology integration still require evidence from those environments. [Masonry CPU harness](https://github.com/linebender/xilem/blob/main/masonry_testing/src/harness.rs), [Egui snapshot guidance](https://github.com/emilk/egui/blob/main/crates/egui_kittest/README.md#what-to-do-when-ci--another-computer-produces-a-different-image), [Wgpu adapter options](https://docs.rs/wgpu/latest/wgpu/type.RequestAdapterOptions.html).

### Gaps

- The searched GPUI and Floem harness sources yielded no verified stock screenshot baseline workflow. The released `floem_test` and GPUI package versions remain unverified.
- The fetched Iced pages establish headless simulation and snapshots. They leave the renderer feature selection and software-only CI configuration unverified. The searches also yielded no first-party evidence that all five projects use SwiftShader, WARP, or LLVMpipe in CI.

## What engineering pitfalls do the projects document?

### Takeaway

Published reports identify startup shader compilation, adapter and driver variation, atlas recovery after device loss, and native IME composition as distinct failure modes. Wgpu provides a safe window-surface API. Raw native handles and persistent pipeline caches have separate safety obligations.

### Cited findings

- **Pipeline startup.** A Vello maintainer reported that compute-pipeline creation dominated roughly two seconds of Android startup. In a Pixel 6 experiment, cached pipeline creation took about 30 ms on subsequent launches. Those are figures for that experiment. Wgpu 30.0.2 describes `PipelineCache` reuse on the same or similar devices and exposes an unsafe `Device::create_pipeline_cache`. [Vello maintainer’s wgpu report](https://github.com/gfx-rs/wgpu/issues/5293), 2024-02-23, [Wgpu PipelineCache](https://docs.rs/wgpu/latest/wgpu/struct.PipelineCache.html), [Device cache API](https://docs.rs/wgpu/latest/wgpu/struct.Device.html#method.create_pipeline_cache), 30.0.2, accessed 2026-11-24, Direct for the reported experiment and API contract.
- **Backend and driver variation.** Egui lists differences in MSAA, texture filtering, and floating-point evaluation across GPUs, drivers, and Metal/Vulkan/DX12 as causes of different snapshots. Zed’s Linux docs show how to collect adapter diagnostics with wgpu logging for graphics initialization failures. Wgpu’s debugging wiki points to RenderDoc and Xcode GPU captures. [Egui snapshot troubleshooting](https://github.com/emilk/egui/blob/main/crates/egui_kittest/README.md#what-to-do-when-ci--another-computer-produces-a-different-image), development branch, [Zed Linux graphics diagnostics](https://zed.dev/docs/linux#reporting-graphics-issues), [Wgpu debugging wiki](https://github.com/gfx-rs/wgpu/wiki/Debugging-wgpu-Applications), rolling docs, accessed 2026-11-24, Direct.
- **Atlas recovery.** A Zed user reported repeated Vulkan device loss on Linux/X11, apparent context recovery, and then an out-of-bounds panic in `WgpuAtlas`. The report leaves the cause of the original device loss unresolved. It documents a recovery failure and supplies no measurement of atlas fragmentation. [Zed WgpuAtlas report](https://github.com/zed-industries/zed/issues/62998), 2026-08-21, accessed 2026-11-24, Direct for the reported observation.
- **Surface lifetimes.** Wgpu 30.0.2 exposes safe `Instance::create_surface` for a supported `SurfaceTarget`, including a window-handle provider. `create_surface_unsafe` accepts raw handles and requires the caller to uphold their validity and lifetime. [Wgpu safe surface creation](https://docs.rs/wgpu/latest/wgpu/struct.Instance.html#method.create_surface), [Unsafe target and safety requirements](https://docs.rs/wgpu/30.0.2/wgpu/enum.SurfaceTargetUnsafe.html), 30.0.2, accessed 2026-11-24, Direct.
- **IME ordering.** A Zed Windows report describes duplicate insertion during Chinese IME composition. The reporter attributes it to regular key/character dispatch running alongside the IME composition path. This is evidence of the reported failure mode, with no cross-platform reproduction or current fix verified in this pass. [Zed Chinese IME report](https://github.com/zed-industries/zed/issues/56327), 2026 report, accessed 2026-11-24, Direct for the report and its diagnosis.

### Inferences

- A toolkit crate can likely keep `#![forbid(unsafe_code)]` with ordinary supported window targets by using wgpu’s safe surface API. Raw embedding and persisted pipeline-cache loading are distinct integration choices. [Wgpu Instance](https://docs.rs/wgpu/latest/wgpu/struct.Instance.html), [Unsafe surface target](https://docs.rs/wgpu/30.0.2/wgpu/enum.SurfaceTargetUnsafe.html), [Device cache API](https://docs.rs/wgpu/latest/wgpu/struct.Device.html#method.create_pipeline_cache).
- The IME report suggests testing preedit, candidate placement, and final commit independently of plain typing. The device-loss report suggests checking the validity of renderer resources after recreation, including atlas entries. [Zed Chinese IME report](https://github.com/zed-industries/zed/issues/56327), [Zed WgpuAtlas report](https://github.com/zed-industries/zed/issues/62998).

### Gaps

- Searches for Iced/Xilem compile times and monomorphization yielded architecture discussion and historical leads. They did not yield a current, controlled measurement that separates application generics from dependencies or linking.
- The atlas searches yielded device-loss recovery evidence and downstream reports. They did not establish how quickly atlases fragment under representative workloads or how much it costs.
- No verified old-GPU compatibility matrix covering the five projects, quantitative Metal/DX12/Vulkan startup comparison, or measured accessibility retrofit cost surfaced.

## Leads

- Levien’s Xilem architecture article discusses large static view types, `AnyView`, and monomorphization. It is a historical design lead for evaluating compile-time tradeoffs. [Xilem architecture article](https://raphlinus.github.io/rust/gui/2022/05/07/ui-architecture.html).
- Iced’s Comet repository is a lead for inspecting and time-travel debugging applications. Its integration and release status need a separate check. [Iced Comet](https://github.com/iced-rs/comet).
- Zed’s renderer source surfaced a dedicated headless wgpu context. Follow its callers to determine whether a current GPUI test or benchmark verifies output pixels. [GPUI wgpu context](https://github.com/zed-industries/zed/blob/main/crates/gpui_wgpu/src/wgpu_context.rs).
- AccessKit maintainer Matt Campbell’s RustWeek talk is a lead for platform demonstrations and the project’s accessibility design tradeoffs. [AccessKit reusable UI accessibility talk](https://www.youtube.com/watch?v=OI2TzJ6Sw10).