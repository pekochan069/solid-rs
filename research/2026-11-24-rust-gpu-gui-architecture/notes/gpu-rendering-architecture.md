# GPU 2D UI Rendering Architecture in Rust

## What I searched

As-of and access date for all sources was 2026-11-24. I looked for release records, the code that turns a scene into GPU work, and implementations that distinguish command reuse, pixel reuse, and partial presentation.

| Query | What it returned |
|---|---|
| `wgpu releases 2026 GitHub` | Official gfx-rs release history, a separate Go project with the same name, and third-party guides. |
| `Linebender vello releases hybrid cpu` | Linebender posts, changelogs, and release leads for the compute and sparse-strips renderers. |
| `anyrender vello skia femtovg releases` | AnyRender repository activity and ecosystem comparisons. |
| `rust skia safe femtovg releases crates` | Rust-Skia release history and API docs. FemtoVG needed a direct package lookup. |
| `Zed GPUI rendering scene batching atlas shader` | Zed renderer material and third-party scene analyses. |
| `Iced iced_wgpu renderer quad text layer cache` | Iced renderer docs and source leads, with an unrelated fork among the results. |
| `Makepad draw list draw quad SDF rendering` | The Makepad Book’s rendering chapter. |
| `Floem vger renderer wgpu floem_renderer` | Floem’s renderer abstraction docs. |
| `WebRender picture caching partial present damage Mozilla` | A Mozilla report on damage accounting when a render is skipped for presentation. |
| `Skia saveLayer clip color spaces linear sRGB` | Skia canvas declarations and color material. |
| `Vello rendering pipeline tile binning Raph Levien` | Linebender’s architecture document, Levien’s account of compute memory allocation, and his rendering slides. |
| `WebGPU scissor sRGB blending specification` | W3C WebGPU drafts. |
| `anyrender docs.rs DioxusLabs PaintScene` | The upstream AnyRender README and backend table. |
| `femtovg docs.rs wgpu Canvas` | FemtoVG canvas API and changelog leads. |
| `iced_wgpu docs.rs official quad triangle` | Iced’s renderer docs, quad pipeline, and render-loop source. |
| `vello_hybrid vello_cpu docs.rs latest` | Official crate docs alongside a separately named fork. |
| `site:github.com/zed-industries/zed gpui mac windows wgpu renderer batching` | GPUI platform dispatch, renderer source leads, and proposals. |
| `site:github.com/iced-rs/iced wgpu renderer layer damage` | Iced renderer source and a layer bug report. |
| `site:firefox-source-docs.mozilla.org webrender picture caching tiles retained` | Mozilla’s renderer overview and cache design. |
| `Khronos EGL buffer age swap buffers damage` | EGL specifications and vendor guidance. |

I opened the package histories for `wgpu`, `vello`, `vello_hybrid`, `vello_cpu`, `anyrender`, `skia-safe`, `femtovg`, and `iced_wgpu`, plus the gfx-rs, Linebender, and Rust-Skia release pages. I opened Linebender’s architecture notes, Levien’s post, AnyRender’s README and 0.14 API docs, Rust-Skia’s API docs, Skia’s canvas and color docs, the FemtoVG and Vger repositories, the Makepad Book and repository, Floem’s renderer docs, Zed’s scene, platform and wgpu renderer source, Iced’s 0.14 API and tagged renderer source, Mozilla’s renderer overview and damage report, W3C WebGPU, and Khronos’s two EGL damage-related extensions. The direct `vello_gpu` package page, the crates.io API endpoints for `vello_gpu` and `anyrender`, and the wgpu `SurfaceTexture` docs did not open successfully.

## How do the rendering approaches compare?

### Takeaway

`wgpu` provides GPU access. GPUI and Iced implement UI-specific drawing pipelines on it, while compute Vello and sparse-strips Vello implement general vector drawing with different CPU/GPU splits. Skia and FemtoVG offer canvas-style drawing APIs. AnyRender provides a common drawing interface with separate backends.

### Cited findings

- The latest `wgpu` release visible in gfx-rs and its published package history was `30.0.2`, dated 2026-11-12. [gfx-rs releases](https://github.com/gfx-rs/wgpu/releases), [wgpu package versions](https://docs.rs/crate/wgpu/latest), 30.0.2, accessed 2026-11-24, Direct.
- GPUI’s wgpu renderer uses instanced batches with distinct pipelines for quads, shadows, paths, underlines, sprites and surfaces. It includes a shader variant for adapters without storage buffers, including WebGL2. GPUI’s platform dispatcher selects platform-specific implementations and uses a Metal renderer for macOS headless rendering. [GPUI wgpu renderer](https://github.com/zed-industries/zed/blob/main/crates/gpui_wgpu/src/wgpu_renderer.rs), [GPUI platform dispatch](https://github.com/zed-industries/zed/blob/main/crates/gpui_platform/src/gpui_platform.rs), `main` as accessed 2026-11-24, Direct.
- `iced_wgpu` `0.14.0`, published 2025-12-07, records a layer stack and has distinct quad, triangle, text and image renderer state. Its public docs list rounded quads, clip areas, glyphon text, images, SVG and triangle meshes. [Iced 0.14 renderer source](https://raw.githubusercontent.com/iced-rs/iced/0.14.0/wgpu/src/lib.rs), [iced_wgpu package versions](https://docs.rs/crate/iced_wgpu/latest), 0.14.0, accessed 2026-11-24, Direct.
- Makepad documents deferred draw lists, instance data, and merging compatible draw calls. Its repository describes Metal on macOS, DX11 on Windows, OpenGL on Linux and WebGL on the web. Its instancing design can be studied independently of wgpu. [Makepad rendering mechanism](https://makepad.rs/guide/widget/widget-render), [Makepad repository](https://github.com/makepad/makepad), undated guide and repository as accessed 2026-11-24, Direct.
- The `vello` `0.12.0` release, dated 2026-11-11, is titled “Vello Research.” It encodes paths, draws, transforms and resources into buffers, then dispatches GPU compute stages using prefix scans. Levien describes the pipeline as “producing a separate optimized byte-code like program for each 16x16 pixel tile” and reports that scene changes can make intermediate buffer requirements unpredictable. The published crate describes itself as alpha. [Vello releases](https://github.com/linebender/vello/releases), [Vello package and README](https://docs.rs/crate/vello/latest), [Vello architecture](https://github.com/linebender/vello/blob/main/ARCHITECTURE.md), [Levien’s compute analysis](https://raphlinus.github.io/gpu/2025/03/21/good-parallel-computer.html), 0.12.0 and 2025-03-21, accessed 2026-11-24, Direct.
- The old `vello_hybrid` package history ends at `0.2.0` on 2026-08-07. Its successor `vello_gpu` shares CPU preprocessing with `vello_cpu`. The CPU flattens paths, bins them into tiles and generates sparse horizontal strips. `vello_gpu` uploads strips and paint data for vertex/fragment rasterization and compositing through wgpu or WebGL2. `vello_cpu` uses CPU SIMD, with optional multithreading, and reached `0.4.0` on 2026-11-11. The AnyRender compatibility table names `vello_gpu` `0.4` for its 0.15 line. [vello_hybrid package](https://docs.rs/crate/vello_hybrid/latest), [vello_cpu package](https://docs.rs/crate/vello_cpu/latest), [Vello architecture](https://github.com/linebender/vello/blob/main/ARCHITECTURE.md), [AnyRender compatibility table](https://github.com/DioxusLabs/anyrender#version-compatibility), versions as stated, accessed 2026-11-24, Direct.
- `skia-safe` `0.157.0`, published 2026-11-03, wraps C++ Skia. It defaults to CPU rasterization and exposes feature-gated GPU support for OpenGL, Vulkan, Metal and Direct3D. SVG and paragraph layout are also feature-gated. [skia-safe package versions](https://docs.rs/crate/skia-safe/latest), [Rust-Skia API docs](https://docs.rs/skia-safe/latest/skia_safe/), 0.157.0, accessed 2026-11-24, Direct.
- FemtoVG is a Rust port of NanoVG. The `0.28.0` package, published 2026-10-27, lists a wgpu backend and an optional `wgpu ^30.0.0` dependency. The same README still says rendering uses “one OpenGl (ES) 3.0+ backend,” while its feature list also names OpenGL ES2 and wgpu. [FemtoVG 0.28 package and README](https://docs.rs/crate/femtovg/latest), 0.28.0, accessed 2026-11-24, Direct.
- AnyRender defines `PaintScene`, `WindowRenderer`, and `ImageRenderer`. It also exports a recording `Scene`. The latest package-history entry I could verify was `0.14.0`, dated 2026-10-03. The upstream README also describes a 0.15 line and says that line renames its hybrid adapter from `anyrender_vello_hybrid` to `anyrender_vello_gpu`. Its backend list includes Skia and Vello variants and names FemtoVG as a possible contribution. Floem separately documents Vello, Skia via AnyRender, Vger, and tiny-skia implementations behind its own renderer trait. [AnyRender package history](https://docs.rs/crate/anyrender/latest), [AnyRender 0.14 API](https://docs.rs/anyrender/0.14.0/anyrender/), [AnyRender upstream README](https://github.com/DioxusLabs/anyrender), [Floem renderer abstraction](https://lapce.dev/floem/floem_renderer/index.html), versions as stated, accessed 2026-11-24, Direct.

### Inferences

- A renderer specialized for quads and sprites can keep common widget drawing on a small set of pipelines. Arbitrary paths, nested clips, and isolated blending increase the work its own scene format and shaders must express. General vector engines already define more of that drawing model. The sources establish the division of work. They do not establish a performance ranking for desktop UI.
- Choosing a backend through AnyRender still leaves version compatibility and rendering behavior to verify. Its dependency matrix shows how the specific adapter, vector engine and wgpu versions move together.

### Gaps

- I could verify the `vello_gpu` rename and upstream’s use of the `0.4` line, but the direct registry lookup did not establish the exact latest patch or publication date. I also could not verify publication of AnyRender 0.15 independently of the upstream README.
- The Zed sources establish the wgpu implementation and platform dispatch. They leave the renderer selected by each released desktop build unverified. I found no controlled comparison of these libraries on the same desktop UI scene and hardware.

## What drawing operations and pipeline stages does a desktop UI need?

### Takeaway

The scene needs to carry draw order, geometry, transforms, clips, paint and group boundaries wherever those change the result. The backend then resolves resources, prepares geometry and coverage, rasterizes, composites and presents.

### Cited findings

- GPUI uses separate quad and shadow pipelines. Iced exposes rounded quads and separate triangle meshes. Compute Vello has a blurred rounded-rectangle operation, with inverted blur coverage for inset shadows added in 0.12.0. FemtoVG documents Bézier fills and strokes and transformed gradient stops. These are concrete examples of how backgrounds, borders, shapes and shadows can enter a pipeline. [GPUI wgpu renderer](https://github.com/zed-industries/zed/blob/main/crates/gpui_wgpu/src/wgpu_renderer.rs), [Iced 0.14 renderer](https://docs.rs/iced_wgpu/0.14.0/iced_wgpu/), [Vello releases](https://github.com/linebender/vello/releases), [FemtoVG 0.28 package](https://docs.rs/crate/femtovg/latest), versions as stated, accessed 2026-11-24, Direct.
- WebGPU’s integer scissor discards fragments outside an axis-aligned rectangle after the viewport transform. Skia exposes transformed rectangle, rounded-rectangle and path clips. FemtoVG documents rounded scissoring and arbitrary-path clipping with fill rules. [WebGPU `setScissorRect`](https://www.w3.org/TR/webgpu/#dom-gpurenderpassencoder-setscissorrect), [SkCanvas reference](https://api.skia.org/classSkCanvas.html), [FemtoVG 0.28 package](https://docs.rs/crate/femtovg/latest), WebGPU draft 2026-11-16, undated Skia reference and FemtoVG 0.28.0, accessed 2026-11-24, Direct.
- Skia `saveLayer` draws children into a layer and applies alpha, color and image filters, and blend mode on restore. FemtoVG separately documents group capture with declared opacity, filters and layer masks. Mozilla describes turning pictures with effects into render passes with input dependencies. [SkCanvas `saveLayer`](https://api.skia.org/classSkCanvas.html), [FemtoVG 0.28 package](https://docs.rs/crate/femtovg/latest), [Mozilla rendering overview](https://firefox-source-docs.mozilla.org/gfx/RenderingOverview.html), FemtoVG 0.28.0 and undated reference docs, accessed 2026-11-24, Direct.
- Mozilla describes preparing glyphs and SVG during scene building, with glyphs sampled from a texture atlas. Iced routes text through glyphon and keeps a separate image cache in feature-enabled builds. Vello offers a separate SVG integration that draws into its vector scene. SVG can therefore enter a renderer as raster content or vector drawing commands, depending on the integration. [Mozilla rendering overview](https://firefox-source-docs.mozilla.org/gfx/RenderingOverview.html), [Iced 0.14 renderer source](https://raw.githubusercontent.com/iced-rs/iced/0.14.0/wgpu/src/lib.rs), [Vello SVG integration notes](https://docs.rs/crate/vello/latest), Iced 0.14.0, Vello 0.12.0 and undated Mozilla docs, accessed 2026-11-24, Direct.
- WebGPU states that “WebGPU textures hold raw numeric data.” An `-srgb` texture format supplies transfer encoding and decoding. The spec treats gamut, transfer function and alpha premultiplication at color-managed inputs and outputs as separate properties. [WebGPU color spaces and encoding](https://www.w3.org/TR/webgpu/#color-spaces-and-encoding), draft 2026-11-16, accessed 2026-11-24, Direct.

### Inferences

- For an instanced widget pipeline, a rounded quad can carry bounds, radii, border and paint parameters. A rectangular scissor can bound work for a rounded or path clip, whose actual edge coverage can come from a shader test, a mask or stencil. The clip transform and nesting determine which fragments survive.
- Group opacity must apply to the combined result when children overlap. Multiplying each child’s alpha independently changes the overlap. Skia’s layer restore illustrates where the opacity belongs.
- The renderer needs a documented color policy across paints, gradients, images, glyph coverage, intermediate layers and presentation. A format suffix alone cannot specify gamut conversion or the alpha precision and conventions expected across passes.

### Gaps

- The sources establish examples of each operation. I did not verify backend-by-backend parity for transformed rounded clips, nested opacity and blend modes, SVG filters, subpixel text, or wide-gamut output.

## How do display lists, batching, layer caching and damage tracking interact?

### Takeaway

A renderer can reuse drawing commands, reuse rasterized layers, redraw only damaged pixels, or tell the compositor which pixels changed. Each saves work at a different stage. Fine-grained reactive updates help when the renderer can map affected views to stable commands or bounded areas of the screen.

### Cited findings

- GPUI can replay a range of the previous scene’s paint operations, including layer boundaries. It sorts primitives by draw order and uses atlas texture as a secondary key for sprites. The inspected wgpu render pass still clears the frame target before drawing scene batches. Command replay and a fresh framebuffer render coexist in this implementation. [GPUI scene source](https://github.com/zed-industries/zed/blob/main/crates/gpui/src/scene.rs#L268), [GPUI wgpu render pass](https://github.com/zed-industries/zed/blob/main/crates/gpui_wgpu/src/wgpu_renderer.rs#L1335), `main` as accessed 2026-11-24, Direct.
- Makepad’s `DrawList` view optimization retains a view’s drawing commands. Its `Texture` mode renders the view to a texture and draws that texture as a unit. [Makepad view optimization](https://makepad.rs/guide/widget/widget-render), undated, accessed 2026-11-24, Direct.
- WebRender groups pictures into slices expected to update together. A tile cache tracks which tiles need rerasterization, and interned drawing data lets it compare tile contents across frames. Its docs describe subdividing repeatedly invalidated tiles and recombining them after a period of stability. [Mozilla picture caching and interning](https://firefox-source-docs.mozilla.org/gfx/RenderingOverview.html#caching), undated, accessed 2026-11-24, Direct.
- EGL buffer age reports how many frames have elapsed since the acquired buffer’s contents were defined, with age zero indicating invalid contents. Swap-with-damage sends changed surface rectangles to the compositor. Mozilla recorded a bug caused by tracking damage between renders when the compositor needed damage between presents. [EGL buffer age](https://registry.khronos.org/EGL/extensions/EXT/EGL_EXT_buffer_age.txt), revision 12, 2013-06-13, [EGL swap with damage](https://registry.khronos.org/EGL/extensions/KHR/EGL_KHR_swap_buffers_with_damage.txt), version 13, 2020-02-20, [Mozilla partial-present bug](https://bugzilla.mozilla.org/show_bug.cgi?id=1668472), Firefox 84, accessed 2026-11-24, Direct.

### Inferences

- Batching compatible commands reduces state changes and submissions within a frame. Reordering overlapping translucent draws can change source-over output. Sorting therefore needs an ordering constraint, such as the draw-order key in GPUI, or proof that the reordered operations cannot affect each other.
- Paint invalidation for a moved or removed view needs its previous bounds as well as its new bounds. Clips, transforms, shadows and filters can change the affected area. Repairing damaged pixels also requires drawing any intersecting content needed to reconstruct those pixels in composition order.
- Reactive invalidation can reduce command rebuilding even when the renderer clears and redraws the target. Pixel caching can additionally save raster work for stable, costly content, at the expense of texture memory and invalidation bookkeeping. Broad animation and effects that cross cache boundaries can reduce those savings.
- Partial raster and partial presentation can pay off independently. A renderer can keep its own persistent texture and repair a region there, then draw a full window frame from it. Direct partial repaint of a rotating window buffer depends on preserved contents or buffer age, and compositor damage depends on what was last presented.

### Gaps

- I did not verify a portable wgpu 30.0.2 API for buffer age or damage rectangles on surface presentation. The EGL behavior establishes the distinction for EGL, not a wgpu guarantee.
- I found concrete cache designs and a partial-presentation failure report. I found no matched measurements showing which strategy minimizes frame time or energy across sparse edits, scrolling and broad animation in a fine-grained Rust UI.

## Leads

- Linebender’s architecture document links the sparse-strips design and Vello CPU thesis. Follow those for coverage encoding and memory behavior. [Vello architecture](https://github.com/linebender/vello/blob/main/ARCHITECTURE.md).
- The AnyRender compatibility matrix is a starting point for checking which renderer adapters can share a wgpu device. Verify the selected published manifests before integrating them. [AnyRender compatibility matrix](https://github.com/DioxusLabs/anyrender#version-compatibility).