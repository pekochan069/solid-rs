# Layout Engines and Text Shaping/Rasterization Stack in Rust GUI

## What I searched

As of 2026-11-24. All cited sources were accessed on that date.

| Exact query | Results that informed the research |
|---|---|
| `Taffy LayoutPartialTree cache mark_dirty intrinsic measurement` | Taffy tree and cache source, plus a TaffyJS explanation. |
| `Masonry Widget layout BoxConstraints Flutter layout constraints` | Flutter’s box constraints documentation. |
| `morphorm Rust layout auto size stack grid cache` | Morphorm rustdoc and a GPUI adapter’s compatibility notes. |
| `Parley fontique harfrust text selection PlainEditor line break` | Parley’s text-stack README and changelog, initially indexed through mirrors. |
| `cosmic-text Buffer shape run layout Editor SwashCache` | COSMIC Text 0.19.0 buffer source and older rendered copies of editor source. |
| `Zed GPUI text system DirectWrite CoreText Linux cosmic text` | GPUI implementation discussion, a Zed maintainer video, and third-party notes. |
| `Vello glyphs skrifa swash color subpixel antialiasing Atlas` | Vello Glifo atlas key source and rasterizer comparisons. |
| `Parley layout incremental caching width measurement Linebender` | Parley’s changelog entry on intrinsic width caching. |
| `Masonry Xilem BoxConstraints Widget layout measure place site:docs.rs` | Masonry’s `Widget` and `LayoutCtx` docs. |
| `parley Linebender fontique HarfRust Skrifa PlainEditor site:docs.rs` | A Parley 0.11.0 manifest. The findings use 0.13.0 docs where available. |
| `Zed gpui DirectWrite CoreText CosmicTextSystem site:github.com/zed-industries/zed` | GPUI’s platform README and Zed implementation references. |
| `Flutter Inside Flutter sublinear layout relayout boundary markNeedsLayout` | Flutter’s architecture article and dirty-layout API. |
| `GPUI CosmicTextSystem gpui_wgpu text_system CoreText Zed Windows text system mac source` | References to the three platform implementation paths. |
| `Core Text CTLine string index CoreText Apple Programming Guide font fallback` | Archived Core Text materials and WebKit integration notes. |
| `DirectWrite GetLineMetrics HitTestTextRange color glyph Microsoft Pango layout index_to_pos x_ranges` | Microsoft’s DWriteCore spec and references to Pango paragraph APIs. |
| `Vello draw_glyphs color bitmap glyphs Swash zeno subpixel rasterize Fontations Skrifa` | First-party Skrifa README and third-party Vello comparisons. |
| `Parley 0.13 PlainEditor parley editor rich text November 2026 Linebender` | Parley changelog, releases, and the open `RichEditor` proposal. |
| `Flutter IntrinsicHeight worst case O(N²) depth tree` | Flutter’s `IntrinsicHeight` docs and related intrinsic sizing APIs. |

I opened the version registries and the primary API, source, and changelog pages cited below, as well as the [Xilem repository](https://github.com/linebender/xilem). Direct opens of several versioned Taffy rustdoc URLs failed, so I used `latest` pages whose headers showed 0.16.0. The versioned Parley editor URLs and release source index were inaccessible in the fetch tool. I used the first-party changelog and pull request to establish the editor evidence below.

## How do Taffy, box constraints, and Morphorm compare?

### Takeaway

Taffy provides CSS-derived layout algorithms for a library-owned or application-owned tree. Flutter and Masonry let each container decide how to measure and place its children under box constraints. Morphorm provides its own depth-first algorithm and a smaller set of layout rules.

### Cited findings

- Taffy 0.16.0 was published on 2026-11-23. The crate enables Flexbox, CSS Grid, and Block layout by default. `TaffyTree` owns nodes, styles, and computed geometry. Its low-level API lets the caller keep its own tree and dispatch individual nodes through functions such as `compute_flexbox_layout` and `compute_grid_layout`. [Taffy release record](https://docs.rs/crate/taffy/0.16.0), [Taffy architecture and features](https://docs.rs/taffy/latest/taffy/), 0.16.0, accessed 2026-11-24, Direct.
- `LayoutPartialTree` exposes child traversal, core styles, unrounded layout writes, and `compute_child_layout`. Its custom-tree example dispatches Flexbox, Grid, and measured leaves and wraps dispatch in `compute_cached_layout`. `CacheTree` supplies storage for that helper. `TaffyTree::mark_dirty` clears the changed node’s cache and walks to its ancestors, stopping if it reaches a node whose cache is already empty. The cache accounts for known dimensions, available space, and parent size. [LayoutPartialTree](https://docs.rs/taffy/latest/taffy/trait.LayoutPartialTree.html), [Taffy tree source](https://docs.rs/taffy/latest/src/taffy/tree/taffy_tree.rs.html#938-967), [Taffy cache source](https://docs.rs/taffy/latest/src/taffy/tree/cache.rs.html#125-150), 0.16.0, accessed 2026-11-24, Direct.
- Flutter’s box protocol passes minimum and maximum width and height down, returns sizes up, and leaves placement to the parent. Masonry’s `Widget::layout` takes `&BoxConstraints` and returns `Size`. A container uses `run_layout` and `place_child` and chooses the order in which to measure its children. Masonry’s docs qualify the one-visit convention: “For efficiency, a container should only invoke layout of a child widget once, though there is nothing enforcing this.” [Inside Flutter](https://docs.flutter.dev/resources/inside-flutter#sublinear-layout), [Masonry Widget](https://docs.rs/masonry_core/latest/masonry_core/core/trait.Widget.html#tymethod.layout), live docs, accessed 2026-11-24, Direct.
- Morphorm describes a depth-first, one-pass algorithm. It offers Row, Column, Overlay, and Grid, with pixel, percentage, stretch, and auto sizes. Its `Node` trait lets an existing tree or ECS supply styles and children. `SubLayout` can carry context for measuring or caching wrapped text, while the documented `Cache` trait stores layout output. [Morphorm crate docs](https://docs.rs/morphorm/0.9.0/morphorm/), 0.9.0, accessed 2026-11-24, Direct.

### Inferences

- If CSS sizing and track behavior are part of the API contract, Taffy supplies those algorithms. If layout is defined widget by widget, the Masonry protocol gives container authors direct control over measurement order and placement. Morphorm fits a contract defined around its own stack, stretch, and grid semantics. This follows from the respective APIs, without a performance ranking.
- `LayoutPartialTree` may avoid maintaining a second node tree in a retained GUI. The adapter still needs a rule for invalidating the relevant cache entries when externally stored style, children, or text measurement inputs change. This follows from the custom-tree example and `TaffyTree::mark_dirty`.

### Gaps

- I found no comparable benchmark for these three approaches under the same workloads, particularly deep trees with wrapped labels and reactive edits.
- Morphorm documents its output store. The sources I opened did not establish a built-in dirty-propagation and subtree-reuse contract comparable to `TaffyTree::mark_dirty`.

## What does a production text stack need, and how do the options compare?

### Takeaway

A GPU GUI needs font matching and fallback, Unicode analysis, shaping, width-dependent line layout, cursor and selection geometry, and a renderer for monochrome and color glyphs. Parley and COSMIC Text both use HarfRust in the releases examined. They differ in font discovery, editor APIs, and what they hand to the renderer.

### Cited findings

- Latest published versions observed were [Parley 0.13.0](https://docs.rs/crate/parley/0.13.0) and [Fontique 0.13.0](https://docs.rs/crate/fontique/0.13.0), 2026-11-12, [COSMIC Text 0.19.0](https://docs.rs/crate/cosmic-text/0.19.0), 2026-04-22, [Skrifa 0.50.0](https://docs.rs/crate/skrifa/0.50.0), 2026-11-10, [HarfRust 0.16.1](https://docs.rs/crate/harfrust/0.16.1), 2026-11-19, and [Swash 0.2.10](https://docs.rs/crate/swash/0.2.10), 2026-07-17. These are publication versions, and Parley’s changelog labels its 0.13.0 entry 2026-11-06. [Parley changelog](https://github.com/linebender/parley/blob/main/CHANGELOG.md#0130---2026-11-06), accessed 2026-11-24, Direct.
- Parley’s stack uses Fontique for font enumeration and fallback, HarfRust for shaping, Skrifa over `read-fonts` for font metrics and outlines, and ICU4X for Unicode analysis such as segmentation and bidi. Parley builds styled paragraphs, breaks and aligns lines, and exposes glyph runs and caret positions. The default Fontique `system` feature enables system font backends. Its optional `fontconfig-dlopen` mode can build without Fontconfig, in which case system font fallback is unavailable. [Parley text stack](https://github.com/linebender/parley#the-parley-text-stack), undated, [Parley crate docs](https://docs.rs/parley/0.13.0/parley/), [Fontique crate docs](https://docs.rs/fontique/0.13.0/fontique/), 0.13.0, accessed 2026-11-24, Direct.
- Parley’s styled layout builders accept range, tree, or indexed style runs. Its changelog documents `PlainEditor` selection rectangles, byte-based navigation, and IME preedit. Version 0.13.0 adds `Layout::primary_caret`, which returns the line and caret x coordinate for a byte index. The proposed `RichEditor` adds per-range styled editing and IME composition, and the pull request was open when fetched. [Parley crate docs](https://docs.rs/parley/0.13.0/parley/), [Parley changelog](https://github.com/linebender/parley/blob/main/CHANGELOG.md), 0.13.0 and historical entries, [RichEditor proposal](https://github.com/linebender/parley/pull/747), opened 2026-08-14, accessed 2026-11-24, Direct.
- COSMIC Text’s docs specify “Shaping utilizes harfrust, font discovery utilizes fontdb, and the rasterization is optional and utilizes swash.” `Buffer` takes UTF-8 and attributes, exposes visible layout runs, and supports coordinate hit-testing. `LayoutRun::highlight` returns an iterator of horizontal selection spans. `Editor` wraps the buffer and provides rendering hooks for the cursor, selection, and selected text. This establishes styled layout and editing operations. [COSMIC Text crate docs](https://docs.rs/cosmic-text/0.19.0/cosmic_text/), [Buffer](https://docs.rs/cosmic-text/0.19.0/cosmic_text/struct.Buffer.html), [LayoutRun](https://docs.rs/cosmic-text/0.19.0/cosmic_text/struct.LayoutRun.html#method.highlight), [Editor](https://docs.rs/cosmic-text/0.19.0/cosmic_text/struct.Editor.html), 0.19.0, accessed 2026-11-24, Direct.
- Swash can rasterize outlines, layered color outlines, and embedded bitmaps. It exposes separate controls for fractional offsets and subpixel rasterization format. Skrifa exposes scaled and optionally hinted outlines, color-glyph and bitmap data, and hinting targets. Those capabilities cover different stages of rendering. COSMIC Text’s raster cache key includes font and glyph identifiers, font size, and x/y subpixel bins. [Swash scaling and rendering](https://docs.rs/swash/0.2.10/swash/scale/index.html), 0.2.10, [Skrifa crate docs](https://docs.rs/skrifa/0.50.0/skrifa/), 0.50.0, [COSMIC Text CacheKey](https://docs.rs/cosmic-text/0.19.0/cosmic_text/struct.CacheKey.html), 0.19.0, accessed 2026-11-24, Direct.
- Core Text exposes fonts, typeset lines, glyph runs, and Quartz drawing. DirectWrite exposes system font and text layout services and documents ClearType and fractional positioning. Pango is a paragraph engine with itemization, shaping, wrapping, and logical-position-to-glyph-position conversion. GPUI’s fetched source shows Core Text on Apple platforms and DirectWrite on Windows. Its WGPU backend contains `CosmicTextSystem`, so the available GPUI evidence points to COSMIC Text for that backend. Pango is a separate comparison point. [Apple Core Text guide](https://developer.apple.com/library/archive/documentation/StringsTextFonts/Conceptual/CoreText_Programming/Overview/Overview.html), archive, [Microsoft DirectWrite overview](https://learn.microsoft.com/en-us/windows/win32/directwrite/introducing-directwrite), live docs, [Pango Layout](https://docs.gtk.org/Pango/class.Layout.html), 1.58.2, [GPUI Apple text system](https://github.com/zed-industries/zed/blob/main/crates/gpui_apple/src/text_system.rs), [GPUI Windows text system](https://github.com/zed-industries/zed/blob/main/crates/gpui_windows/src/direct_write.rs), [GPUI WGPU text system](https://github.com/zed-industries/zed/blob/main/crates/gpui_wgpu/src/cosmic_text_system.rs), `main` snapshots, accessed 2026-11-24, Direct.

### Inferences

- Shared shaping code can reduce one source of cross-platform variation. Identical wrapping and glyph images would also depend on which concrete fonts and fallback fonts are selected, how text is segmented, and how glyphs are hinted and rasterized. This follows from the Fontique, COSMIC Text, and Swash APIs.
- A GUI editor needs a contract above the paragraph engine for document mutations, undo, IME, and accessibility. Use the layout engine’s cluster and caret mapping for hit-testing and selection. A logical selection can produce multiple visual spans on one line, as reflected in COSMIC Text’s `highlight` iterator and Parley’s bidi-aware caret API.

### Gaps

- I found no equivalent cross-platform test of complex scripts, mixed-direction caret and selection behavior, IME, or color emoji across these options.
- The Parley release’s public editor rustdoc was inaccessible. The changelog establishes historical `PlainEditor` capabilities, while the open pull request establishes the status of the proposed `RichEditor`. I did not verify a complete released rich-document editor API in either library.
- I did not verify per-backend coverage of individual color font formats, LCD antialiasing under transparency and transforms, or the exact GPUI rasterization paths in every shipping build.

## How do layout and text measurement couple together?

### Takeaway

A text leaf needs intrinsic width information and a height measured at its actual wrapping width. Cache shaping independently of line placement so intrinsic probes and width changes can reuse shaped runs. Container geometry can still change when a leaf’s measured size changes.

### Cited findings

- Taffy 0.16.0’s `compute_layout_with_measure` takes `FnMut(LayoutInput, NodeId, Option<&mut NodeContext>, &Style) -> LayoutOutput`. Its example delegates to `compute_leaf_layout`, whose inner measurement receives known dimensions and available space. `AvailableSpace` distinguishes `Definite`, `MinContent`, and `MaxContent`. A CSS adapter can therefore distinguish an intrinsic query from a measurement at a concrete width. [TaffyTree measurement API](https://docs.rs/taffy/latest/taffy/struct.TaffyTree.html#method.compute_layout_with_measure), [AvailableSpace](https://docs.rs/taffy/latest/taffy/style/enum.AvailableSpace.html), 0.16.0, accessed 2026-11-24, Direct.
- Parley says `Layout` “supports re-linebreaking and re-aligning many times.” Its docs require rebuilding the layout when text or applied styles change. `calculate_content_widths` recalculates on every call and documents possible incorrect results for mixed-direction text. [Parley crate docs](https://docs.rs/parley/0.13.0/parley/), [Layout::calculate_content_widths](https://docs.rs/parley/0.13.0/parley/layout/struct.Layout.html#method.calculate_content_widths), 0.13.0, accessed 2026-11-24, Direct.
- COSMIC Text caches shaping and layout on `BufferLine`. Changing buffer width sets `RELAYOUT`, and the dirty handler can reset line layout independently of shaping. `shape_until_scroll` resolves pending dirty flags and lays out visible lines. [BufferLine](https://docs.rs/cosmic-text/0.19.0/cosmic_text/struct.BufferLine.html), [COSMIC Text buffer source](https://docs.rs/crate/cosmic-text/0.19.0/source/src/buffer.rs), 0.19.0, accessed 2026-11-24, Direct.
- Flutter reuses a clean child’s layout when it gets the same constraints. Its architecture documentation uses `RenderParagraph` as an example of a text leaf that can reuse layout under stable constraints. Its `IntrinsicHeight` API also documents that a speculative pass can produce layout “O(N²) in the depth of the tree.” [Inside Flutter](https://docs.flutter.dev/resources/inside-flutter), [IntrinsicHeight](https://api.flutter.dev/flutter/widgets/IntrinsicHeight-class.html), live docs, accessed 2026-11-24, Direct.

### Inferences

- Cache shaped text by text revision and the inputs that can change shaping or font fallback, such as fonts, size, features, and language. Cache intrinsic answers and line layout separately using the relevant width, wrap configuration, and layout inputs. Use the final width’s line layout for drawing, hit-testing, and selection. This follows from Parley’s rebreaking API, COSMIC Text’s separate line caches, and Taffy’s measurement inputs.
- Batch reactive edits before layout. Mark the affected text leaf dirty when a measurement input changes, reuse unchanged children under equivalent constraints, and route changes that only affect painting to painting when the renderer can apply them independently. Repeated synchronous root layout for each mutation can multiply traversal work. An edit that changes a grid track or flex distribution can legitimately trigger measurements in other nodes. This follows from Taffy’s ancestor invalidation and Flutter’s constraint reuse.
- Count container visits, leaf measurements, shaping operations, and line-break operations separately when testing update cost. That would distinguish repeated tree traversal from repeated work inside a measured leaf.

### Gaps

- I found no end-to-end complexity guarantee for Taffy or Morphorm across arbitrary nested layout and reactive writes. Flutter’s quadratic example describes its speculative intrinsic sizing path.
- The available sources give API behavior and implementation details. They do not establish the cache hit rate or frame cost for any proposed Rust GUI workload.

## Leads

- Check the selected GPU renderer’s coverage for color vector glyphs, embedded bitmap strikes, fractional placement, and antialiasing, including on transparent surfaces. Swash’s raster output and Skrifa’s font data leave different work to the renderer. [Swash rendering](https://docs.rs/swash/0.2.10/swash/scale/index.html), [Skrifa](https://docs.rs/skrifa/0.50.0/skrifa/).
- For editing, verify native IME range conventions and accessibility text geometry against the same layout used for rendering. Mixed bidi selection, ligatures, emoji sequences, and a width change during composition make concrete test cases. [Parley caret API](https://docs.rs/parley/0.13.0/parley/layout/struct.Layout.html#method.primary_caret), [COSMIC Text selection spans](https://docs.rs/cosmic-text/0.19.0/cosmic_text/struct.LayoutRun.html#method.highlight).