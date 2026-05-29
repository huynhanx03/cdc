# Client Refactor And Benchmark Cleanup Design

Date: 2026-05-29

## Goal

Refactor the React client so language, theme, navigation, formatting, status labels, and common layout primitives are explicit reusable modules instead of component-local magic. Remove real Docker-backed pipeline benchmarks from the benchmark suite so `bench-pipeline` stays local, fast, and stable.

## Decisions

- Do not keep Docker-backed benchmark code in `benchmarks/pipeline`. Docker-backed coverage remains integration-test territory, not benchmark territory.
- Keep the client on the existing Vite, React, TanStack Query, Zustand, i18next, Tailwind, and shadcn-style component stack.
- Use small config modules for choices that appear across the app: supported languages, theme modes, nav items, flow status metadata, connector labels, and Explorer labels.
- Keep routes and feature pages intact; refactor around them instead of rebuilding product flows.
- Treat missing translations as defects for first-party UI added in this pass. API values, connector product names, subject names, and enum codes remain literal.
- Support three theme modes: light, dark, and system. Theme resolution must update when the OS color scheme changes in system mode.
- Prefer reusable shadcn-backed primitives over bespoke repeated markup: page header, metric grid item, empty table row, loading rows, toolbar selectors, status badge.

## Architecture

### Client Providers

Create a provider layer that owns app-wide dependencies:

- `AppProviders`: TanStack Query, tooltip provider, browser router, toast.
- `queryClient`: a single query client module with declared defaults.
- `ThemeProvider`: applies the resolved theme class to the document root and keeps system mode in sync with media-query changes.

`App.tsx` should become mostly route declarations and should not construct providers or query clients inline.

### Language

Language support becomes declarative:

- `SUPPORTED_LANGUAGES` defines language code, i18n label key, native label, and document language tag.
- `LanguageSwitcher` renders from that list.
- i18n resources are built from the same declared language list.
- HTML `lang` is updated when language changes.

### Theme

Theme support becomes declarative:

- `THEME_OPTIONS` defines mode, label key, and icon.
- `ThemeSwitcher` renders from that list.
- Theme persistence remains `cdc-theme`.
- System theme listens to `prefers-color-scheme`.

### Navigation

Navigation config should avoid string icon lookups. `NAV_ITEMS` should use actual icon components. `Sidebar` becomes rendering logic only.

### Shared UI

Introduce reusable app primitives:

- `PageHeader` for page titles, descriptions, back action, and right actions.
- `MetricTile` for compact operational metrics outside dashboard cards.
- `EmptyTableRow` and `LoadingTableRows` for repeated table states.
- `StatusBadge` config-based labels/classes.

Feature pages should import these rather than repeat border/card/table markup and hardcoded copy.

### i18n Cleanup

Move hardcoded user-facing strings from Explorer and touched Manager forms into locale files. Keep table data, route params, connector names, NATS subjects, and raw API messages literal.

### Benchmark Cleanup

Delete the Docker-backed NATS benchmark file and remove Docker-backed benchmark placeholders from pipeline benchmarks. The remaining `make bench-pipeline` should measure local CPU-bound source decode and transform stages only.

## Validation

Required commands:

- `make bench-pipeline`
- `make test-unit`
- `cd website && npm run lint`
- `cd website && npm run build`
- `git diff --check`

Frontend dependency installation may be temporary. Generated `website/node_modules` and `website/dist` must not be left behind after verification.
