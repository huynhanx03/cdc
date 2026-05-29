# Client Refactor And Benchmark Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove Docker-backed pipeline benchmarks and refactor the React client into explicit reusable language, theme, navigation, provider, and shared UI modules.

**Architecture:** Keep the current app routes and feature behavior, but move app-wide decisions into small declarative modules. Reuse existing shadcn-style components and create small shared primitives for repeated page/table/metric patterns.

**Tech Stack:** Go benchmarks, React 19, Vite, TanStack Query, i18next, Zustand, Tailwind, shadcn-style UI, lucide-react.

---

## Task 1: Remove Docker-Backed Benchmarks

**Files:**
- Delete: `benchmarks/pipeline/nats_bench_test.go`
- Delete: `benchmarks/pipeline/sink_bench_test.go`
- Delete: `benchmarks/pipeline/e2e_freshness_bench_test.go`

- [x] Delete benchmark files that require Docker or only skip Docker-backed work.
- [x] Run `make bench-pipeline`.
- [x] Confirm output only includes source decode and transform benchmarks.

## Task 2: Extract Client Providers

**Files:**
- Create: `website/src/app/query-client.ts`
- Create: `website/src/app/providers.tsx`
- Modify: `website/src/App.tsx`
- Modify: `website/src/main.tsx`

- [x] Move QueryClient creation to `app/query-client.ts`.
- [x] Add `AppProviders` with query, tooltip, router, and toaster.
- [x] Wrap `App` from `main.tsx` with `AppProviders`.
- [x] Remove provider construction from `App.tsx`.

## Task 3: Refactor Theme And Language

**Files:**
- Create: `website/src/config/theme.ts`
- Create: `website/src/config/language.ts`
- Create: `website/src/components/layout/ThemeSwitcher.tsx`
- Create: `website/src/components/layout/LanguageSwitcher.tsx`
- Modify: `website/src/stores/theme.ts`
- Modify: `website/src/lib/i18n/index.ts`
- Modify: `website/src/components/layout/TopBar.tsx`

- [x] Declare supported languages and theme modes in config files.
- [x] Update i18n init from declared language config and update document language.
- [x] Update theme store to resolve system mode and react to OS theme changes.
- [x] Replace inline dropdowns in TopBar with reusable switcher components.

## Task 4: Refactor Navigation And Shared UI

**Files:**
- Create: `website/src/config/navigation.ts`
- Create: `website/src/components/shared/PageHeader.tsx`
- Create: `website/src/components/shared/MetricTile.tsx`
- Create: `website/src/components/shared/TableState.tsx`
- Create: `website/src/config/status.ts`
- Modify: `website/src/config/routes.ts`
- Modify: `website/src/components/layout/Sidebar.tsx`
- Modify: `website/src/components/layout/Breadcrumb.tsx`
- Modify: `website/src/components/shared/StatusBadge.tsx`

- [x] Move nav items to `config/navigation.ts` with actual lucide icon components.
- [x] Keep `routes.ts` as route path constants only.
- [x] Make Sidebar render declared nav entries without icon string maps.
- [x] Add reusable page/table/metric primitives.
- [x] Move status labels/classes to `config/status.ts`.

## Task 5: Apply Client Cleanup Across Pages

**Files:**
- Modify Explorer pages and components under `website/src/features/explorer/**`
- Modify Manager flow/source/sink surfaces touched by lint and visible hardcoded strings
- Modify locale files under `website/src/lib/i18n/locales/*.json`

- [x] Replace repeated page headers, metric cards, loading rows, and empty rows in Explorer pages.
- [x] Replace hardcoded Explorer labels with translation keys.
- [x] Replace Manager form helper copy and flow status labels with translation/config helpers.
- [x] Keep connector product names literal.

## Task 6: Verify And Cleanup

**Files:**
- No source edits unless verification fails.

- [x] Run `PATH=/tmp/codex-node/node-v24.14.0-darwin-arm64/bin:$PATH npm ci` if `website/node_modules` is missing.
- [x] Run `PATH=/tmp/codex-node/node-v24.14.0-darwin-arm64/bin:$PATH npm run lint` in `website`.
- [x] Run `PATH=/tmp/codex-node/node-v24.14.0-darwin-arm64/bin:$PATH npm run build` in `website`.
- [x] Remove `website/node_modules` and `website/dist`.
- [x] Run `make test-unit`.
- [x] Run `make bench-pipeline`.
- [x] Run `git diff --check`.
