import { lazy, Suspense, type ReactNode } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import { Shell } from '@/components/layout/Shell';
import { Skeleton } from '@/components/ui/skeleton';
import { ROUTES } from '@/config/routes';

// Lazy-loaded pages for code splitting
const DashboardPage = lazy(() => import('@/features/dashboard/page'));
const ExplorerOverviewPage = lazy(() => import('@/features/explorer/overview/page'));
const ExplorerTopicsPage = lazy(() => import('@/features/explorer/topics/page'));
const ExplorerTopicDetailPage = lazy(() => import('@/features/explorer/topics/detail'));
const ExplorerPartitionDetailPage = lazy(() => import('@/features/explorer/partitions/detail'));
const ExplorerConsumersPage = lazy(() => import('@/features/explorer/consumers/page'));
const ExplorerConsumerDetailPage = lazy(() => import('@/features/explorer/consumers/detail'));
const ExplorerDLQPage = lazy(() => import('@/features/explorer/dlq/page'));
const SourcesPage = lazy(() => import('@/features/manager/sources/page'));
const SinksPage = lazy(() => import('@/features/manager/sinks/page'));
const FlowsPage = lazy(() => import('@/features/manager/flows/page'));
const FlowDetailPage = lazy(() => import('@/features/manager/flows/detail'));

/** Page loading fallback. */
function PageLoader() {
  return (
    <div className="space-y-4 p-6">
      <Skeleton className="h-8 w-48" />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-[120px]" />
        ))}
      </div>
      <Skeleton className="h-[200px]" />
    </div>
  );
}

/** Root App component — providers + routing. */
export default function App() {
  return (
    <Routes>
      <Route element={<Shell />}>
        <Route index element={page(<DashboardPage />)} />
        <Route path={ROUTES.EXPLORER} element={page(<ExplorerOverviewPage />)} />
        <Route path={ROUTES.EXPLORER_TOPICS} element={page(<ExplorerTopicsPage />)} />
        <Route path={ROUTES.EXPLORER_TOPIC_DETAIL} element={page(<ExplorerTopicDetailPage />)} />
        <Route path={ROUTES.EXPLORER_TOPIC_PARTITION} element={page(<ExplorerPartitionDetailPage />)} />
        <Route path={ROUTES.EXPLORER_CONSUMERS} element={page(<ExplorerConsumersPage />)} />
        <Route path={ROUTES.EXPLORER_CONSUMER_DETAIL} element={page(<ExplorerConsumerDetailPage />)} />
        <Route path={ROUTES.EXPLORER_DLQ} element={page(<ExplorerDLQPage />)} />
        <Route path={ROUTES.MANAGER} element={<Navigate to={ROUTES.MANAGER_SOURCES} replace />} />
        <Route path={ROUTES.MANAGER_SOURCES} element={page(<SourcesPage />)} />
        <Route path={ROUTES.MANAGER_SINKS} element={page(<SinksPage />)} />
        <Route path={ROUTES.MANAGER_FLOWS} element={page(<FlowsPage />)} />
        <Route path={ROUTES.MANAGER_FLOW_DETAIL} element={page(<FlowDetailPage />)} />
      </Route>
    </Routes>
  );
}

function page(children: ReactNode) {
  return <Suspense fallback={<PageLoader />}>{children}</Suspense>;
}
