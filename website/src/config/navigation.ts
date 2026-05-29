import {
  Activity,
  Database,
  GitBranch,
  HardDrive,
  Inbox,
  Layers,
  LayoutDashboard,
  RadioTower,
  Search,
  Settings,
  type LucideIcon,
} from 'lucide-react';

import { ROUTES } from './routes';

export interface NavChildItem {
  key: string;
  labelKey: string;
  path: string;
  icon: LucideIcon;
}

export interface NavItem {
  key: string;
  labelKey: string;
  path: string;
  icon: LucideIcon;
  children?: NavChildItem[];
}

export const NAV_ITEMS: NavItem[] = [
  {
    key: 'dashboard',
    labelKey: 'nav.dashboard',
    path: ROUTES.DASHBOARD,
    icon: LayoutDashboard,
  },
  {
    key: 'explorer',
    labelKey: 'nav.explorer',
    path: ROUTES.EXPLORER_TOPICS,
    icon: Search,
    children: [
      { key: 'overview', labelKey: 'nav.overview', path: ROUTES.EXPLORER, icon: Activity },
      { key: 'topics', labelKey: 'nav.topics', path: ROUTES.EXPLORER_TOPICS, icon: Layers },
      { key: 'consumers', labelKey: 'nav.consumers', path: ROUTES.EXPLORER_CONSUMERS, icon: RadioTower },
      { key: 'dlq', labelKey: 'nav.dlq', path: ROUTES.EXPLORER_DLQ, icon: Inbox },
    ],
  },
  {
    key: 'manager',
    labelKey: 'nav.manager',
    path: ROUTES.MANAGER_SOURCES,
    icon: Settings,
    children: [
      { key: 'sources', labelKey: 'nav.sources', path: ROUTES.MANAGER_SOURCES, icon: Database },
      { key: 'sinks', labelKey: 'nav.sinks', path: ROUTES.MANAGER_SINKS, icon: HardDrive },
      { key: 'flows', labelKey: 'nav.flows', path: ROUTES.MANAGER_FLOWS, icon: GitBranch },
    ],
  },
];
