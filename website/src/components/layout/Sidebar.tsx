import { NavLink, useLocation } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { ChevronDown, ChevronLeft, ChevronRight, Zap } from 'lucide-react';
import { useState } from 'react';
import { useSidebarStore } from '@/stores/sidebar';
import { cn } from '@/lib/utils';
import { NAV_ITEMS } from '@/config/navigation';
import { ROUTES } from '@/config/routes';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

/** Sidebar navigation — collapsible, dark-themed, with active indicators. */
export function Sidebar() {
  const { t } = useTranslation();
  const collapsed = useSidebarStore((s) => s.collapsed);
  const toggle = useSidebarStore((s) => s.toggle);
  const location = useLocation();
  const [openGroups, setOpenGroups] = useState<Record<string, boolean>>({
    explorer: location.pathname.startsWith('/explorer'),
    manager: location.pathname.startsWith('/manager'),
  });

  const toggleGroup = (key: string) => {
    setOpenGroups((current) => ({ ...current, [key]: !current[key] }));
  };

  return (
    <aside
      className={cn(
        'fixed inset-y-0 left-0 z-30 flex flex-col border-r border-border bg-card transition-all duration-300',
        collapsed ? 'w-16' : 'w-64',
      )}
    >
      {/* Logo */}
      <div className="flex h-14 items-center gap-3 border-b border-border px-4">
        <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-primary">
          <Zap className="h-4 w-4 text-primary-foreground" />
        </div>
        {!collapsed && (
          <span className="text-base font-semibold tracking-tight text-foreground">
            CDC Engine
          </span>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-1 overflow-y-auto px-2 py-3">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon;

          // Items with children
          if (item.children) {
            const isActive = location.pathname.startsWith(`/${item.key}`);
            const isOpen = openGroups[item.key] ?? false;
            return (
              <div key={item.key}>
                <button
                  onClick={() => toggleGroup(item.key)}
                  className={cn(
                    'flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors cursor-pointer',
                    isActive
                      ? 'bg-accent text-accent-foreground'
                      : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground',
                  )}
                >
                  {collapsed ? (
                    <Tooltip>
                      <TooltipTrigger render={<Icon className="h-5 w-5 shrink-0" />} />
                      <TooltipContent side="right">
                        {t(item.labelKey)}
                      </TooltipContent>
                    </Tooltip>
                  ) : (
                    <>
                      <Icon className="h-5 w-5 shrink-0" />
                      <span className="flex-1 text-left">{t(item.labelKey)}</span>
                      <ChevronDown
                        className={cn(
                          'h-4 w-4 transition-transform',
                          isOpen && 'rotate-180',
                        )}
                      />
                    </>
                  )}
                </button>

                {/* Sub items */}
                {isOpen && !collapsed && (
                  <div className="ml-4 mt-1 space-y-0.5 border-l border-border pl-3">
                    {item.children.map((child) => {
                      const SubIcon = child.icon;
                      return (
                        <NavLink
                          key={child.key}
                          to={child.path}
                          className={({ isActive }) =>
                            cn(
                              'flex items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors cursor-pointer',
                              isActive
                                ? 'bg-accent text-accent-foreground font-medium'
                                : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground',
                            )
                          }
                        >
                          <SubIcon className="h-4 w-4 shrink-0" />
                          <span>{t(child.labelKey)}</span>
                        </NavLink>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          }

          // Simple nav item
          return (
            <NavLink
              key={item.key}
              to={item.path}
              end={item.path === ROUTES.DASHBOARD}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors cursor-pointer',
                  isActive
                    ? 'bg-accent text-accent-foreground'
                    : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground',
                )
              }
            >
              {collapsed ? (
                <Tooltip>
                  <TooltipTrigger render={<Icon className="h-5 w-5 shrink-0" />} />
                  <TooltipContent side="right">
                    {t(item.labelKey)}
                  </TooltipContent>
                </Tooltip>
              ) : (
                <>
                  <Icon className="h-5 w-5 shrink-0" />
                  <span>{t(item.labelKey)}</span>
                </>
              )}
            </NavLink>
          );
        })}
      </nav>

      {/* Collapse toggle */}
      <div className="border-t border-border p-2">
        <button
          onClick={toggle}
          className="flex w-full items-center justify-center rounded-lg p-2 text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground cursor-pointer"
          aria-label={t('nav.toggleSidebar')}
        >
          {collapsed ? (
            <ChevronRight className="h-5 w-5" />
          ) : (
            <ChevronLeft className="h-5 w-5" />
          )}
        </button>
      </div>
    </aside>
  );
}
