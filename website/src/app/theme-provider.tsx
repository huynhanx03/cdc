import { useEffect, type ReactNode } from 'react';

import { type ResolvedTheme, type ThemeMode } from '@/config/theme';
import { useThemeStore } from '@/stores/theme';

const SYSTEM_THEME_QUERY = '(prefers-color-scheme: dark)';

export function ThemeProvider({ children }: { children: ReactNode }) {
  const theme = useThemeStore((state) => state.theme);

  useEffect(() => {
    applyTheme(theme);
    if (theme !== 'system') return undefined;

    const mediaQuery = window.matchMedia(SYSTEM_THEME_QUERY);
    const onChange = () => applyTheme('system');
    mediaQuery.addEventListener('change', onChange);
    return () => mediaQuery.removeEventListener('change', onChange);
  }, [theme]);

  return children;
}

function applyTheme(theme: ThemeMode): void {
  const resolved = resolveTheme(theme);
  document.documentElement.classList.toggle('dark', resolved === 'dark');
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = resolved;
}

function resolveTheme(theme: ThemeMode): ResolvedTheme {
  if (theme !== 'system') return theme;
  return window.matchMedia(SYSTEM_THEME_QUERY).matches ? 'dark' : 'light';
}
