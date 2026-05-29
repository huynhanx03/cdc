import { Monitor, Moon, Sun, type LucideIcon } from 'lucide-react';

export const THEME_STORAGE_KEY = 'cdc-theme';

export const THEME_MODES = ['light', 'dark', 'system'] as const;

export type ThemeMode = (typeof THEME_MODES)[number];

export type ResolvedTheme = Exclude<ThemeMode, 'system'>;

export interface ThemeOption {
  mode: ThemeMode;
  labelKey: string;
  icon: LucideIcon;
}

export const THEME_OPTIONS: ThemeOption[] = [
  { mode: 'light', labelKey: 'theme.light', icon: Sun },
  { mode: 'dark', labelKey: 'theme.dark', icon: Moon },
  { mode: 'system', labelKey: 'theme.system', icon: Monitor },
];

export const DEFAULT_THEME: ThemeMode = 'system';

export function isThemeMode(value: string): value is ThemeMode {
  return THEME_MODES.includes(value as ThemeMode);
}
