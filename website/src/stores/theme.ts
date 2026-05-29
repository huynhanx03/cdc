import { create } from 'zustand';
import { persist } from 'zustand/middleware';

import { DEFAULT_THEME, THEME_STORAGE_KEY, type ThemeMode } from '@/config/theme';

interface ThemeState {
  theme: ThemeMode;
  setTheme: (theme: ThemeMode) => void;
}

export const useThemeStore = create<ThemeState>()(
  persist(
    (set) => ({
      theme: DEFAULT_THEME,
      setTheme: (theme) => set({ theme }),
    }),
    { name: THEME_STORAGE_KEY },
  ),
);
