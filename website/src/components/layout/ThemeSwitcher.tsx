import { Check, Palette } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { THEME_OPTIONS } from '@/config/theme';
import { useThemeStore } from '@/stores/theme';

export function ThemeSwitcher() {
  const { t } = useTranslation();
  const theme = useThemeStore((state) => state.theme);
  const setTheme = useThemeStore((state) => state.setTheme);
  const active = THEME_OPTIONS.find((option) => option.mode === theme) ?? THEME_OPTIONS[0];
  const ActiveIcon = active.icon;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={
          <Button variant="ghost" size="icon" className="h-9 w-9 cursor-pointer" aria-label={t('theme.label')}>
            <ActiveIcon className="h-4 w-4" />
          </Button>
        }
      />
      <DropdownMenuContent align="end">
        <div className="flex items-center gap-2 px-1.5 py-1 text-xs font-medium text-muted-foreground">
          <Palette className="h-3.5 w-3.5" />
          {t('theme.label')}
        </div>
        {THEME_OPTIONS.map((option) => {
          const Icon = option.icon;
          return (
            <DropdownMenuItem
              key={option.mode}
              onClick={() => setTheme(option.mode)}
              className="cursor-pointer justify-between"
            >
              <span className="flex items-center gap-2">
                <Icon className="h-4 w-4" />
                {t(option.labelKey)}
              </span>
              {theme === option.mode ? <Check className="h-4 w-4" /> : null}
            </DropdownMenuItem>
          );
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
