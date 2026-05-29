import { Check, Globe } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES, isLanguageCode } from '@/config/language';

export function LanguageSwitcher() {
  const { t, i18n } = useTranslation();
  const resolvedLanguage = i18n.resolvedLanguage ?? i18n.language;
  const activeLanguage = isLanguageCode(resolvedLanguage) ? resolvedLanguage : DEFAULT_LANGUAGE;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={
          <Button variant="ghost" size="icon" className="h-9 w-9 cursor-pointer" aria-label={t('language.label')}>
            <Globe className="h-4 w-4" />
          </Button>
        }
      />
      <DropdownMenuContent align="end">
        <div className="flex items-center gap-2 px-1.5 py-1 text-xs font-medium text-muted-foreground">
          <Globe className="h-3.5 w-3.5" />
          {t('language.label')}
        </div>
        {SUPPORTED_LANGUAGES.map((language) => (
          <DropdownMenuItem
            key={language.code}
            onClick={() => i18n.changeLanguage(language.code)}
            className="cursor-pointer justify-between"
          >
            <span>
              {t(language.labelKey)}
              <span className="ml-2 text-xs text-muted-foreground">{language.nativeLabel}</span>
            </span>
            {activeLanguage === language.code ? <Check className="h-4 w-4" /> : null}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
