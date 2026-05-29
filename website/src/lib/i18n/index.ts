import i18n from 'i18next';
import type { Resource } from 'i18next';
import { initReactI18next } from 'react-i18next';
import LanguageDetector from 'i18next-browser-languagedetector';

import {
  DEFAULT_LANGUAGE,
  LANGUAGE_STORAGE_KEY,
  SUPPORTED_LANGUAGES,
  htmlLangFor,
} from '@/config/language';
import en from './locales/en.json';
import vi from './locales/vi.json';
import zh from './locales/zh.json';

const resources = {
  en: { translation: en },
  vi: { translation: vi },
  zh: { translation: zh },
} satisfies Resource;

i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    resources,
    supportedLngs: SUPPORTED_LANGUAGES.map((language) => language.code),
    fallbackLng: DEFAULT_LANGUAGE,
    interpolation: {
      escapeValue: false,
    },
    detection: {
      order: ['localStorage', 'navigator'],
      caches: ['localStorage'],
      lookupLocalStorage: LANGUAGE_STORAGE_KEY,
    },
  });

i18n.on('languageChanged', (language) => {
  document.documentElement.lang = htmlLangFor(language);
});

document.documentElement.lang = htmlLangFor(i18n.resolvedLanguage ?? i18n.language);

export default i18n;
