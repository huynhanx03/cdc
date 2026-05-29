export const LANGUAGE_STORAGE_KEY = 'cdc-language';

export const SUPPORTED_LANGUAGES = [
  { code: 'en', labelKey: 'language.en', nativeLabel: 'English', htmlLang: 'en' },
  { code: 'vi', labelKey: 'language.vi', nativeLabel: 'Tiếng Việt', htmlLang: 'vi' },
  { code: 'zh', labelKey: 'language.zh', nativeLabel: '中文', htmlLang: 'zh-CN' },
] as const;

export type LanguageCode = (typeof SUPPORTED_LANGUAGES)[number]['code'];

export const DEFAULT_LANGUAGE: LanguageCode = 'en';

export function isLanguageCode(value: string): value is LanguageCode {
  return SUPPORTED_LANGUAGES.some((language) => language.code === value);
}

export function htmlLangFor(code: string): string {
  return SUPPORTED_LANGUAGES.find((language) => language.code === code)?.htmlLang ?? DEFAULT_LANGUAGE;
}
