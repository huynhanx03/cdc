import { Breadcrumb } from './Breadcrumb';
import { LanguageSwitcher } from './LanguageSwitcher';
import { ThemeSwitcher } from './ThemeSwitcher';

/** Top bar — breadcrumb + theme toggle + language selector. */
export function TopBar() {
  return (
    <header className="sticky top-0 z-20 flex h-14 items-center justify-between border-b border-border bg-card/80 px-6 backdrop-blur-md">
      <Breadcrumb />
      <div className="flex items-center gap-1">
        <ThemeSwitcher />
        <LanguageSwitcher />
      </div>
    </header>
  );
}
