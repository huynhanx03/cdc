"use client";

import { usePathname, useRouter } from "next/navigation";
import { 
  LayoutDashboard, 
  Layers, 
  Grid2X2, 
  MessageSquare, 
  Settings, 
  Zap,
  Activity,
  Box
} from "lucide-react";

interface NavItem {
  label: string;
  href: string;
  icon: React.ElementType;
}

const navItems: NavItem[] = [
  { label: "Dashboard", href: "/", icon: LayoutDashboard },
  { label: "Topics", href: "/topics", icon: Layers },
  { label: "Partitions", href: "/partitions", icon: Grid2X2 },
  { label: "Messages", href: "/messages", icon: MessageSquare },
  { label: "System Config", href: "/config", icon: Settings },
];

export default function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();

  return (
    <aside className="sidebar flex flex-col p-6 h-full border-r border-white/5 bg-black/40 backdrop-blur-xl">
      {/* Brand */}
      <div className="flex items-center gap-3 mb-10 px-2 cursor-pointer" onClick={() => router.push("/")}>
        <div className="p-2 rounded-xl bg-gradient-to-tr from-blue-600 to-emerald-400">
          <Zap className="w-5 h-5 text-white fill-white" />
        </div>
        <div>
          <h1 className="text-xl font-bold tracking-tight text-white leading-none">CDC Engine</h1>
          <p className="text-[10px] uppercase tracking-widest text-slate-500 font-bold mt-1">Next Generation</p>
        </div>
      </div>

      {/* Primary Navigation */}
      <nav className="flex-1 space-y-2">
        <div className="text-[10px] uppercase tracking-widest text-slate-500 font-bold mb-4 px-2">Navigation</div>
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive =
            item.href === "/"
              ? pathname === "/"
              : pathname.startsWith(item.href);
          
          return (
            <button
              key={item.href}
              onClick={() => router.push(item.href)}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-all group ${
                isActive 
                  ? "bg-blue-500/10 text-blue-400 border border-blue-500/20 shadow-[0_0_20px_rgba(59,130,246,0.1)]" 
                  : "text-slate-400 hover:text-white hover:bg-white/5"
              }`}
            >
              <Icon className={`w-4 h-4 transition-transform group-hover:scale-110 ${isActive ? "text-blue-400" : "text-slate-500"}`} />
              {item.label}
            </button>
          );
        })}
      </nav>

      {/* Footer Info */}
      <div className="mt-auto pt-6 border-t border-white/5">
        <div className="flex items-center gap-3 p-3 rounded-xl bg-white/[0.02] border border-white/5">
          <div className="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.5)]" />
          <div className="flex-1 min-width-0">
            <p className="text-xs font-bold text-white truncate">Operational</p>
            <p className="text-[10px] text-slate-500 uppercase tracking-tighter">Everything normal</p>
          </div>
          <Activity className="w-3.5 h-3.5 text-slate-600" />
        </div>
      </div>
    </aside>
  );
}
