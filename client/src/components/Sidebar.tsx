"use client";

import { usePathname, useRouter } from "next/navigation";

const navItems = [
  { label: "Dashboard", href: "/", icon: "◉" },
  { label: "Topics", href: "/topics", icon: "▤" },
  { label: "Partitions", href: "/partitions", icon: "⊞" },
  { label: "Messages", href: "/messages", icon: "≡" },
  { label: "System Config", href: "/config", icon: "⚙" },
];

export default function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <span className="logo-icon">⟁</span>
        CDC Engine
      </div>

      <ul className="nav-links">
        {navItems.map((item) => {
          const isActive =
            item.href === "/"
              ? pathname === "/"
              : pathname.startsWith(item.href);
          return (
            <li
              key={item.href}
              className={`nav-item ${isActive ? "active" : ""}`}
              onClick={() => router.push(item.href)}
            >
              <span className="nav-icon">{item.icon}</span>
              {item.label}
            </li>
          );
        })}
      </ul>

      <div className="sidebar-footer">
        <div className="health-dot" id="health-indicator" />
        <span className="health-text">Connected</span>
      </div>
    </aside>
  );
}
