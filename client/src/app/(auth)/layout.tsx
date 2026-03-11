import type { Metadata } from "next";
import "../globals.css";

export const metadata: Metadata = {
  title: "Login — CDC Platform",
};

export default function AuthLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body
        style={{
          minHeight: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          background: "#0d1117",
          fontFamily: "'Inter', -apple-system, sans-serif",
        }}
      >
        {children}
      </body>
    </html>
  );
}
