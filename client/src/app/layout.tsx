import type { Metadata } from "next";
import "./globals.css";
import Sidebar from "@/components/Sidebar";
import TopBar from "@/components/TopBar";

export const metadata: Metadata = {
  title: "CDC UI Platform",
  description: "Track topics, partitions, and change data capture flows.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <div className="app-container">
          <Sidebar />
          <div className="main-wrapper">
            <TopBar />
            <main className="main-content">{children}</main>
          </div>
        </div>
      </body>
    </html>
  );
}
