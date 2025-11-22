"use client";
import { useEffect, useState } from "react";
import { useLayout } from "@/contexts/LayoutContext";
import Sidebar from "@/components/sidemenu/Sidebar";
import TopMenu from "@/components/topmenu/TopMenu";
import MusicPlayer from "@/components/footer/MusicPlayer";

export default function Layout({ children }: { children: React.ReactNode }) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  const { audioRef } = useLayout();

  if (!mounted) return null; 

  return (
    <div className="layout-shell">
      <div className="layout-body">
        <aside className="layout-sidebar-left"><Sidebar /></aside>
        <main className="layout-content ps-3" style={{ overflowX:'hidden',overflowY:'auto' }}>
          <TopMenu />
          {children}
        </main>
      </div>
      <footer className="layout-footer p-1 melo-flex-between melo-footer">
        <MusicPlayer ref={audioRef} />
      </footer>
    </div>
  );
}
