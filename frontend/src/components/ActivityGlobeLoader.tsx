"use client";

import nextDynamic from "next/dynamic";

const ActivityGlobe = nextDynamic(() => import("@/components/ActivityGlobe"), {
  ssr: false,
  loading: () => (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-6 animate-pulse">
      <div className="h-4 w-44 bg-slate-200 rounded mb-4" />
      <div className="h-[420px] rounded-2xl bg-slate-100" />
    </div>
  ),
});

export default function ActivityGlobeLoader() {
  return <ActivityGlobe />;
}
