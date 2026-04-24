export default function Loading() {
  return (
    <div className="space-y-6 animate-pulse">
      {/* Back link skeleton */}
      <div className="h-4 w-32 bg-slate-200 rounded" />

      {/* Main card skeleton */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 space-y-4">
        <div className="h-6 w-1/2 bg-slate-200 rounded" />
        <div className="grid grid-cols-2 gap-4 mt-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="h-14 bg-slate-100 rounded-lg" />
          ))}
        </div>
      </div>

      {/* Chart skeleton */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
        <div className="h-5 w-48 bg-slate-200 rounded mb-4" />
        <div className="h-48 bg-slate-100 rounded-lg" />
      </div>
    </div>
  );
}
