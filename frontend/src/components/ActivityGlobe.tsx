"use client";

import { useEffect, useRef, useState, type ComponentType } from "react";
import GlobeBase from "react-globe.gl";
import { fetchGeoHeatmap, type GeoHeatmapPoint } from "@/lib/api";

const Globe = GlobeBase as unknown as ComponentType<any>;

const TIMEFRAMES = [
  { label: "24h",    hours: 24  },
  { label: "1 week", hours: 168 },
  { label: "1 month",hours: 720 },
] as const;

type Hours = typeof TIMEFRAMES[number]["hours"];

function checkWebGL(): boolean {
  try {
    const canvas = document.createElement("canvas");
    return !!(canvas.getContext("webgl") || canvas.getContext("experimental-webgl"));
  } catch {
    return false;
  }
}

export default function ActivityGlobe() {
  const globeRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [hours, setHours] = useState<Hours>(168);
  const [data, setData] = useState<GeoHeatmapPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [size, setSize] = useState({ width: 0, height: 0 });
  const [webglAvailable, setWebglAvailable] = useState<boolean | null>(null);

  useEffect(() => {
    setWebglAvailable(checkWebGL());
  }, []);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(false);

    fetchGeoHeatmap(hours)
      .then((points) => {
        if (!active) return;
        setData(points);
        setLoading(false);
      })
      .catch(() => {
        if (!active) return;
        setError(true);
        setLoading(false);
      });

    return () => { active = false; };
  }, [hours]);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;
    const ro = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      setSize({
        width: Math.floor(entry.contentRect.width),
        height: Math.floor(entry.contentRect.height),
      });
    });
    ro.observe(element);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    if (!globeRef.current) return;
    globeRef.current.pointOfView({ lat: 18, lng: 10, altitude: 2.2 }, 1200);
  }, [data.length]);

  return (
    <section className="bg-white border border-slate-200 rounded-xl shadow-sm p-6">
      <div className="flex items-start justify-between gap-4 mb-4">
        <div>
          <h2 className="text-base font-semibold text-slate-800">Geocoded Activity Globe</h2>
          <p className="mt-1 text-sm text-slate-500">
            Event intensity by user location, rendered as a heatmap layer.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center bg-slate-50 border border-slate-200 rounded-lg p-1">
            {TIMEFRAMES.map((tf) => (
              <button
                key={tf.hours}
                type="button"
                onClick={() => setHours(tf.hours)}
                className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${
                  hours === tf.hours
                    ? "bg-white text-slate-800 shadow-sm border border-slate-200"
                    : "text-slate-500 hover:text-slate-700"
                }`}
              >
                {tf.label}
              </button>
            ))}
          </div>
          <div className="hidden sm:flex items-center gap-2 text-xs text-slate-500">
            <span className="inline-flex h-2 w-2 rounded-full bg-cyan-400/80" />
            heat layer
          </div>
        </div>
      </div>

      <div
        ref={containerRef}
        className="relative overflow-hidden rounded-2xl border border-slate-100 bg-gradient-to-b from-slate-50 via-white to-sky-50/50 h-[420px] sm:h-[520px]"
      >
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_50%_30%,rgba(56,189,248,0.12),transparent_40%),radial-gradient(circle_at_50%_75%,rgba(14,165,233,0.06),transparent_45%)]" />

        {loading && (
          <div className="absolute inset-0 flex items-center justify-center z-10">
            <div className="h-10 w-10 rounded-full border-2 border-sky-200 border-t-sky-500 animate-spin" />
          </div>
        )}

        {error && (
          <div className="absolute inset-0 flex items-center justify-center px-6 text-center text-sm text-slate-500">
            Geo heatmap data could not be loaded.
          </div>
        )}

        <div className="relative h-full w-full">
          {webglAvailable === false && (
            <div className="absolute inset-0 flex items-center justify-center px-6 text-center text-sm text-slate-500">
              WebGL is not available in this browser — globe cannot be rendered.
            </div>
          )}
          {!error && webglAvailable === true && (
            <Globe
              ref={globeRef}
              width={size.width || 800}
              height={size.height || 420}
              globeImageUrl="//unpkg.com/three-globe/example/img/earth-blue-marble.jpg"
              backgroundColor="rgba(255,255,255,0)"
              animateIn={false}
              showAtmosphere={true}
              atmosphereColor="#7dd3fc"
              atmosphereAltitude={0.14}
              heatmapData={data}
              heatmapPointLat="lat"
              heatmapPointLng="lng"
              heatmapPointWeight="count"
              heatmapBandwidth={1.4}
              heatmapColor={["#fef9c3", "#fde047", "#f97316", "#dc2626", "#7f1d1d"]}
              heatmapIntensity={0.9}
              heatmapOpacity={0.85}
              heatmapTopResolution={256}
              heatmapAltitude={0.01}
              heatmapMargin={0.1}
              enablePointerInteraction={false}
              showGraticules={false}
            />
          )}
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between gap-4 text-xs text-slate-500">
        <span>{data.length.toLocaleString()} aggregated geo locations</span>
        <span>heatmap layer only, no discrete markers</span>
      </div>
    </section>
  );
}
