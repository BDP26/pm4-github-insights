"use client";

import { useEffect, useRef, useState, type ComponentType } from "react";
import GlobeBase from "react-globe.gl";
import { fetchGeoHeatmap, type GeoHeatmapPoint } from "@/lib/api";
import { fetchGlobeCityLabels, type GlobeCityLabel } from "@/lib/globeCities";

const Globe = GlobeBase as unknown as ComponentType<any>;

type Hours = number | null;

const INTERVAL_FRAMES: { label: string; hours: number }[] = [
  { label: "24h",     hours: 24  },
  { label: "1 week",  hours: 168 },
  { label: "1 month", hours: 720 },
];

function checkWebGL(): boolean {
  try {
    const canvas = document.createElement("canvas");
    return !!(canvas.getContext("webgl") || canvas.getContext("experimental-webgl"));
  } catch {
    return false;
  }
}

export default function ActivityGlobe() {
  const globeRef     = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [hours, setHours]               = useState<Hours>(null);
  const [data, setData]                 = useState<GeoHeatmapPoint[]>([]);
  const [cityLabels, setCityLabels]     = useState<GlobeCityLabel[]>([]);
  const [loading, setLoading]           = useState(true);
  const [error, setError]               = useState(false);
  const [cityLoading, setCityLoading]   = useState(false);
  const [cityError, setCityError]       = useState(false);
  const [showCities, setShowCities]     = useState(false);
  const [size, setSize]                 = useState({ width: 0, height: 0 });
  const [webglAvailable, setWebglAvailable] = useState<boolean | null>(null);

  useEffect(() => { setWebglAvailable(checkWebGL()); }, []);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(false);
    fetchGeoHeatmap(hours)
      .then((pts) => { if (active) { setData(pts); setLoading(false); } })
      .catch(() => { if (active) { setError(true); setLoading(false); } });
    return () => { active = false; };
  }, [hours]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => {
      if (!e) return;
      setSize({ width: Math.floor(e.contentRect.width), height: Math.floor(e.contentRect.height) });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    if (!globeRef.current) return;
    globeRef.current.pointOfView({ lat: 18, lng: 10, altitude: 2.2 }, 1200);
  }, [data.length]);

  useEffect(() => {
    if (!showCities) return;
    if (cityLabels.length > 0) return;

    let active = true;

    setCityLoading(true);
    setCityError(false);

    fetchGlobeCityLabels()
      .then((labels) => {
        if (!active) return;
        setCityLabels(labels);
      })
      .catch(() => {
        if (!active) return;
        setCityError(true);
      })
      .finally(() => {
        if (!active) return;
        setCityLoading(false);
      });

    return () => {
      active = false;
    };
  }, [cityLabels.length, showCities]);

  const heatmapLayer = [data];

  const cityOverlay = showCities
    ? cityLabels.filter((label: GlobeCityLabel) => label.categories.length > 0)
    : [];

  const heatmapConfig = {
    heatmapPointLat:    "lat",
    heatmapPointLng:    "lng",
    heatmapPointWeight: "count",
    heatmapBandwidth:   0.9,
    heatmapBaseAltitude: 0,
    heatmapTopAltitude: 0.7,
    heatmapColorSaturation: 2.8,
    heatmapsTransitionDuration: 3000,
  };

  return (
    <section className="bg-white border border-slate-200 rounded-xl shadow-sm p-6">
      <div className="flex items-start justify-between gap-4 mb-4">
        <div>
          <h2 className="text-base font-semibold text-slate-800">Geocoded Activity Globe</h2>
          <p className="mt-1 text-sm text-slate-500">
            Event intensity by user location, rendered as a heatmap layer.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {/* interval buttons */}
          <div className="flex items-center bg-slate-50 border border-slate-200 rounded-lg p-1">
            {INTERVAL_FRAMES.map((tf) => (
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
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setShowCities((value: boolean) => !value)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                showCities
                  ? "bg-blue-50 text-blue-700 border-blue-200"
                  : "bg-slate-50 text-slate-500 border-slate-200 hover:text-slate-700"
              }`}
            >
              Show Cities
            </button>
          </div>
          {/* dedicated all-time button */}
          <button
            type="button"
            onClick={() => setHours(null)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
              hours === null
                ? "bg-indigo-50 text-indigo-700 border-indigo-200"
                : "bg-slate-50 text-slate-500 border-slate-200 hover:text-slate-700"
            }`}
          >
            All time
          </button>
          <div className="hidden sm:flex items-center gap-1.5 text-xs text-slate-500 ml-1">
            <span className="inline-flex h-2 w-2 rounded-full bg-orange-400/80" />
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

        {cityError && showCities && !error && (
          <div className="absolute left-4 top-4 z-10 rounded-lg border border-amber-200 bg-white/90 px-3 py-2 text-xs text-amber-700 shadow-sm">
            City labels could not be loaded.
          </div>
        )}

        {cityLoading && showCities && (
          <div className="absolute left-4 top-4 z-10 rounded-lg border border-sky-200 bg-white/90 px-3 py-2 text-xs text-sky-700 shadow-sm">
            Loading city labels...
          </div>
        )}

        <div className="relative h-full w-full">
          {webglAvailable === false && (
            <div className="absolute inset-0 flex items-center justify-center px-6 text-center text-sm text-slate-500">
              WebGL not available — globe cannot be rendered.
            </div>
          )}
          {!error && webglAvailable === true && (
            <Globe
              ref={globeRef}
              width={size.width || 800}
              height={size.height || 420}
              globeImageUrl="//cdn.jsdelivr.net/npm/three-globe/example/img/earth-blue-marble.jpg"
              bumpImageUrl="//cdn.jsdelivr.net/npm/three-globe/example/img/earth-topology.png"
              backgroundColor="#f8fbff"
              animateIn={false}
              showAtmosphere={true}
              atmosphereColor="#93c5fd"
              atmosphereAltitude={0.12}
              heatmapsData={heatmapLayer}
              labelsData={cityOverlay}
              labelLat={(d: GlobeCityLabel) => d.lat}
              labelLng={(d: GlobeCityLabel) => d.lng}
              labelText={(d: GlobeCityLabel) => d.text}
              labelSize={(d: GlobeCityLabel) => Math.max(d.size, 0.8)}
              labelDotRadius={(d: GlobeCityLabel) => d.dotRadius}
              labelColor={(d: GlobeCityLabel) => d.color}
              // Position labels close to globe surface, aligned with atmosphere layer
              labelAltitude={0.25}
              labelResolution={2}
              {...heatmapConfig}
              enablePointerInteraction={false}
              showGraticules={false}
            />
          )}
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between gap-4 text-xs text-slate-500">
        <span>{data.length.toLocaleString()} aggregated geo locations</span>
        <span>
          {showCities && cityOverlay.length > 0
            ? `${cityOverlay.length.toLocaleString()} city labels shown`
            : "heatmap layer only"}
        </span>
      </div>
    </section>
  );
}
