export type GlobeCityKind = "capital" | "major";

export type GlobeCityLabel = {
  lat: number;
  lng: number;
  text: string;
  size: number;
  dotRadius: number;
  color: string;
  categories: GlobeCityKind[];
  primaryKind: GlobeCityKind;
  population: number;
};

type NaturalEarthFeature = {
  properties?: {
    name?: string | null;
    latitude?: number | null;
    longitude?: number | null;
    pop_max?: number | null;
    adm0cap?: number | null;
    capalt?: number | null;
    worldcity?: number | null;
    megacity?: number | null;
  };
};

// Import the Natural Earth data from a local TypeScript module (full dataset, no runtime fetch).
import naturalEarthData from "./natural-earth-cities";

let cityLabelsPromise: Promise<GlobeCityLabel[]> | null = null;

function toPopulation(value: number | null | undefined): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function toLabel(feature: NaturalEarthFeature): GlobeCityLabel | null {
  const properties = feature.properties;
  if (!properties) return null;

  const name = properties.name?.trim();
  const lat = properties.latitude;
  const lng = properties.longitude;
  if (!name || typeof lat !== "number" || typeof lng !== "number") return null;

  const population = toPopulation(properties.pop_max);
  const isCapital = properties.adm0cap === 1 || properties.capalt === 1;
  const isMajorCity = properties.megacity === 1 || properties.worldcity === 1 || population >= 1_000_000;

  if (!isCapital && !isMajorCity) return null;

  const categories: GlobeCityKind[] = [];
  if (isCapital) categories.push("capital");
  if (isMajorCity) categories.push("major");

  const primaryKind: GlobeCityKind = isCapital ? "capital" : "major";
  const baseSize = primaryKind === "capital" ? 0.62 : 0.54;
  const scaledSize = Math.sqrt(Math.max(population, 1)) * (primaryKind === "capital" ? 0.00048 : 0.00044);
  const size = Math.max(baseSize, Math.min(primaryKind === "capital" ? 1.2 : 1.32, scaledSize));

  return {
    lat,
    lng,
    text: name,
    size,
    dotRadius: size * 0.52,
    color: "rgba(255, 255, 255, 0.92)",
    categories,
    primaryKind,
    population,
  };
}

function toLabelsFromData(data: unknown): GlobeCityLabel[] {
  const d = data as { features?: NaturalEarthFeature[] };
  const features = Array.isArray(d?.features) ? d.features : [];
  return features
    .map(toLabel)
    .filter((label): label is GlobeCityLabel => label !== null)
    .sort((left, right) => {
      if (right.population !== left.population) return right.population - left.population;
      return left.text.localeCompare(right.text);
    });
}

async function loadCityLabels(): Promise<GlobeCityLabel[]> {
  const labels = toLabelsFromData(naturalEarthData as unknown);
  if (labels.length > 0) return labels;
  throw new Error("failed to load city data");
}

// Load city labels from local bundled Natural Earth data (no network required).
export async function fetchGlobeCityLabels(): Promise<GlobeCityLabel[]> {
  cityLabelsPromise ??= loadCityLabels().catch((error) => {
    // Allow retries after transient/network/public-file failures.
    cityLabelsPromise = null;
    throw error;
  });
  return cityLabelsPromise;
}
