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

type NaturalEarthCollection = {
  features?: NaturalEarthFeature[];
};

const CITY_DATA_URL = "https://cdn.jsdelivr.net/gh/nvkelso/natural-earth-vector@master/geojson/ne_110m_populated_places_simple.geojson";

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
  const baseSize = primaryKind === "capital" ? 0.42 : 0.34;
  const scaledSize = Math.sqrt(Math.max(population, 1)) * (primaryKind === "capital" ? 0.00038 : 0.00034);
  const size = Math.max(baseSize, Math.min(primaryKind === "capital" ? 1.0 : 1.12, scaledSize));

  return {
    lat,
    lng,
    text: name,
    size,
    dotRadius: size * 0.44,
    color: primaryKind === "capital" ? "rgba(245, 158, 11, 0.82)" : "rgba(14, 165, 233, 0.75)",
    categories,
    primaryKind,
    population,
  };
}

async function loadCityLabels(): Promise<GlobeCityLabel[]> {
  const response = await fetch(CITY_DATA_URL);
  if (!response.ok) {
    throw new Error(`Failed to load city labels (${response.status})`);
  }

  const payload = (await response.json()) as NaturalEarthCollection;
  const features = Array.isArray(payload.features) ? payload.features : [];
  return features.map(toLabel).filter((label): label is GlobeCityLabel => label !== null).sort((left, right) => {
    if (right.population !== left.population) return right.population - left.population;
    return left.text.localeCompare(right.text);
  });
}

export async function fetchGlobeCityLabels(): Promise<GlobeCityLabel[]> {
  cityLabelsPromise ??= loadCityLabels();
  return cityLabelsPromise;
}