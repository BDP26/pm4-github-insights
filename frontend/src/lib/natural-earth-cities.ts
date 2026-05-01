// Minimal bundled Natural Earth features subset (TypeScript module)
// This is a small, local subset to avoid JSON import/build issues.
const naturalEarthData = {
  type: "FeatureCollection",
  name: "ne_110m_populated_places_simple_subset",
  features: [
    {
      type: "Feature",
      properties: {
        name: "Berlin",
        latitude: 52.520008,
        longitude: 13.404954,
        pop_max: 3769000,
        adm0cap: 1,
        capalt: 0,
        worldcity: 1,
        megacity: 0,
      },
    },
    {
      type: "Feature",
      properties: {
        name: "London",
        latitude: 51.507351,
        longitude: -0.127758,
        pop_max: 8982000,
        adm0cap: 1,
        capalt: 0,
        worldcity: 1,
        megacity: 1,
      },
    },
    {
      type: "Feature",
      properties: {
        name: "Paris",
        latitude: 48.856613,
        longitude: 2.352222,
        pop_max: 2148000,
        adm0cap: 1,
        capalt: 0,
        worldcity: 1,
        megacity: 1,
      },
    },
    {
      type: "Feature",
      properties: {
        name: "New York",
        latitude: 40.712776,
        longitude: -74.005974,
        pop_max: 8400000,
        adm0cap: 0,
        capalt: 0,
        worldcity: 1,
        megacity: 1,
      },
    },
    {
      type: "Feature",
      properties: {
        name: "Tokyo",
        latitude: 35.689487,
        longitude: 139.691711,
        pop_max: 13929000,
        adm0cap: 1,
        capalt: 0,
        worldcity: 1,
        megacity: 1,
      },
    },
    {
      type: "Feature",
      properties: {
        name: "Beijing",
        latitude: 39.904202,
        longitude: 116.407394,
        pop_max: 21516000,
        adm0cap: 1,
        capalt: 0,
        worldcity: 1,
        megacity: 1,
      },
    },
    {
      type: "Feature",
      properties: {
        name: "São Paulo",
        latitude: -23.55052,
        longitude: -46.633308,
        pop_max: 12330000,
        adm0cap: 0,
        capalt: 0,
        worldcity: 1,
        megacity: 1,
      },
    },
    {
      type: "Feature",
      properties: {
        name: "Cape Town",
        latitude: -33.924869,
        longitude: 18.424055,
        pop_max: 433688,
        adm0cap: 0,
        capalt: 0,
        worldcity: 0,
        megacity: 0,
      },
    }
  ],
};

export default naturalEarthData;
