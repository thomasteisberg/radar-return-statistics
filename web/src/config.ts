export type Hemisphere = "antarctic" | "arctic";

export interface StoreConfig {
  label: string;
  url: string;
  hemisphere: Hemisphere;
}

export const STORES: StoreConfig[] = [
  {
    label: "Amundsen Sea Embayment",
    url: "https://opr-radar-metrics.s3.us-west-2.amazonaws.com/icechunk/ase/",
    hemisphere: "antarctic",
  },
  {
    label: "UTIG",
    url: "https://opr-radar-metrics.s3.us-west-2.amazonaws.com/icechunk/utig/",
    hemisphere: "antarctic",
  },
  {
    label: "Greenland",
    url: "https://opr-radar-metrics.s3.us-west-2.amazonaws.com/icechunk/greenland/",
    hemisphere: "arctic",
  },
];

export const ICE_PERMITTIVITY = 3.17;
export const C = 299792458; // speed of light m/s

// Raw zarr array names each display variable depends on.
// Omitted keys default to [variableName] (1:1 mapping).
export const VARIABLE_DEPS: Record<string, string[]> = {
  rssnr: ["surface_twtt", "bed_twtt", "surface_power_dB", "bed_power_dB"],
};

export const VARIABLES: Record<
  string,
  { label: string; cmap: string; unit: string }
> = {
  rssnr: {
    label: "Required Surface SNR",
    cmap: "viridis",
    unit: "dB",
  },
  surface_elevation: {
    label: "Surface Elevation",
    cmap: "terrain",
    unit: "m WGS84",
  },
  bed_elevation: {
    label: "Bed Elevation",
    cmap: "terrain",
    unit: "m WGS84",
  },
  surface_power_dB: {
    label: "Surface Power",
    cmap: "viridis",
    unit: "dB",
  },
  bed_power_dB: {
    label: "Bed Power",
    cmap: "viridis",
    unit: "dB",
  },
  surface_twtt: {
    label: "Surface TWTT",
    cmap: "viridis",
    unit: "s",
  },
  bed_twtt: {
    label: "Bed TWTT",
    cmap: "viridis",
    unit: "s",
  },
};
