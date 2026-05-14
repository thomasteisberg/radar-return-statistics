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

// Display variable name -> zarr array name in the store. Omitted keys default
// to a 1:1 mapping. Add an entry here when the display name differs from the
// stored array (e.g. RSSNR is stored as `required_surface_snr_dB`).
export const VARIABLE_SOURCE: Record<string, string> = {
  rssnr: "required_surface_snr_dB",
};

export interface VariableInfo {
  label: string;
  cmap: string;
  unit: string;
  // Multiplier applied when formatting values for display (legend + tooltip).
  // Stored data is unchanged; e.g. TWTT is stored in seconds but shown as µs.
  displayScale?: number;
}

export const VARIABLES: Record<string, VariableInfo> = {
  rssnr: {
    label: "Required Surface SNR",
    cmap: "turbo",
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
    cmap: "turbo",
    unit: "dB",
  },
  bed_power_dB: {
    label: "Bed Power",
    cmap: "turbo",
    unit: "dB",
  },
  surface_twtt: {
    label: "Surface TWTT",
    cmap: "turbo",
    unit: "µs",
    displayScale: 1e6,
  },
  bed_twtt: {
    label: "Bed TWTT",
    cmap: "turbo",
    unit: "µs",
    displayScale: 1e6,
  },
};
