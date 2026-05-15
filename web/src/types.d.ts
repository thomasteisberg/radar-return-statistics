// `georaster` ships no type definitions; declare the single function we use.
declare module "georaster" {
  export interface GeoRaster {
    projection: number;
    noDataValue: number | null | undefined;
    [key: string]: unknown;
  }
  // Accepts an ArrayBuffer or a URL string (URL enables COG ranged reads).
  export default function parseGeoraster(
    input: ArrayBuffer | string,
  ): Promise<GeoRaster>;
}
