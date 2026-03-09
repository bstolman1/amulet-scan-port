import pako from "pako";

export interface DashboardData {
  version: string;
  exportDate: string;
  dashboard: {
    title: string;
    tiles: Array<{
      key: string;
      i: string;
      x: number;
      y: number;
      w: number;
      h: number;
      query: string;
      title: string;
      displayType: string;
      dataConfig: any;
      skipQueryValidation: boolean;
    }>;
    props: {
      enableDynamicCharting?: boolean;
      autoChartAggregations?: boolean;
      refreshInterval?: number;
    };
  };
}

/**
 * Processes a dashboard file from URL and returns the parsed data
 */
export const processDashboardFileFromUrl = async (
  url: string
): Promise<DashboardData> => {
  try {
    // Use query parameter for cache-busting
    const separator = url.includes("?") ? "&" : "?";
    const cacheBustUrl = `${url}${separator}_t=${Date.now()}`;
    const response = await fetch(cacheBustUrl, {
      cache: "no-cache",
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch dashboard: ${response.statusText}`);
    }

    const arrayBuffer = await response.arrayBuffer();

    // Try to decompress with pako first (for compressed files)
    let content: string;
    try {
      const decompressed = pako.inflate(arrayBuffer, { to: "string" });
      content = typeof decompressed === "string" ? decompressed : "";
    } catch (compressionError) {
      // If decompression fails, try reading as text (for uncompressed files)
      const textDecoder = new TextDecoder();
      content = textDecoder.decode(arrayBuffer);
    }

    const jsonData = JSON.parse(content);

    // Validate the structure
    if (!jsonData.dashboard) {
      throw new Error("Invalid .aqldash file: missing dashboard data");
    }

    if (!jsonData.dashboard.tiles || !Array.isArray(jsonData.dashboard.tiles)) {
      throw new Error("Invalid .aqldash file: missing or invalid tiles array");
    }

    // Ensure required fields exist with defaults
    return {
      version: jsonData.version || "1.0",
      exportDate: jsonData.exportDate || new Date().toISOString(),
      dashboard: {
        ...jsonData.dashboard,
        props: jsonData.dashboard.props || {},
      },
    };
  } catch (err) {
    throw new Error(
      `Failed to process dashboard file: ${
        err instanceof Error ? err.message : "Unknown error"
      }`
    );
  }
};
