// CDN service that provides a list of files from the Sync Insights CDN

import pako from "pako";

export interface CDNFile {
  id: string;
  name: string;
  title?: string; // Dashboard title from the file
  url: string; // CDN URL
  size: number; // File size in bytes
  type: string; // MIME type
  lastModified: string; // ISO date string
  extension: string;
  category?: string; // Optional category (e.g., 'dashboard', 'document', 'image')
}

// CDN base URL
const CDN_BASE_URL = "https://cdn.chata.io/syncinsights/dashboards";
const DASHBOARDS_LIST_URL = `${CDN_BASE_URL}/si_dashboards.json`;

interface DashboardsListResponse {
  dashboards: string[];
}

/**
 * Fetches and extracts the title from a dashboard file
 * This is a lightweight operation that only fetches and parses enough to get the title
 */
const fetchDashboardTitle = async (fileUrl: string): Promise<string | null> => {
  try {
    // Use query parameter for cache-busting to avoid CORS issues
    const separator = fileUrl.includes("?") ? "&" : "?";
    const cacheBustUrl = `${fileUrl}${separator}_t=${Date.now()}`;
    const response = await fetch(cacheBustUrl, {
      cache: "no-cache",
    });
    if (!response.ok) {
      return null;
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

    // Parse JSON and extract title
    const jsonData = JSON.parse(content);
    return jsonData.dashboard?.title || null;
  } catch (error) {
    console.error(`Error fetching title from ${fileUrl}:`, error);
    return null;
  }
};

/**
 * Fetch files from CDN
 * Fetches the dashboard list from the CDN and transforms it to CDNFile format
 * Also fetches titles from each dashboard file
 */
export const fetchCDNFiles = async (): Promise<CDNFile[]> => {
  try {
    // Use query parameter for cache-busting to avoid CORS issues
    const separator = DASHBOARDS_LIST_URL.includes("?") ? "&" : "?";
    const cacheBustUrl = `${DASHBOARDS_LIST_URL}${separator}_t=${Date.now()}`;
    const response = await fetch(cacheBustUrl, {
      cache: "no-cache",
    });

    if (!response.ok) {
      throw new Error(
        `Failed to fetch dashboards list: ${response.statusText}`
      );
    }

    const data: DashboardsListResponse = await response.json();

    // Transform the dashboard filenames to CDNFile format
    const files: CDNFile[] = data.dashboards.map((filename, index) => {
      const fileUrl = `${CDN_BASE_URL}/${filename}`;

      return {
        id: `dashboard-${index + 1}`,
        name: filename,
        url: fileUrl,
        size: 0, // Size unknown until we fetch the file
        type: "application/octet-stream",
        lastModified: new Date().toISOString(), // We don't have this info from the list
        extension: ".aqldash",
        category: "dashboard",
      };
    });

    // Fetch titles for all dashboards in parallel
    const titlePromises = files.map((file) =>
      fetchDashboardTitle(file.url).then((title) => ({
        id: file.id,
        title:
          title ||
          file.name
            .replace(/\.aqldash$/, "")
            .replace(/_/g, " ")
            .replace(/\s+/g, " ")
            .trim(),
      }))
    );

    const titles = await Promise.all(titlePromises);

    // Merge titles into files
    return files.map((file) => {
      const titleData = titles.find((t) => t.id === file.id);
      return {
        ...file,
        title: titleData?.title,
      };
    });
  } catch (error) {
    console.error("Error fetching CDN files:", error);
    // Return empty array on error
    return [];
  }
};
