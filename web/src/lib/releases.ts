import { apiFetch } from "./api";
import type { QualityProfile, ResolverTrace } from "./types";

/** Release console API (/ether/api/releases): search, pins, blocklist, history. */

export interface ReleaseInfo {
  title?: string;
  edition?: string;
  year?: number;
  seasons?: number[] | null;
  episodes?: number[] | null;
  fullSeason?: boolean;
  height?: number;
  source?: string;
  videoCodec?: string;
  hdr10?: boolean;
  dolbyVision?: boolean;
  audioCodec?: string;
  atmos?: boolean;
  channels?: string;
  group?: string;
  languages?: string[] | null;
  proper?: boolean;
  repack?: boolean;
}

export interface Release {
  id: string;
  guid?: string;
  indexer: string;
  name: string;
  size: number;
  pubDate: string;
  grabs?: number;
  password?: boolean;
  info: ReleaseInfo;
  score: number;
}

export interface FormatMatch {
  id: string;
  name: string;
  score: number;
}

export interface HealthResult {
  releaseId: string;
  checkedAt: string;
  healthy: boolean;
  reason?: string;
  duration?: number;
  /** "check" (an owner check), "verified" (passed during playback) or "blocked". */
  source: "check" | "verified" | "blocked";
  blocked?: boolean;
}

export interface ConsoleRelease {
  release: Release;
  score: number;
  formatScore: number;
  customFormats?: FormatMatch[] | null;
  compatibility?: string;
  /** Why source routing moved this release (faster source, or the hedge). */
  route?: string;
  rejected: boolean;
  reason?: string;
  /** Position in the order Ether tries releases (the pin first); 0 when rejected. */
  rank?: number;
  winner?: boolean;
  blocked?: boolean;
  blockReason?: string;
  pinned?: boolean;
  health?: HealthResult | null;
}

export type PinStatus = "" | "healthy" | "unhealthy" | "rejected" | "unavailable";

export interface Pin {
  titleKey: string;
  titleName: string;
  release: Release;
  note?: string;
  pinnedAt: string;
  lastStatus?: PinStatus;
  lastReason?: string;
  lastCheckedAt?: string;
}

export interface ConsoleSearch {
  titleKey: string;
  titleName: string;
  profile: string;
  cached: boolean;
  searchedAt: string;
  duration: number;
  winner?: string;
  pin?: Pin | null;
  pinNote?: string;
  releases: ConsoleRelease[] | null;
}

export interface BlockEntry {
  releaseId: string;
  name?: string;
  indexer?: string;
  titleKey: string;
  titleName?: string;
  reason: string;
  source: "auto" | "owner";
  manual: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface HistoryRecord {
  id: number;
  at: string;
  titleKey: string;
  titleName: string;
  profile?: string;
  releaseId?: string;
  releaseName?: string;
  indexer?: string;
  size?: number;
  error?: string;
  failover?: boolean;
  pinned?: boolean;
  candidates: number;
  trace?: ResolverTrace | null;
}

export interface ChildTitle {
  key: string;
  type: string;
  title: string;
  season?: number;
  episode?: number;
  aired?: string;
  pinned?: boolean;
}

export interface ConsoleProfiles {
  default: string;
  profiles: string[];
  special: string[];
}

export type StarrApp = "radarr" | "sonarr";

/** A TRaSH Guides quality profile offered for import. */
export interface TrashProfile {
  /** File name without .json; what the import endpoints take. */
  id: string;
  trashId?: string;
  name: string;
  /** Plain text; never HTML. */
  description?: string;
  group?: number;
  /** The guide page (always https://trash-guides.info). */
  url?: string;
  scoreSet?: string;
  formats: number;
}

/** present: already in Custom scoring; add: added by the import. */
export type ImportFormatStatus = "present" | "add" | "unsupported" | "missing";

export interface ImportFormat {
  id: string;
  name: string;
  score: number;
  status: ImportFormatStatus;
  reason?: string;
}

/** A previewed import: the profile as it will be saved and its custom formats. */
export interface QualityImportPlan {
  app: StarrApp;
  profile: QualityProfile;
  scoreSet?: string;
  formats: ImportFormat[];
  skippedQualities: string[];
  warnings: string[];
}

/** A TRaSH profile by id, or pasted profile JSON. */
export type QualityImportSource = { app: StarrApp } & (
  { profile: string; json?: undefined } | { json: string; profile?: undefined }
);

/** Profile names the server understands beside the configured ones. */
export const PROFILE_NONE = "none";
export const PROFILE_DOWNLOAD = "download";

function qs(params: Record<string, string | number | undefined | null>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params))
    if (v !== undefined && v !== null && v !== "") q.set(k, String(v));
  const s = q.toString();
  return s ? `?${s}` : "";
}

const base = "/releases";
const post = <T>(path: string, body: unknown) =>
  apiFetch<T>(base + path, { method: "POST", body: JSON.stringify(body) });
const del = (path: string) => apiFetch<null>(base + path, { method: "DELETE" });

export const releaseConsole = {
  profiles: () => apiFetch<ConsoleProfiles>(`${base}/profiles`),
  search: (key: string, opts: { profile?: string; fresh?: boolean } = {}, signal?: AbortSignal) =>
    apiFetch<ConsoleSearch>(
      `${base}/search${qs({ key, profile: opts.profile, fresh: opts.fresh ? 1 : undefined })}`,
      { signal },
    ),
  children: async (key: string, signal?: AbortSignal) =>
    (
      await apiFetch<{ items: ChildTitle[] | null }>(
        `${base}/titles/${encodeURIComponent(key)}/children`,
        { signal },
      )
    ).items ?? [],
  checkHealth: (releaseId: string, key?: string) =>
    post<HealthResult>("/health", { releaseId, key: key ?? "" }),
  forget: (key: string) => post<{ ok: boolean }>("/forget", { key }),
  pins: async (signal?: AbortSignal) =>
    (await apiFetch<{ pins: Pin[] | null }>(`${base}/pins`, { signal })).pins ?? [],
  pin: (key: string, releaseId: string, note: string) =>
    post<Pin>("/pins", { key, releaseId, note }),
  unpin: (key: string) => del(`/pins/${encodeURIComponent(key)}`),
  blocklist: async (
    params: { key?: string; source?: string; q?: string; limit?: number; offset?: number },
    signal?: AbortSignal,
  ) => {
    const res = await apiFetch<{ items: BlockEntry[] | null; total: number }>(
      `${base}/blocklist${qs(params)}`,
      { signal },
    );
    return { items: res.items ?? [], total: res.total ?? 0 };
  },
  block: (body: {
    releaseId: string;
    key?: string;
    reason: string;
    name?: string;
    indexer?: string;
  }) => post<BlockEntry>("/blocklist", body),
  unblock: (releaseId: string) => del(`/blocklist/${encodeURIComponent(releaseId)}`),
  history: async (
    params: { key?: string; limit?: number; offset?: number },
    signal?: AbortSignal,
  ) => {
    const res = await apiFetch<{ items: HistoryRecord[] | null; total: number }>(
      `${base}/history${qs(params)}`,
      { signal },
    );
    return { items: res.items ?? [], total: res.total ?? 0 };
  },
  historyEntry: (id: number) => apiFetch<HistoryRecord>(`${base}/history/${id}`),
  trashProfiles: async (app: StarrApp, signal?: AbortSignal) =>
    (await apiFetch<{ items: TrashProfile[] | null }>(`${base}/trash/${app}/profiles`, { signal }))
      .items ?? [],
  previewImport: (source: QualityImportSource, signal?: AbortSignal) =>
    apiFetch<QualityImportPlan>(`${base}/quality-import/preview`, {
      method: "POST",
      body: JSON.stringify(source),
      signal,
    }),
  importProfile: (source: QualityImportSource, name: string) =>
    post<{ name: string; formatsAdded: number }>("/quality-import", { ...source, name }),
};

/** "WEB-DL 1080p" for an imported quality gate such as "webdl-1080"; DVD has no resolution. */
export function qualityGateLabel(gate: string): string {
  const [source = "", height = ""] = gate.split("-");
  const name = SOURCES[source] ?? source;
  return source === "dvd" ? name : `${name} ${height === "2160" ? "4K" : `${height}p`}`;
}

/** "Blu-ray" for a source key. */
export function sourceLabel(source: string): string {
  return SOURCES[source] ?? source;
}

/** "4K", "1080p", or "SD" for unknown heights. */
function resolutionLabel(info: ReleaseInfo | undefined): string {
  const h = info?.height ?? 0;
  if (h >= 2160) return "4K";
  if (h > 0) return `${h}p`;
  return "SD";
}

const SOURCES: Record<string, string> = {
  remux: "Remux",
  bluray: "Blu-ray",
  webdl: "WEB-DL",
  webrip: "WEBRip",
  hdtv: "HDTV",
  dvd: "DVD",
  screener: "Screener",
  cam: "CAM",
};

/** "4K Remux · HEVC · TrueHD Atmos" from parsed release info. */
export function qualityLabel(info: ReleaseInfo | undefined): string {
  if (!info) return "Unknown";
  const parts = [resolutionLabel(info)];
  if (info.source && SOURCES[info.source]) parts[0] += ` ${SOURCES[info.source]}`;
  if (info.videoCodec) parts.push(info.videoCodec.toUpperCase());
  if (info.audioCodec)
    parts.push(
      `${info.audioCodec.toUpperCase()}${info.channels ? ` ${info.channels}` : ""}${info.atmos ? " Atmos" : ""}`,
    );
  return parts.join(" · ");
}

/** Dynamic range badge text, or "" for SDR. */
export function rangeLabel(info: ReleaseInfo | undefined): string {
  if (info?.dolbyVision && info?.hdr10) return "DV · HDR10";
  if (info?.dolbyVision) return "Dolby Vision";
  if (info?.hdr10) return "HDR10";
  return "";
}

/** Whole days since a release was posted, or null when unknown. */
export function ageDays(pubDate: string | undefined, now: Date = new Date()): number | null {
  if (!pubDate) return null;
  const t = new Date(pubDate).getTime();
  if (!Number.isFinite(t) || new Date(pubDate).getUTCFullYear() < 1971) return null;
  return Math.max(0, Math.floor((now.getTime() - t) / 86400000));
}

export const PIN_STATUS_LABEL: Record<Exclude<PinStatus, "">, string> = {
  healthy: "Playing",
  unhealthy: "Failing",
  rejected: "Not playable here",
  unavailable: "Check failed",
};
