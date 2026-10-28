/**
 * Shapes of the owner-UI API contract. Kept in one file so the screens, the
 * fake backend and the tests agree on a single definition.
 */

export type MediaKind = "movie" | "show" | "season" | "episode";

export interface TitleCard {
  key: string;
  type: MediaKind;
  title: string;
  year?: number;
  summary?: string;
  poster?: string;
  backdrop?: string;
  rating?: number;
  contentRating?: string;
  runtimeMinutes?: number;
  genres?: string[];
  showTitle?: string;
  season?: number;
  episode?: number;
}

export interface TitleDetail extends TitleCard {
  cast?: { id: number; name: string; role?: string; image?: string }[];
  directors?: { id: number; name: string; image?: string }[];
  keywords?: { id: number; name: string }[] | string[];
  collection?: { id: number; name: string } | null;
}

const RULE_FIELDS = [
  "genre",
  "keyword",
  "person",
  "studio",
  "network",
  "country",
  "language",
  "year",
  "decade",
  "rating",
  "votes",
  "popularity",
  "runtime",
  "contentRating",
  "released",
  "watchProvider",
  "collection",
  "watched",
  "inProgress",
  "addedAt",
] as const;
export type RuleField = (typeof RULE_FIELDS)[number];

const RULE_OPS = ["is", "isNot", "in", "notIn", "gte", "lte", "between", "within"] as const;
export type RuleOp = (typeof RULE_OPS)[number];

export interface Rule {
  field: RuleField;
  op: RuleOp;
  value: unknown;
  label?: string;
}

export type TmdbListName =
  | "trending_day"
  | "trending_week"
  | "popular"
  | "top_rated"
  | "now_playing"
  | "upcoming"
  | "airing_today"
  | "on_the_air";

export type LookupSourceKind = "tmdbKeyword" | "tmdbCompany" | "tmdbNetwork" | "tmdbPerson";

export type Source =
  | { kind: "catalog" }
  | { kind: "tmdbList"; list: TmdbListName; mediaType: "movie" | "show" }
  | { kind: "tmdbCollection"; id: number; label?: string }
  | { kind: "tmdbDiscover"; params: Record<string, string> }
  | { kind: LookupSourceKind; id: number; label?: string }
  | { kind: "watchProvider"; id: number; region: string; label?: string }
  | { kind: "traktList"; url: string }
  | { kind: "letterboxdList"; url: string }
  | { kind: "mdblist"; url: string }
  | { kind: "titles"; keys: string[] };

export type SourceKind = Source["kind"];

export type CollectionSort =
  "popularity" | "rating" | "released" | "title" | "added" | "random" | "personal" | "source";

export type Refresh = "live" | "hourly" | "daily" | "weekly";
export type ArtworkMode = "auto" | "collage" | "backdrop" | "tmdb" | "custom";
export type DeviceClass = "tv" | "mobile" | "web" | "desktop";

export interface TargetSchedule {
  days?: number[];
  fromHour?: number;
  toHour?: number;
  devices?: DeviceClass[];
}

export interface CollectionSpec {
  localOnly?: boolean;
  id?: string;
  name: string;
  description?: string;
  mediaType: "movie" | "show" | "mixed";
  sources: Source[];
  match: "all" | "any";
  rules: Rule[];
  exclude: Rule[];
  sort: CollectionSort;
  sortDesc: boolean;
  limit: number;
  refresh: Refresh;
  rotate?: { size: number; every: "daily" | "weekly" } | null;
  artwork: { mode: ArtworkMode; url?: string; accent?: string };
  targets: {
    collectionsTab: boolean;
    homeRow: boolean;
    sectionRow: boolean;
    users: number[] | "all";
    schedule?: TargetSchedule | null;
  };
  enabled: boolean;
  templateId?: string;
  updatedAt?: string;
  lastBuiltAt?: string;
  itemCount?: number;
  /** Set by the server on built-in collections (read-only in the studio). */
  builtIn?: boolean;
}

export interface CollectionPreview {
  total: number;
  items: TitleCard[];
  warnings: string[];
  tookMs: number;
}

export interface CollectionTemplate {
  id: string;
  name: string;
  description: string;
  category: string;
  spec: CollectionSpec;
}

export type FacetName = "genre" | "decade" | "contentRating" | "studio" | "country" | "language";
export interface FacetValue {
  key: string;
  title: string;
  count: number;
}

export type LookupKind =
  "person" | "keyword" | "company" | "network" | "collection" | "watchProvider";
export interface LookupResult {
  id: number;
  name: string;
  subtitle?: string;
  image?: string;
}

// ---- Viewers and personalization ------------------------------------------

export type ViewerAccessStatus = "owner" | "active" | "imported" | "inactive";

export interface Viewer {
  id: number;
  plexId: number;
  username: string;
  title: string;
  thumb?: string;
  isOwner: boolean;
  accessStatus?: ViewerAccessStatus;
  lastActive?: string;
  plays30d: number;
  hoursWatched30d: number;
}

export interface ViewerActivity {
  title: TitleCard;
  at: string;
  completed: boolean;
  progress: number;
}

export interface ViewerDetail extends Viewer {
  recent: ViewerActivity[];
}

export type OverrideKind = "genre" | "person" | "keyword";
export interface TasteOverride {
  kind: OverrideKind;
  value: string;
  weight: number;
}

export interface TasteProfile {
  userId: number;
  updatedAt: string;
  confidence: number;
  genres: { name: string; weight: number }[];
  people: { id: number; name: string; role: string; weight: number; image?: string }[];
  keywords: { name: string; weight: number }[];
  decades: { decade: number; weight: number }[];
  languages: { code: string; weight: number }[];
  habits: {
    weeknightMinutes: number;
    weekendMinutes: number;
    preferredDevice: string;
    bingeScore: number;
    rewatchScore: number;
    peakHours: number[];
  };
  overrides: TasteOverride[];
}

export type LayerName = "global" | "personalized" | "override";

export interface HomeRow {
  hubId: string;
  title: string;
  kind: string;
  reason: string;
  score: number;
  pinned?: boolean;
  hidden?: boolean;
  source: "engine" | "owner" | "plex";
  items: TitleCard[];
  itemReasons?: Record<string, string>;
  /** Provenance in the layered Home Designer. */
  layer?: LayerName;
  slot?: number;
  anchorSlot?: number;
  placement?: string;
  hiddenReason?: string;
  policy?: RowPolicy;
  policyLayer?: "global" | "override";
}

export interface HomeContext {
  device: string;
  daypart: string;
  weekend: boolean;
}

export interface ViewerHome {
  rows: HomeRow[];
  generatedAt: string;
  context: HomeContext;
  /** layout/preview: the previewed viewer, null for the typical viewer. */
  viewer?: { id: number; title: string } | null;
  /** Movies/TV: titles kept off this page because the Home leads with them. */
  homeDeduped?: number;
}

export type LayoutSection = "home" | "movies" | "shows";
export type Device = "tv" | "mobile" | "web" | "desktop";
export type Daypart = "morning" | "afternoon" | "evening" | "late";

export interface LayoutWhen {
  days?: number[];
  fromHour?: number;
  toHour?: number;
  devices?: string[];
  dayparts?: Daypart[];
  months?: number[];
  startDate?: string;
  endDate?: string;
}

export type RowStatus = "always" | "eligible" | "off";

/** The owner's policy for a row (or a family of rows) on a page. */
export interface RowPolicy {
  /** Kind ("short-on-time"), "collection:<id>", a hub id, or a glob ("because.*"). */
  row: string;
  status: RowStatus;
  /** Anchored slot, 1 = top. */
  slot?: number;
  /** Never above this slot. */
  minSlot?: number;
  /** Never below this slot. */
  maxSlot?: number;
  priority?: number;
  maxPerPage?: number;
  maxDaysPerWeek?: number;
  when?: LayoutWhen;
}

export interface LayoutPage {
  section: LayoutSection;
  maxRows?: number;
  deviceMaxRows?: Partial<Record<Device, number>>;
  rows: RowPolicy[];
  /** Keep what the viewer's Home leads with off this page (Movies/TV; default on). */
  dedupeHome?: boolean;
  tuning?: LayoutTuning;
}

/** The page's three dials, each 0..1 (0.5 = engine default); unset inherits. */
export interface LayoutTuning {
  discovery?: number;
  freshness?: number;
  variety?: number;
}
export type TuningDial = keyof LayoutTuning;

export interface ViewerLayout {
  userId: number;
  pages: LayoutPage[];
}

export interface HomeLayout {
  global: LayoutPage[];
  viewers: ViewerLayout[];
  updatedAt?: string;
}

export type RowGroup = "picks" | "explore" | "shows" | "household" | "collections" | "plex";

export interface AvailableRow {
  hubId: string;
  title: string;
  kind: string;
  description: string;
  /** Pages the row can appear on; empty or missing means every page. */
  sections?: LayoutSection[];
  group?: RowGroup;
}

export interface RankerFeature {
  name: string;
  label: string;
  weight: number;
}

export interface RankerInsight {
  trainedAt?: string;
  examples: number;
  positives: number;
  auc: number;
  baselineAuc: number;
  active: boolean;
  features: RankerFeature[] | null;
}

export interface LayoutDoc {
  layout: HomeLayout;
  availableRows: AvailableRow[];
  defaults: { maxRows: Record<LayoutSection, number> };
}

export interface LayoutImpact {
  viewerId: number;
  title: string;
  overrides: number;
  changed: boolean;
  added: string[];
  removed: string[];
  moved: number;
}

export interface LayoutImpactResult {
  viewers: LayoutImpact[];
  summary: { viewers: number; withOverrides: number; changed: number };
}

/** Context of a Home preview. */
export interface PreviewContext {
  device: Device;
  at: string;
  section?: LayoutSection;
}

// ---- Insights ---------------------------------------------------------------

export interface RowInsight {
  hubId: string;
  title: string;
  impressions7d: number;
  plays7d: number;
  playRate: number;
}

export interface ActivityDay {
  date: string;
  plays: number;
  hours: number;
  uniqueViewers: number;
}
