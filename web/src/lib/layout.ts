import type {
  Daypart,
  Device,
  HomeLayout,
  LayoutPage,
  LayoutSection,
  LayoutTuning,
  LayoutWhen,
  RowPolicy,
  RowStatus,
  TuningDial,
} from "./contract";

/**
 * Home Designer helpers for the layered layout: an Everyone template,
 * per-viewer overrides on top. Edits are immutable; empty pages and viewer
 * layers are pruned so an untouched document compares equal to the saved one.
 */

/** A layer of the document: Everyone's page, or a viewer's overrides (their id). */
export type LayerKey = "global" | number;

export const EMPTY_LAYOUT: HomeLayout = { global: [], viewers: [] };

export const SECTIONS: { value: LayoutSection; label: string }[] = [
  { value: "home", label: "Home" },
  { value: "movies", label: "Movies" },
  { value: "shows", label: "TV" },
];
export const DEVICES: { value: Device; label: string }[] = [
  { value: "tv", label: "TV" },
  { value: "mobile", label: "Phone" },
  { value: "web", label: "Web" },
  { value: "desktop", label: "Desktop" },
];
export const DAYPARTS: { value: Daypart; label: string }[] = [
  { value: "morning", label: "Morning" },
  { value: "afternoon", label: "Afternoon" },
  { value: "evening", label: "Evening" },
  { value: "late", label: "Late" },
];
export const MONTHS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
] as const;

export const STATUS_LABEL: Record<RowStatus, string> = {
  always: "Always",
  eligible: "Eligible",
  off: "Off",
};
export const STATUS_HINT: Record<RowStatus, string> = {
  always: "On the page whenever it has titles",
  eligible: "Personalization decides whether and where",
  off: "Never shown",
};

/** Priority steps the owner picks from (ranking weight). */
export const PRIORITIES: { value: number; label: string }[] = [
  { value: 0.3, label: "Much lower" },
  { value: 0.6, label: "Lower" },
  { value: 1, label: "Normal" },
  { value: 1.5, label: "Higher" },
  { value: 2.5, label: "Much higher" },
];

/** Rows Plex builds outside the engine's arrangement (no policies). */
export const FRONT_END_ROWS = new Set<string>();
export const CONTINUE = "continue-watching";

// ---- time -------------------------------------------------------------------------

/** The next moment (from `now`, local time) that falls on `day` at `hour`:00. */
export function atFor(day: number, hour: number, now: Date = new Date()): Date {
  const d = new Date(now);
  d.setHours(hour, 0, 0, 0);
  let delta = (day - now.getDay() + 7) % 7;
  if (delta === 0 && d.getTime() < now.getTime() - 3600_000) delta = 7;
  d.setDate(d.getDate() + delta);
  return d;
}

export function daypartOf(hour: number): Daypart {
  if (hour >= 5 && hour < 12) return "morning";
  if (hour >= 12 && hour < 17) return "afternoon";
  if (hour >= 17 && hour < 22) return "evening";
  return "late";
}

export function hasWhen(w: LayoutWhen | undefined): w is LayoutWhen {
  return Boolean(
    w &&
    ((w.days?.length ?? 0) > 0 ||
      w.fromHour !== undefined ||
      w.toHour !== undefined ||
      (w.devices?.length ?? 0) > 0 ||
      (w.dayparts?.length ?? 0) > 0 ||
      (w.months?.length ?? 0) > 0 ||
      Boolean(w.startDate) ||
      Boolean(w.endDate)),
  );
}

export interface WhenContext {
  day: number;
  hour: number;
  device: string;
  date?: Date;
}

/** Whether a schedule allows the context (every set field must match). */
export function whenMatches(w: LayoutWhen | undefined, ctx: WhenContext): boolean {
  if (!w) return true;
  if (w.days?.length && !w.days.includes(ctx.day)) return false;
  if (w.devices?.length && !w.devices.includes(ctx.device)) return false;
  if (w.dayparts?.length && !w.dayparts.includes(daypartOf(ctx.hour))) return false;
  const from = w.fromHour;
  const to = w.toHour;
  if (from !== undefined || to !== undefined) {
    const f = from ?? 0;
    const t = to ?? 24;
    const inside = f <= t ? ctx.hour >= f && ctx.hour < t : ctx.hour >= f || ctx.hour < t;
    if (!inside) return false;
  }
  if (ctx.date) {
    if (w.months?.length && !w.months.includes(ctx.date.getMonth() + 1)) return false;
    const iso = isoDate(ctx.date);
    if (w.startDate && iso < w.startDate) return false;
    if (w.endDate && iso > w.endDate) return false;
  }
  return true;
}

export function isoDate(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

export function describeWhen(w: LayoutWhen | undefined, dayNames: readonly string[]): string {
  if (!hasWhen(w)) return "Any time";
  const parts: string[] = [];
  if (w.days?.length) {
    const d = [...w.days].sort();
    if (d.length === 5 && [1, 2, 3, 4, 5].every((x) => d.includes(x))) parts.push("Weekdays");
    else if (d.length === 2 && d.includes(0) && d.includes(6)) parts.push("Weekends");
    else parts.push(d.map((x) => dayNames[x]).join(", "));
  }
  if (w.fromHour !== undefined || w.toHour !== undefined) {
    const fmt = (h: number) => `${String(h).padStart(2, "0")}:00`;
    parts.push(`${fmt(w.fromHour ?? 0)}–${fmt(w.toHour ?? 24)}`);
  }
  if (w.dayparts?.length)
    parts.push(w.dayparts.map((d) => DAYPARTS.find((x) => x.value === d)?.label ?? d).join(", "));
  if (w.devices?.length)
    parts.push(w.devices.map((d) => DEVICES.find((x) => x.value === d)?.label ?? d).join(", "));
  if (w.months?.length)
    parts.push(
      [...w.months]
        .sort((a, b) => a - b)
        .map((m) => MONTHS[m - 1])
        .join(", "),
    );
  if (w.startDate && w.endDate) parts.push(`${w.startDate} to ${w.endDate}`);
  else if (w.startDate) parts.push(`from ${w.startDate}`);
  else if (w.endDate) parts.push(`until ${w.endDate}`);
  return parts.join(" · ");
}

// ---- reading the document -----------------------------------------------------------

export function pagesOf(doc: HomeLayout, layer: LayerKey): LayoutPage[] {
  if (layer === "global") return doc.global ?? [];
  return doc.viewers?.find((v) => v.userId === layer)?.pages ?? [];
}

export function pageOf(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
): LayoutPage | undefined {
  return pagesOf(doc, layer).find((p) => (p.section ?? "home") === section);
}

/** The policy a layer sets for exactly this row key. */
export function policyIn(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  row: string,
): RowPolicy | undefined {
  return pageOf(doc, layer, section)?.rows.find((r) => r.row === row);
}

/** How specifically a policy's row targets a row: 3 hub id, 2 kind, 1 glob, 0 none. */
export function matchRank(pattern: string, hubId: string, kind: string): number {
  let stem = hubId;
  for (const p of ["home.", "movie.", "tv."]) {
    if (hubId.startsWith(p)) {
      stem = hubId.slice(p.length);
      break;
    }
  }
  if (pattern === hubId || pattern === stem) return 3;
  if (pattern === kind) return 2;
  if (/[*?[]/.test(pattern)) {
    const re = globToRegExp(pattern);
    if (re && (re.test(hubId) || re.test(stem) || re.test(kind))) return 1;
  }
  return 0;
}

function globToRegExp(glob: string): RegExp | null {
  let src = "";
  for (let i = 0; i < glob.length; i++) {
    const c = glob[i] as string;
    if (c === "*") src += "[^/]*";
    else if (c === "?") src += "[^/]";
    else if (c === "[") {
      const end = glob.indexOf("]", i + 1);
      if (end < 0) return null;
      src += `[${glob.slice(i + 1, end).replace(/^!/, "^")}]`;
      i = end;
    } else src += c.replace(/[.+^${}()|\\]/g, "\\$&");
  }
  try {
    return new RegExp(`^${src}$`);
  } catch {
    return null;
  }
}

function bestIn(page: LayoutPage | undefined, hubId: string, kind: string): RowPolicy | undefined {
  let best: RowPolicy | undefined;
  let rank = 0;
  for (const r of page?.rows ?? []) {
    const m = matchRank(r.row, hubId, kind);
    if (m > rank) {
      best = r;
      rank = m;
    }
  }
  return best;
}

export interface Effective {
  policy: RowPolicy;
  layer: "global" | "override";
}

/** A row's effective policy for a viewer (null = typical viewer): override, else Global. */
export function effectivePolicy(
  doc: HomeLayout,
  viewerId: number | null,
  section: LayoutSection,
  hubId: string,
  kind = hubId,
): Effective | null {
  if (viewerId !== null) {
    const p = bestIn(pageOf(doc, viewerId, section), hubId, kind);
    if (p) return { policy: p, layer: "override" };
  }
  const g = bestIn(pageOf(doc, "global", section), hubId, kind);
  return g ? { policy: g, layer: "global" } : null;
}

/** Viewers whose override layer sets this exact row key on the page. */
export function viewersOverriding(doc: HomeLayout, section: LayoutSection, row: string): number[] {
  return (doc.viewers ?? [])
    .filter((v) => v.pages.some((p) => p.section === section && p.rows.some((r) => r.row === row)))
    .map((v) => v.userId);
}

/** How many settings a viewer overrides (all pages, or one). */
export function overrideCount(doc: HomeLayout, userId: number, section?: LayoutSection): number {
  const pages = pagesOf(doc, userId).filter((p) => !section || p.section === section);
  return pages.reduce(
    (n, p) =>
      n +
      p.rows.length +
      (p.maxRows ? 1 : 0) +
      Object.values(p.deviceMaxRows ?? {}).filter(Boolean).length +
      (p.dedupeHome !== undefined ? 1 : 0) +
      tuningCount(p.tuning),
    0,
  );
}

/** The page length a layer sets for a device (undefined = inherits). */
export function lengthIn(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  device: Device | null,
): number | undefined {
  const p = pageOf(doc, layer, section);
  if (!p) return undefined;
  const n = device ? p.deviceMaxRows?.[device] : p.maxRows;
  return n && n > 0 ? n : undefined;
}

/** Effective page length and where it comes from. */
export function effectiveLength(
  doc: HomeLayout,
  viewerId: number | null,
  section: LayoutSection,
  device: Device,
  def: number,
): { n: number; layer: "global" | "override" | "default" } {
  const layers: [LayerKey, "global" | "override"][] =
    viewerId !== null
      ? [
          [viewerId, "override"],
          ["global", "global"],
        ]
      : [["global", "global"]];
  for (const [layer, name] of layers) {
    const d = lengthIn(doc, layer, section, device);
    if (d) return { n: d, layer: name };
    const p = lengthIn(doc, layer, section, null);
    if (p) return { n: p, layer: name };
  }
  return { n: def, layer: "default" };
}

// ---- editing ------------------------------------------------------------------------

function emptyPage(p: LayoutPage): boolean {
  return (
    p.rows.length === 0 &&
    !p.maxRows &&
    !Object.values(p.deviceMaxRows ?? {}).some(Boolean) &&
    p.dedupeHome === undefined &&
    !tuningCount(p.tuning)
  );
}

function tuningCount(t: LayoutTuning | undefined): number {
  return t ? TUNING_DIALS.filter((d) => t[d] !== undefined).length : 0;
}

export const TUNING_DIALS: TuningDial[] = ["discovery", "freshness", "variety"];
export const DIAL_DEFAULT = 0.5;

/** The dial value a layer sets (undefined = inherits). */
export function tuningIn(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  dial: TuningDial,
): number | undefined {
  return pageOf(doc, layer, section)?.tuning?.[dial];
}

/** Effective dial value and where it comes from (viewer → Everyone → default). */
export function effectiveTuning(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  dial: TuningDial,
): { v: number; from: "override" | "global" | "default" } {
  if (layer !== "global") {
    const own = tuningIn(doc, layer, section, dial);
    if (own !== undefined) return { v: own, from: "override" };
  }
  const g = tuningIn(doc, "global", section, dial);
  if (g !== undefined) return { v: g, from: "global" };
  return { v: DIAL_DEFAULT, from: "default" };
}

/** Set or clear (undefined) one dial in a layer. */
export function setTuning(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  dial: TuningDial,
  v: number | undefined,
): HomeLayout {
  return updatePage(doc, layer, section, (page) => {
    const t: LayoutTuning = { ...(page.tuning ?? {}) };
    if (v === undefined) delete t[dial];
    else t[dial] = Math.round(Math.min(1, Math.max(0, v)) * 100) / 100;
    return { ...page, tuning: tuningCount(t) ? t : undefined };
  });
}

/** Whether a page keeps the Home's lead titles off (Movies/TV), and from which layer. */
export function effectiveDedupe(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
): { on: boolean; from: "override" | "global" | "default" } {
  if (layer !== "global") {
    const own = pageOf(doc, layer, section)?.dedupeHome;
    if (own !== undefined) return { on: own, from: "override" };
  }
  const g = pageOf(doc, "global", section)?.dedupeHome;
  if (g !== undefined) return { on: g, from: "global" };
  return { on: true, from: "default" };
}

export function setDedupe(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  on: boolean | undefined,
): HomeLayout {
  return updatePage(doc, layer, section, (page) => ({ ...page, dedupeHome: on }));
}

/** Rows that can appear on a page. */
export function rowFits(row: { sections?: LayoutSection[] }, section: LayoutSection): boolean {
  return !row.sections || row.sections.length === 0 || row.sections.includes(section);
}

/** Drop empty pages and viewer layers; order pages and viewers canonically. */
export function prune(doc: HomeLayout): HomeLayout {
  const order = (ps: LayoutPage[]) =>
    ps
      .filter((p) => !emptyPage(p))
      .sort(
        (a, b) =>
          SECTIONS.findIndex((s) => s.value === a.section) -
          SECTIONS.findIndex((s) => s.value === b.section),
      );
  return {
    ...doc,
    global: order(doc.global ?? []),
    viewers: (doc.viewers ?? [])
      .map((v) => ({ ...v, pages: order(v.pages) }))
      .filter((v) => v.pages.length > 0)
      .sort((a, b) => a.userId - b.userId),
  };
}

/** The document without server stamps, for change detection. */
function comparable(doc: HomeLayout): unknown {
  const p = prune(doc);
  return { global: p.global, viewers: p.viewers };
}

export function sameLayout(a: HomeLayout, b: HomeLayout): boolean {
  return JSON.stringify(comparable(a)) === JSON.stringify(comparable(b));
}

/** Apply fn to a layer's page (created when missing), then prune. */
export function updatePage(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  fn: (p: LayoutPage) => LayoutPage,
): HomeLayout {
  const edit = (pages: LayoutPage[]) => {
    const i = pages.findIndex((p) => p.section === section);
    const cur: LayoutPage = i >= 0 ? (pages[i] as LayoutPage) : { section, rows: [] };
    const next = fn({ ...cur, rows: [...cur.rows] });
    return i >= 0 ? pages.map((p, j) => (j === i ? next : p)) : [...pages, next];
  };
  if (layer === "global") return prune({ ...doc, global: edit(doc.global ?? []) });
  const viewers = [...(doc.viewers ?? [])];
  const i = viewers.findIndex((v) => v.userId === layer);
  if (i >= 0)
    viewers[i] = {
      ...(viewers[i] as { userId: number; pages: LayoutPage[] }),
      pages: edit((viewers[i] as { pages: LayoutPage[] }).pages),
    };
  else viewers.push({ userId: layer, pages: edit([]) });
  return prune({ ...doc, viewers });
}

/** Normalise a policy the way the server does (status for anchors, neutral values dropped). */
export function tidyPolicy(p: RowPolicy): RowPolicy {
  const out: RowPolicy = { row: p.row, status: p.status };
  if (p.slot) {
    out.slot = p.slot;
    if (out.status !== "off") out.status = "always";
  } else {
    if (p.minSlot) out.minSlot = p.minSlot;
    if (p.maxSlot) out.maxSlot = p.maxSlot;
  }
  if (p.priority && p.priority !== 1) out.priority = p.priority;
  if (p.maxPerPage) out.maxPerPage = p.maxPerPage;
  if (p.maxDaysPerWeek && p.maxDaysPerWeek < 7) out.maxDaysPerWeek = p.maxDaysPerWeek;
  if (hasWhen(p.when)) out.when = p.when;
  return out;
}

/** Set (add or replace) a row's policy in a layer. Anchoring a slot frees it from any other row in that layer. */
export function setPolicy(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  policy: RowPolicy,
): HomeLayout {
  const p = tidyPolicy(policy);
  return updatePage(doc, layer, section, (page) => {
    let rows = page.rows.map((r) =>
      p.slot && r.row !== p.row && r.slot === p.slot
        ? tidyPolicy({ ...r, slot: undefined, status: r.status === "always" ? "always" : r.status })
        : r,
    );
    const i = rows.findIndex((r) => r.row === p.row);
    rows = i >= 0 ? rows.map((r, j) => (j === i ? p : r)) : [...rows, p];
    return { ...page, rows };
  });
}

/** Remove a row's policy from a layer ("reset to global" for a viewer, "back to default" for Global). */
export function removePolicy(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  row: string,
): HomeLayout {
  return updatePage(doc, layer, section, (page) => ({
    ...page,
    rows: page.rows.filter((r) => r.row !== row),
  }));
}

/** Set or clear (undefined) a page length, for all devices (device null) or one. */
export function setPageLength(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  device: Device | null,
  n: number | undefined,
): HomeLayout {
  return updatePage(doc, layer, section, (page) => {
    if (!device) return { ...page, maxRows: n && n > 0 ? Math.round(n) : undefined };
    const dm = { ...(page.deviceMaxRows ?? {}) };
    if (n && n > 0) dm[device] = Math.round(n);
    else delete dm[device];
    return { ...page, deviceMaxRows: Object.keys(dm).length ? dm : undefined };
  });
}

/** Remove every override a viewer has (on one page, or all). */
export function resetViewer(doc: HomeLayout, userId: number, section?: LayoutSection): HomeLayout {
  return prune({
    ...doc,
    viewers: (doc.viewers ?? []).map((v) =>
      v.userId !== userId
        ? v
        : { ...v, pages: section ? v.pages.filter((p) => p.section !== section) : [] },
    ),
  });
}

/** Anchored rows of a layer's page, by slot. */
export function anchorsOf(doc: HomeLayout, layer: LayerKey, section: LayoutSection): RowPolicy[] {
  return [...(pageOf(doc, layer, section)?.rows ?? [])]
    .filter((r) => r.slot)
    .sort((a, b) => (a.slot ?? 0) - (b.slot ?? 0));
}

/** Reorder a layer's anchors: the slots they hold are reassigned in the new order. */
export function reorderAnchors(
  doc: HomeLayout,
  layer: LayerKey,
  section: LayoutSection,
  rows: string[],
): HomeLayout {
  const anchors = anchorsOf(doc, layer, section);
  const slots = anchors.map((a) => a.slot as number);
  return updatePage(doc, layer, section, (page) => ({
    ...page,
    rows: page.rows.map((r) => {
      const i = rows.indexOf(r.row);
      return r.slot && i >= 0 && i < slots.length ? { ...r, slot: slots[i] } : r;
    }),
  }));
}

/** The first slot no anchor holds in a layer. */
export function freeSlot(doc: HomeLayout, layer: LayerKey, section: LayoutSection): number {
  const used = new Set(anchorsOf(doc, layer, section).map((a) => a.slot));
  let s = 1;
  while (used.has(s)) s++;
  return s;
}

/** Short chips that summarise a policy. */
export function policyChips(p: RowPolicy, dayNames: readonly string[]): string[] {
  const out: string[] = [];
  if (p.slot) out.push(`Slot ${p.slot}`);
  if (p.minSlot) out.push(`Not above ${p.minSlot}`);
  if (p.maxSlot) out.push(`Not below ${p.maxSlot}`);
  if (p.priority && p.priority !== 1)
    out.push(PRIORITIES.find((x) => x.value === p.priority)?.label ?? `×${p.priority}`);
  if (p.maxPerPage) out.push(`≤${p.maxPerPage} per page`);
  if (p.maxDaysPerWeek) out.push(`${p.maxDaysPerWeek} days a week`);
  if (hasWhen(p.when)) out.push(describeWhen(p.when, dayNames));
  return out;
}
