import { DashboardPanel } from "@braedonsaunders/appkit-dashboard/primitives";
import { Badge, Skeleton, cn } from "@braedonsaunders/appkit-ui";
import { Server } from "lucide-react";
import type { ReactNode } from "react";
import { api } from "../lib/api";
import { formatBytes, formatRate } from "../lib/format";
import { useAsync, useInterval } from "../lib/hooks";
import type { LoadGPU, SystemLoad } from "../lib/types";
import { Meter } from "./charts";
import { LiveChart, type LiveSeries } from "./LiveChart";

/** How often the frontend polls load: every 10 s; the server samples every 2 s. */
export const LOAD_POLL_MS = 10_000;

function pct(v: number | undefined): string {
  return v === undefined || !Number.isFinite(v) ? "—" : `${Math.round(v)}%`;
}

/** "NVIDIA GeForce RTX 4080" → "RTX 4080". */
function shortGPUName(name: string): string {
  return name.replace(/^(NVIDIA|AMD|Apple|Intel\(R\))\s+/i, "").replace(/^GeForce\s+/i, "") || name;
}

const PRIMARY = { stroke: "stroke-primary", fill: "fill-primary", swatch: "bg-primary" };

/**
 * One gauge in the Server stack: name, a large live reading and its context
 * on top, the five-minute trace filling the rest of its share of the panel.
 */
function Instrument({
  label,
  value,
  context,
  chart,
  className,
}: {
  label: string;
  value: ReactNode;
  context?: ReactNode;
  chart?: ReactNode;
  className?: string;
}) {
  return (
    <div
      role="group"
      aria-label={label}
      className={cn("flex min-h-0 flex-col gap-1.5 py-3 first:pt-0 last:pb-0", className)}
    >
      <div className="flex items-end justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[11px] font-medium uppercase tracking-wide text-fg-subtle">{label}</p>
          <p className="truncate text-2xl font-semibold leading-tight text-fg">{value}</p>
        </div>
        {context ? (
          <div className="min-w-0 shrink text-right text-xs text-fg-muted">{context}</div>
        ) : null}
      </div>
      {chart}
    </div>
  );
}

/** Chart box: a fixed band when the page scrolls, its share of the height in the cockpit. */
const chartBox = "h-20 cockpit:h-auto cockpit:flex-1";

/**
 * This server's live CPU, memory, network and GPU load as a stack of
 * instruments with five-minute traces, plus what Ether itself is using.
 */
export function ServerPanel({ hwaccel, className }: { hwaccel?: string; className?: string }) {
  const load = useAsync((signal) => api.systemLoad(signal), []);
  useInterval(() => void load.reload(), LOAD_POLL_MS);
  const data = load.data;
  return (
    <DashboardPanel
      title="Server"
      icon={<Server size={16} />}
      className={className}
      bodyClassName="flex min-h-0 flex-col"
      actions={
        <span className="flex items-center gap-2">
          {hwaccel ? <Badge variant="secondary">{hwaccel}</Badge> : null}
          <span className="flex items-center gap-1.5 text-xs text-fg-muted">
            <span
              className={cn(
                "size-1.5 rounded-full",
                data && !data.error ? "bg-success" : "bg-fg-subtle",
              )}
            />
            {data && !data.error ? "Live" : "Waiting"}
          </span>
        </span>
      }
    >
      {data?.error || (load.error && !data) ? (
        <p className="text-sm text-fg-muted">
          Load isn't available{data?.error ? `: ${data.error}` : "."}
        </p>
      ) : !data ? (
        <div className="flex flex-1 flex-col gap-3">
          {Array.from({ length: 4 }, (_, i) => (
            <Skeleton key={i} className="h-20 flex-1 rounded-lg" />
          ))}
        </div>
      ) : (
        <Vitals data={data} />
      )}
    </DashboardPanel>
  );
}

function Vitals({ data }: { data: SystemLoad }) {
  const cur = data.current;
  const h = data.history;
  const times = h.map((s) => s.at);
  const memPct = (v: number) => (data.memTotal ? (v / data.memTotal) * 100 : 0);
  const gpu: LoadGPU | undefined = [...data.gpus].sort(
    (a, b) => (b.util ?? -1) - (a.util ?? -1),
  )[0];
  const one = (key: string, label: string, values: number[]): LiveSeries[] => [
    { key, label, values, ...PRIMARY },
  ];
  const net: LiveSeries[] = [
    {
      key: "down",
      label: "Down",
      values: h.map((s) => s.netRxBps),
      stroke: "stroke-series-down",
      fill: "fill-series-down",
      swatch: "bg-series-down",
    },
    {
      key: "up",
      label: "Up",
      values: h.map((s) => s.netTxBps),
      stroke: "stroke-series-up",
      fill: "fill-series-up",
      swatch: "bg-series-up",
    },
  ];
  const ether = [
    cur ? `${pct(cur.procCpu)} CPU` : null,
    cur ? formatBytes(cur.procRss) : null,
  ].filter(Boolean);

  return (
    <>
      <div className="flex min-h-0 flex-1 flex-col divide-y divide-border-subtle">
        <Instrument
          label="CPU"
          value={pct(cur?.cpu)}
          context={[
            data.load?.length ? `load ${data.load[0]!.toFixed(2)}` : null,
            `${data.cores} cores`,
          ]
            .filter(Boolean)
            .join(" · ")}
          className="cockpit:flex-1"
          chart={
            <LiveChart
              className={chartBox}
              series={one(
                "cpu",
                "CPU",
                h.map((s) => s.cpu),
              )}
              times={times}
              max={100}
              format={(v) => `${Math.round(v)}%`}
              area
              ariaLabel={`CPU use over the last 5 minutes, now ${pct(cur?.cpu)}`}
            />
          }
        />
        <Instrument
          label="Memory"
          value={cur ? pct(memPct(cur.memUsed)) : "—"}
          context={
            <>
              {cur ? `${formatBytes(cur.memUsed)} of ${formatBytes(data.memTotal)}` : null}
              {data.swapUsed > 0 ? (
                <span className="block">swap {formatBytes(data.swapUsed)}</span>
              ) : null}
            </>
          }
          className="cockpit:flex-1"
          chart={
            <LiveChart
              className={chartBox}
              series={one(
                "mem",
                "Memory",
                h.map((s) => memPct(s.memUsed)),
              )}
              times={times}
              max={100}
              format={(v) => `${Math.round(v)}%`}
              area
              ariaLabel={`Memory in use over the last 5 minutes, now ${cur ? pct(memPct(cur.memUsed)) : "unknown"}`}
            />
          }
        />
        <Instrument
          label="Network"
          value={
            <span className="flex flex-wrap items-baseline gap-x-3">
              <span className="flex items-center gap-1.5">
                <span className="size-2 rounded-full bg-series-down" aria-hidden />
                <span aria-label="Download">↓</span> {cur ? formatRate(cur.netRxBps) : "—"}
              </span>
              <span className="flex items-center gap-1.5 text-lg text-fg-muted">
                <span className="size-2 rounded-full bg-series-up" aria-hidden />
                <span aria-label="Upload">↑</span> {cur ? formatRate(cur.netTxBps) : "—"}
              </span>
            </span>
          }
          className="cockpit:flex-1"
          chart={
            <LiveChart
              className={chartBox}
              series={net}
              times={times}
              format={(v) => formatRate(v).replace("—", "0")}
              ariaLabel={`Network traffic over the last 5 minutes, now ${cur ? `${formatRate(cur.netRxBps)} down and ${formatRate(cur.netTxBps)} up` : "unknown"}`}
            />
          }
        />
        <Instrument
          label={gpu ? `GPU · ${shortGPUName(gpu.name)}` : "GPU"}
          value={gpu ? pct(gpu.util) : "—"}
          context={
            gpu ? (
              gpu.encoder !== undefined || gpu.decoder !== undefined ? (
                <div className="w-32 space-y-1">
                  <Meter
                    value={(gpu.encoder ?? 0) / 100}
                    label="Encode"
                    valueLabel={pct(gpu.encoder)}
                  />
                  <Meter
                    value={(gpu.decoder ?? 0) / 100}
                    label="Decode"
                    valueLabel={pct(gpu.decoder)}
                  />
                </div>
              ) : gpu.memUsed ? (
                gpu.memTotal ? (
                  `${formatBytes(gpu.memUsed)} of ${formatBytes(gpu.memTotal)}`
                ) : (
                  `${formatBytes(gpu.memUsed)} in use`
                )
              ) : null
            ) : (
              "Not reported by this hardware"
            )
          }
          className={gpu ? "cockpit:flex-1" : undefined}
          chart={
            gpu ? (
              <LiveChart
                className={chartBox}
                series={one(
                  "gpu",
                  "GPU",
                  h.map((s) => s.gpu ?? 0),
                )}
                times={times}
                max={100}
                format={(v) => `${Math.round(v)}%`}
                area
                ariaLabel={`GPU use over the last 5 minutes, now ${pct(gpu.util)}`}
              />
            ) : undefined
          }
        />
      </div>
      <p className="mt-3 flex shrink-0 items-center justify-between gap-2 border-t border-border-subtle pt-3 text-xs text-fg-muted">
        <span>Ether itself</span>
        <span className="tabular-nums text-fg">{ether.join(" · ") || "—"}</span>
      </p>
    </>
  );
}
