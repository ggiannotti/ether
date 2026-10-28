import {
  Badge,
  Button,
  EmptyState,
  Input,
  Switch,
  confirmDialog,
  toast,
  cn,
} from "@braedonsaunders/appkit-ui";
import { RefreshCw, ScrollText, Search, Trash2 } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { ErrorPanel, LoadingRows } from "../components/feedback";
import { Segmented } from "../components/fields";
import { api, errorMessage } from "../lib/api";
import { useDebouncedValue } from "../lib/hooks";
import type { LogEntry } from "../lib/types";

const LEVELS = [
  { value: "", label: "All" },
  { value: "debug", label: "Debug+" },
  { value: "info", label: "Info+" },
  { value: "warn", label: "Warnings+" },
  { value: "error", label: "Errors" },
];

function levelVariant(l: string): "destructive" | "warning" | "info" | "secondary" {
  const v = l.toLowerCase();
  return v.startsWith("err")
    ? "destructive"
    : v.startsWith("warn")
      ? "warning"
      : v === "info"
        ? "info"
        : "secondary";
}

export default function Logs() {
  const [entries, setEntries] = useState<LogEntry[] | null>(null);
  const [total, setTotal] = useState(0);
  const [level, setLevel] = useState("");
  const [q, setQ] = useState("");
  const query = useDebouncedValue(q, 250);
  const [follow, setFollow] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    try {
      const r = await api.logs({ limit: 200, level, q: query });
      setEntries(
        [...(r.entries ?? [])].reverse().sort((a, b) => Date.parse(b.time) - Date.parse(a.time)),
      );
      setTotal(r.total);
      setError(null);
    } catch (e) {
      setError(e);
    }
  }, [level, query]);

  useEffect(() => {
    void load();
  }, [load]);
  useEffect(() => {
    if (!follow) return;
    const t = window.setInterval(() => void load(), 2000);
    return () => window.clearInterval(t);
  }, [follow, load]);

  const clear = async () => {
    if (
      !(await confirmDialog({
        title: "Clear logs?",
        message: "The in-memory log buffer is emptied. Log files on disk are not affected.",
        confirmLabel: "Clear",
        tone: "danger",
      }))
    )
      return;
    try {
      await api.clearLogs();
      await load();
      toast.success("Logs cleared");
    } catch (e) {
      toast.error("Couldn't clear logs", { description: errorMessage(e) });
    }
  };

  return (
    <section aria-label="Logs" className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-sm text-fg-muted">
          Newest entries first. Showing {entries?.length ?? 0} of {total.toLocaleString()}{" "}
          entries retained in memory.
        </p>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => void load()}>
            <RefreshCw size={14} /> Reload
          </Button>
          <Button variant="ghost" size="sm" onClick={() => void clear()}>
            <Trash2 size={14} /> Clear
          </Button>
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-3" role="search">
        <div className="relative min-w-52 flex-1 sm:max-w-sm">
          <Search
            size={14}
            className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-fg-subtle"
            aria-hidden
          />
          <Input
            type="search"
            className="h-9 pl-8"
            placeholder="Filter messages…"
            value={q}
            onChange={(e) => setQ(e.currentTarget.value)}
            aria-label="Filter logs"
          />
        </div>
        <Segmented
          ariaLabel="Minimum level"
          size="sm"
          value={level}
          onChange={setLevel}
          options={LEVELS}
        />
        <label className="flex items-center gap-2 text-sm text-fg-muted">
          <Switch checked={follow} onChange={(e) => setFollow(e.currentTarget.checked)} /> Live
          updates
        </label>
      </div>

      {error ? (
        <ErrorPanel error={error} onRetry={() => void load()} />
      ) : !entries ? (
        <LoadingRows rows={6} />
      ) : entries.length === 0 ? (
        <EmptyState
          icon={<ScrollText />}
          title="No log entries match"
          description={
            q || level
              ? "Try a broader filter."
              : "The server hasn't logged anything since it started or was last cleared."
          }
        />
      ) : (
        <div
          className="overflow-hidden rounded-xl border border-border bg-surface"
          role="log"
          aria-label="Server logs"
        >
          <div className="max-h-[calc(100vh-15rem)] overflow-auto font-mono text-xs">
            {entries.map((e, i) => (
              <div
                key={i}
                className={cn(
                  "flex flex-wrap items-baseline gap-x-3 gap-y-0.5 border-b border-border-subtle px-3 py-1.5 last:border-b-0",
                  e.level.toLowerCase().startsWith("err") && "bg-danger-subtle/40",
                )}
              >
                <time
                  className="shrink-0 tabular-nums text-fg-subtle"
                  dateTime={e.time}
                  title={new Date(e.time).toLocaleString()}
                >
                  {new Date(e.time).toLocaleTimeString()}
                </time>
                <Badge
                  variant={levelVariant(e.level)}
                  className="w-14 justify-center font-mono text-[10px] uppercase"
                >
                  {e.level}
                </Badge>
                <span className="min-w-0 flex-1 break-words text-fg">{e.message}</span>
                {e.attrs && Object.keys(e.attrs).length ? (
                  <span className="w-full break-all pl-[calc(4.5rem+3.5rem)] text-fg-muted sm:w-auto sm:pl-0">
                    {Object.entries(e.attrs).map(([k, v]) => (
                      <span key={k} className="mr-2">
                        <span className="text-fg-subtle">{k}=</span>
                        {v}
                      </span>
                    ))}
                  </span>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}
