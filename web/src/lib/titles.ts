import { useEffect, useState } from "react";
import { api } from "./api";
import type { TitleDetail } from "./contract";

const cache = new Map<string, Promise<TitleDetail | null>>();

/** Title metadata (artwork) by rating key, cached for the session; null when unknown. */
function loadTitle(key: string): Promise<TitleDetail | null> {
  let p = cache.get(key);
  if (!p) {
    p = api.title(key).catch(() => null);
    cache.set(key, p);
  }
  return p;
}

export function useTitle(key: string | undefined): TitleDetail | null {
  const [t, setT] = useState<TitleDetail | null>(null);
  useEffect(() => {
    if (!key) {
      setT(null);
      return;
    }
    let live = true;
    void loadTitle(key).then((v) => {
      if (live) setT(v);
    });
    return () => {
      live = false;
    };
  }, [key]);
  return t;
}
