import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { useAuth } from "@clerk/clerk-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiFetch } from "../lib/api";

const LS_KEY = "scoutpro_watchlist";

export type WatchlistContextValue = {
  ids: number[];
  isLoading: boolean;
  add: (mlbamId: number) => Promise<void>;
  remove: (mlbamId: number) => Promise<void>;
  has: (mlbamId: number) => boolean;
};

const WatchlistContext = createContext<WatchlistContextValue | null>(null);

function readLocal(): number[] {
  try {
    const raw = localStorage.getItem(LS_KEY);
    return raw ? (JSON.parse(raw) as number[]) : [];
  } catch {
    return [];
  }
}

function writeLocal(ids: number[]) {
  localStorage.setItem(LS_KEY, JSON.stringify(ids));
}

function LocalWatchlistProvider({ children }: { children: React.ReactNode }) {
  const [ids, setIds] = useState<number[]>(readLocal);

  const add = useCallback(async (mlbamId: number) => {
    setIds((prev) => {
      if (prev.includes(mlbamId)) return prev;
      const next = [...prev, mlbamId];
      writeLocal(next);
      return next;
    });
  }, []);

  const remove = useCallback(async (mlbamId: number) => {
    setIds((prev) => {
      const next = prev.filter((x) => x !== mlbamId);
      writeLocal(next);
      return next;
    });
  }, []);

  const value = useMemo<WatchlistContextValue>(
    () => ({
      ids,
      isLoading: false,
      add,
      remove,
      has: (id) => ids.includes(id),
    }),
    [ids, add, remove],
  );

  return <WatchlistContext.Provider value={value}>{children}</WatchlistContext.Provider>;
}

function ClerkWatchlistProvider({ children }: { children: React.ReactNode }) {
  const { getToken, isLoaded, isSignedIn } = useAuth();
  const qc = useQueryClient();

  const listQuery = useQuery({
    queryKey: ["watchlist"],
    enabled: isLoaded && !!isSignedIn,
    queryFn: async () => {
      const t = await getToken();
      const data = (await apiFetch("/api/watchlist", {}, t ?? undefined)) as {
        items?: { mlbam_id: number }[];
      };
      return (data.items ?? []).map((x) => x.mlbam_id);
    },
  });

  const [migrated, setMigrated] = useState(false);

  useEffect(() => {
    if (!isLoaded || !isSignedIn || !listQuery.isSuccess || listQuery.data === undefined || migrated) {
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const fromLs = readLocal();
        if (fromLs.length === 0) {
          setMigrated(true);
          return;
        }
        const server = new Set(listQuery.data);
        const toAdd = fromLs.filter((id) => !server.has(id));
        const t = await getToken();
        for (const id of toAdd) {
          if (cancelled) return;
          await apiFetch(`/api/watchlist/${id}`, { method: "POST" }, t ?? undefined);
        }
        localStorage.removeItem(LS_KEY);
        if (!cancelled) {
          setMigrated(true);
          qc.invalidateQueries({ queryKey: ["watchlist"] });
        }
      } catch {
        setMigrated(true);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [isLoaded, isSignedIn, listQuery.isSuccess, listQuery.data, migrated, getToken, qc]);

  const addMut = useMutation({
    mutationFn: async (mlbamId: number) => {
      const t = await getToken();
      await apiFetch(`/api/watchlist/${mlbamId}`, { method: "POST" }, t ?? undefined);
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["watchlist"] }),
  });

  const removeMut = useMutation({
    mutationFn: async (mlbamId: number) => {
      const t = await getToken();
      await apiFetch(`/api/watchlist/${mlbamId}`, { method: "DELETE" }, t ?? undefined);
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["watchlist"] }),
  });

  const ids = listQuery.data ?? [];
  const value = useMemo<WatchlistContextValue>(
    () => ({
      ids,
      isLoading: !isLoaded || (!!isSignedIn && listQuery.isLoading),
      add: (mlbamId: number) => addMut.mutateAsync(mlbamId),
      remove: (mlbamId: number) => removeMut.mutateAsync(mlbamId),
      has: (id) => ids.includes(id),
    }),
    [ids, isLoaded, isSignedIn, listQuery.isLoading, addMut, removeMut],
  );

  if (!isLoaded) {
    return (
      <WatchlistContext.Provider
        value={{
          ids: [],
          isLoading: true,
          add: async () => {},
          remove: async () => {},
          has: () => false,
        }}
      >
        {children}
      </WatchlistContext.Provider>
    );
  }

  if (!isSignedIn) {
    return <LocalWatchlistProvider>{children}</LocalWatchlistProvider>;
  }

  return <WatchlistContext.Provider value={value}>{children}</WatchlistContext.Provider>;
}

export function WatchlistProvider({ children }: { children: React.ReactNode }) {
  const k = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY || "";
  if (k.startsWith("pk_")) {
    return <ClerkWatchlistProvider>{children}</ClerkWatchlistProvider>;
  }
  return <LocalWatchlistProvider>{children}</LocalWatchlistProvider>;
}

export function useWatchlist(): WatchlistContextValue {
  const ctx = useContext(WatchlistContext);
  if (!ctx) {
    throw new Error("useWatchlist must be used within WatchlistProvider");
  }
  return ctx;
}
