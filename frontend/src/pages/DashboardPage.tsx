import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useAuth } from "@clerk/clerk-react";
import { useQueries } from "@tanstack/react-query";
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "../lib/api";
import { clerkEnabled } from "../lib/clerk";
import { useWatchlist } from "../context/WatchlistContext";
import ScrapeIngestPanel from "../components/ScrapeIngestPanel";
import WatchlistSidebar from "../components/WatchlistSidebar";
import TrackedPlayerCard from "../components/TrackedPlayerCard";

function DashboardBody({ getToken }: { getToken?: () => Promise<string | null> }) {
  const { ids, isLoading: wlLoading } = useWatchlist();

  const mlbPreviews = useQueries({
    queries: ids.map((id) => ({
      queryKey: ["mlb-player", id],
      queryFn: () => apiFetch(`/mlb/player/${id}`) as Promise<{ profile?: { full_name?: string } }>,
    })),
  });

  const nameById = useMemo(() => {
    const m: Record<number, string> = {};
    ids.forEach((id, i) => {
      const q = mlbPreviews[i];
      const n = q.data?.profile?.full_name;
      if (n) m[id] = n;
    });
    return m;
  }, [ids, mlbPreviews]);

  const [advancedOpen, setAdvancedOpen] = useState(false);

  const health = useQuery({
    queryKey: ["health-detail"],
    queryFn: () => apiFetch("/api/health/detail"),
  });

  return (
    <div className="min-h-screen flex flex-col md:flex-row bg-scout-ink">
      <WatchlistSidebar nameById={nameById} />
      <div className="flex-1 p-6 space-y-6">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h1 className="font-display text-2xl text-scout-chalk">Tracked players</h1>
          <Link to="/search" className="text-sm text-scout-clay hover:underline">
            Search prospects
          </Link>
        </div>

        {wlLoading && <p className="text-sm text-scout-chalk/60">Loading watchlist…</p>}

        {!wlLoading && ids.length === 0 && (
          <div className="rounded-lg border border-white/10 p-8 bg-white/5 text-center space-y-3">
            <p className="text-scout-chalk/80">No players tracked yet.</p>
            <p className="text-sm text-scout-chalk/50">Search for a prospect and add them from the profile page.</p>
            <Link to="/search" className="inline-block text-scout-clay underline">
              Go to search
            </Link>
          </div>
        )}

        {ids.length > 0 && (
          <section>
            <h2 className="text-sm font-medium text-scout-chalk/80 mb-3">Your board</h2>
            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
              {ids.map((id) => (
                <TrackedPlayerCard key={id} mlbamId={id} getToken={getToken} />
              ))}
            </div>
          </section>
        )}

        <section className="border-t border-white/10 pt-4">
          <button
            type="button"
            onClick={() => setAdvancedOpen((o) => !o)}
            className="text-sm text-scout-chalk/60 hover:text-scout-chalk mb-2"
          >
            {advancedOpen ? "▼" : "▶"} Advanced (API health & data pipeline)
          </button>
          {advancedOpen && (
            <div className="space-y-4">
              <div className="rounded-lg border border-white/10 p-4 bg-white/5">
                <h2 className="text-sm font-medium text-scout-chalk/80 mb-2">API health</h2>
                {health.isLoading && <p className="text-scout-chalk/60 text-sm">Loading…</p>}
                {health.data && (
                  <pre className="text-xs text-emerald-200/90 overflow-x-auto">{JSON.stringify(health.data, null, 2)}</pre>
                )}
                {health.error && <p className="text-red-300 text-sm">{(health.error as Error).message}</p>}
              </div>
              <ScrapeIngestPanel getToken={getToken ? () => getToken() : undefined} />
              <p className="text-sm text-scout-chalk/50">
                Example profile:{" "}
                <Link to="/player/660670" className="text-scout-clay underline">
                  Jackson Holliday (660670)
                </Link>
              </p>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

function DashboardClerk() {
  const { getToken, isLoaded } = useAuth();
  if (!isLoaded) {
    return <p className="p-6 text-scout-chalk/60">Loading…</p>;
  }
  return <DashboardBody getToken={() => getToken()} />;
}

export default function DashboardPage() {
  if (clerkEnabled()) {
    return <DashboardClerk />;
  }
  return <DashboardBody />;
}
