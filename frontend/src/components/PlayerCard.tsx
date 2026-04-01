import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@clerk/clerk-react";
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { apiFetch } from "../lib/api";
import { clerkEnabled } from "../lib/clerk";
import { useWatchlist } from "../context/WatchlistContext";
import KeyValueGrid from "./KeyValueGrid";
import DataTable from "./DataTable";

type Props = { mlbamId: number };

const mockGds = [
  { gds: 52, perf: 0.31 },
  { gds: 61, perf: 0.28 },
  { gds: 74, perf: 0.35 },
  { gds: 48, perf: 0.22 },
  { gds: 68, perf: 0.33 },
];

function ArrivalGauge({ p }: { p: number }) {
  const pct = Math.round(p * 100);
  return (
    <div className="rounded-lg border border-white/10 p-4 bg-white/5">
      <h3 className="text-sm text-scout-chalk/70 mb-2">MLB arrival (model)</h3>
      <div className="h-3 rounded-full bg-white/10 overflow-hidden">
        <div className="h-full bg-scout-clay transition-all" style={{ width: `${pct}%` }} />
      </div>
      <p className="text-2xl font-display text-scout-chalk mt-2">{pct}%</p>
    </div>
  );
}

function SalaryBars() {
  return (
    <div className="rounded-lg border border-white/10 p-4 bg-white/5">
      <h3 className="text-sm text-scout-chalk/70 mb-2">Salary projection (stub)</h3>
      <div className="flex gap-2 items-end h-24">
        {[40, 65, 55, 80].map((h, i) => (
          <div key={i} className="flex-1 bg-scout-field/80 rounded-t" style={{ height: `${h}%` }} />
        ))}
      </div>
    </div>
  );
}

function PlayerCardInner({ mlbamId, token }: { mlbamId: number; token?: string | null }) {
  const qc = useQueryClient();
  const { add, has } = useWatchlist();

  const mlbQ = useQuery({
    queryKey: ["mlb-player", mlbamId],
    queryFn: () =>
      apiFetch(`/mlb/player/${mlbamId}`) as Promise<{
        profile?: Record<string, unknown>;
        career_hitting?: Record<string, unknown>[];
        career_pitching?: Record<string, unknown>[];
      }>,
  });

  const milbQ = useQuery({
    queryKey: ["milb-stats", mlbamId, token],
    queryFn: () =>
      apiFetch(`/api/players/${mlbamId}/milb-stats`, {}, token ?? undefined) as Promise<{
        ingested?: boolean;
        batting?: Record<string, unknown>[];
        pitching?: Record<string, unknown>[];
      }>,
  });

  const q = useQuery({
    queryKey: ["prediction", mlbamId, token],
    queryFn: () => apiFetch(`/api/predictions/${mlbamId}/latest`, {}, token ?? undefined),
  });

  const gen = useMutation({
    mutationFn: () =>
      apiFetch(`/api/predictions/generate/${mlbamId}`, { method: "POST" }, token ?? undefined),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["prediction", mlbamId] }),
  });

  const profile = mlbQ.data?.profile;
  const displayName =
    (profile?.full_name as string | undefined) ||
    (profile?.fullName as string | undefined) ||
    `Player ${mlbamId}`;
  const subtitle = (profile?.primary_position as string | undefined) || "";

  const pred = q.data as { prediction?: Record<string, unknown> } | undefined;
  const probRaw = pred?.prediction?.mlb_probability;
  const prob = typeof probRaw === "number" ? probRaw : 0.42;

  const onWatchlist = has(mlbamId);

  return (
    <div className="max-w-3xl mx-auto space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="font-display text-2xl text-scout-chalk">{displayName}</h1>
          <p className="text-sm text-scout-chalk/60">
            {subtitle ? `${subtitle} · ` : ""}MLB {mlbamId}
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            className="rounded-md bg-scout-clay px-4 py-2 text-scout-ink text-sm font-medium disabled:opacity-50"
            disabled={gen.isPending}
            onClick={() => gen.mutate()}
          >
            {gen.isPending ? "Generating…" : "Generate report"}
          </button>
          <button
            type="button"
            className="rounded-md border border-white/20 px-4 py-2 text-scout-chalk text-sm disabled:opacity-50"
            disabled={onWatchlist}
            onClick={() => void add(mlbamId)}
          >
            {onWatchlist ? "On watchlist" : "Add to watchlist"}
          </button>
        </div>
      </div>

      {mlbQ.isLoading && <p className="text-scout-chalk/60 text-sm">Loading MLB profile…</p>}
      {mlbQ.error && <p className="text-red-300 text-sm">{(mlbQ.error as Error).message}</p>}

      {profile && Object.keys(profile).length > 0 && (
        <section className="rounded-lg border border-white/10 p-4 bg-white/5">
          <h2 className="text-sm font-medium text-scout-chalk/80 mb-2">MLB profile</h2>
          <KeyValueGrid data={profile} />
        </section>
      )}

      {(mlbQ.data?.career_hitting?.length ?? 0) > 0 || (mlbQ.data?.career_pitching?.length ?? 0) > 0 ? (
        <div className="grid md:grid-cols-2 gap-4">
          <div>
            <h2 className="text-sm text-scout-chalk/80 mb-2">Career hitting (MLB API)</h2>
            <DataTable rows={mlbQ.data?.career_hitting ?? []} />
          </div>
          <div>
            <h2 className="text-sm text-scout-chalk/80 mb-2">Career pitching (MLB API)</h2>
            <DataTable rows={mlbQ.data?.career_pitching ?? []} />
          </div>
        </div>
      ) : null}

      {milbQ.data && milbQ.data.ingested !== false && ((milbQ.data.batting?.length ?? 0) > 0 || (milbQ.data.pitching?.length ?? 0) > 0) && (
        <div className="grid md:grid-cols-2 gap-4">
          <div>
            <h2 className="text-sm text-scout-chalk/80 mb-2">MiLB batting (warehouse)</h2>
            <DataTable rows={milbQ.data.batting ?? []} />
          </div>
          <div>
            <h2 className="text-sm text-scout-chalk/80 mb-2">MiLB pitching (warehouse)</h2>
            <DataTable rows={milbQ.data.pitching ?? []} />
          </div>
        </div>
      )}

      {milbQ.data?.ingested === false && (
        <p className="text-sm text-scout-chalk/50">
          No MiLB rows in the warehouse for this player yet — run ingest from Data tools if you need scraped minors
          stats.
        </p>
      )}

      {q.isLoading && <p className="text-scout-chalk/60">Loading prediction…</p>}
      {q.error && <p className="text-red-300 text-sm">{(q.error as Error).message}</p>}

      <div className="grid gap-4 md:grid-cols-2">
        <ArrivalGauge p={prob} />
        <SalaryBars />
      </div>

      <div className="rounded-lg border border-white/10 p-4 bg-white/5 h-72">
        <h3 className="text-sm text-scout-chalk/70 mb-2">Opponent quality (GDS)</h3>
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#ffffff22" />
            <XAxis type="number" dataKey="perf" name="Perf" stroke="#f4f1ea88" />
            <YAxis type="number" dataKey="gds" name="GDS" stroke="#f4f1ea88" />
            <Tooltip cursor={{ strokeDasharray: "3 3" }} contentStyle={{ background: "#0c1426", border: "1px solid #fff3" }} />
            <Scatter name="Games" data={mockGds} fill="#c45c3e" />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default function PlayerCard({ mlbamId }: Props) {
  if (!clerkEnabled()) {
    return <PlayerCardInner mlbamId={mlbamId} token={undefined} />;
  }
  return <PlayerCardClerk mlbamId={mlbamId} />;
}

function PlayerCardClerk({ mlbamId }: { mlbamId: number }) {
  const { getToken, isLoaded } = useAuth();
  if (!isLoaded) {
    return <p className="text-scout-chalk/60">Loading auth…</p>;
  }
  return (
    <PlayerCardWithToken
      mlbamId={mlbamId}
      tokenFetch={async () => {
        try {
          return await getToken();
        } catch {
          return null;
        }
      }}
    />
  );
}

function PlayerCardWithToken({
  mlbamId,
  tokenFetch,
}: {
  mlbamId: number;
  tokenFetch: () => Promise<string | null>;
}) {
  const [token, setToken] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    tokenFetch().then((t) => {
      if (!cancelled) {
        setToken(t);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [tokenFetch]);

  return <PlayerCardInner mlbamId={mlbamId} token={token} />;
}
