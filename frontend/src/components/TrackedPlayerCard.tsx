import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "../lib/api";
import { clerkEnabled } from "../lib/clerk";

type MlbPlayerPayload = {
  profile?: Record<string, unknown>;
  career_hitting?: Record<string, unknown>[];
  career_pitching?: Record<string, unknown>[];
};

type PredPayload = {
  prediction?: { mlb_probability?: number } | null;
};

type MilbPayload = {
  ingested?: boolean;
  batting?: Record<string, unknown>[];
  pitching?: Record<string, unknown>[];
};

type Props = {
  mlbamId: number;
  getToken?: () => Promise<string | null>;
};

export default function TrackedPlayerCard({ mlbamId, getToken }: Props) {
  const needToken = clerkEnabled();

  const mlb = useQuery({
    queryKey: ["mlb-player", mlbamId],
    queryFn: () => apiFetch(`/mlb/player/${mlbamId}`) as Promise<MlbPlayerPayload>,
  });

  const pred = useQuery({
    queryKey: ["prediction", mlbamId, needToken],
    queryFn: async () => {
      const t = needToken && getToken ? await getToken() : undefined;
      return apiFetch(`/api/predictions/${mlbamId}/latest`, {}, t ?? undefined) as Promise<PredPayload>;
    },
  });

  const milb = useQuery({
    queryKey: ["milb-stats", mlbamId, needToken],
    queryFn: async () => {
      const t = needToken && getToken ? await getToken() : undefined;
      return apiFetch(`/api/players/${mlbamId}/milb-stats`, {}, t ?? undefined) as Promise<MilbPayload>;
    },
  });

  const name =
    (mlb.data?.profile?.full_name as string | undefined) ||
    (mlb.data?.profile?.fullName as string | undefined) ||
    `Player ${mlbamId}`;
  const pos = (mlb.data?.profile?.primary_position as string | undefined) || "—";
  const prob = pred.data?.prediction?.mlb_probability;
  const probLabel = typeof prob === "number" ? `${Math.round(prob * 100)}% MLB prob.` : "—";

  const lastBat = mlb.data?.career_hitting?.[0];
  const lastPitch = mlb.data?.career_pitching?.[0];
  const statHint =
    lastBat && typeof lastBat === "object"
      ? `OPS ${String((lastBat as { ops?: unknown }).ops ?? "—")}`
      : lastPitch && typeof lastPitch === "object"
        ? `ERA ${String((lastPitch as { era?: unknown }).era ?? "—")}`
        : "—";

  const milbRows = (milb.data?.batting?.length ?? 0) + (milb.data?.pitching?.length ?? 0);

  return (
    <article className="rounded-xl border border-white/10 bg-white/5 p-4 flex flex-col gap-3 min-h-[140px]">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <h3 className="font-display text-lg text-scout-chalk leading-tight">{name}</h3>
          <p className="text-xs text-scout-chalk/60">
            {pos} · MLB {mlbamId}
          </p>
        </div>
        <Link
          to={`/player/${mlbamId}`}
          className="text-sm rounded-md bg-scout-clay/90 px-3 py-1.5 text-scout-ink font-medium hover:opacity-90"
        >
          Profile
        </Link>
      </div>
      {mlb.isLoading && <p className="text-xs text-scout-chalk/50">Loading…</p>}
      {mlb.error && <p className="text-xs text-red-300">{(mlb.error as Error).message}</p>}
      <dl className="grid grid-cols-2 gap-2 text-xs text-scout-chalk/80">
        <div>
          <dt className="text-scout-chalk/50">Projection</dt>
          <dd>{pred.isLoading ? "…" : probLabel}</dd>
        </div>
        <div>
          <dt className="text-scout-chalk/50">Career (MLB API)</dt>
          <dd>{statHint}</dd>
        </div>
        <div className="col-span-2">
          <dt className="text-scout-chalk/50">Warehouse MiLB rows</dt>
          <dd>
            {milb.isLoading
              ? "…"
              : milb.data?.ingested === false
                ? "Not ingested"
                : `${milbRows} row(s)`}
          </dd>
        </div>
      </dl>
    </article>
  );
}
