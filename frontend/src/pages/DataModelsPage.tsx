import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@clerk/clerk-react";
import { apiFetch } from "../lib/api";
import { clerkEnabled } from "../lib/clerk";

type Summary = Record<string, unknown>;
type CardPayload = { manifest: Record<string, unknown> | null; registry: Record<string, unknown>[]; artifact_dir: string };

function DataModelsInner({ getToken }: { getToken?: () => Promise<string | null> }) {
  const summary = useQuery({
    queryKey: ["data-summary", "v3", !!getToken],
    queryFn: async () => {
      const t = getToken ? await getToken() : undefined;
      return apiFetch("/api/data/summary?feature_version=v3", {}, t ?? undefined) as Promise<Summary>;
    },
  });

  const card = useQuery({
    queryKey: ["model-card", !!getToken],
    queryFn: async () => {
      const t = getToken ? await getToken() : undefined;
      return apiFetch("/api/models/card", {}, t ?? undefined) as Promise<CardPayload>;
    },
  });

  const glossary = useQuery({
    queryKey: ["feature-glossary", !!getToken],
    queryFn: async () => {
      const t = getToken ? await getToken() : undefined;
      return apiFetch("/api/data/glossary", {}, t ?? undefined) as Promise<Record<string, string>>;
    },
  });

  return (
    <div className="max-w-4xl mx-auto px-4 py-8 space-y-10 text-scout-chalk">
      <div>
        <h1 className="font-display text-3xl mb-2">Data &amp; models</h1>
        <p className="text-scout-chalk/70 text-sm">
          Dataset health, trained arrival models, and feature definitions for ScoutPro v3 (first-K MiLB seasons cutoff).
        </p>
      </div>

      <section className="rounded-xl border border-white/10 bg-white/5 p-6 space-y-3">
        <h2 className="font-display text-xl">Dataset health</h2>
        {summary.isLoading && <p className="text-sm text-scout-chalk/60">Loading…</p>}
        {summary.error && <p className="text-sm text-red-300">{(summary.error as Error).message}</p>}
        {summary.data && (
          <pre className="text-xs text-emerald-200/90 overflow-x-auto whitespace-pre-wrap">
            {JSON.stringify(summary.data, null, 2)}
          </pre>
        )}
      </section>

      <section className="rounded-xl border border-white/10 bg-white/5 p-6 space-y-3">
        <h2 className="font-display text-xl">Model card</h2>
        <p className="text-xs text-scout-chalk/50">Artifacts: {card.data?.artifact_dir ?? "—"}</p>
        {card.isLoading && <p className="text-sm text-scout-chalk/60">Loading…</p>}
        {card.error && <p className="text-sm text-red-300">{(card.error as Error).message}</p>}
        {card.data && (
          <pre className="text-xs text-emerald-200/90 overflow-x-auto whitespace-pre-wrap">
            {JSON.stringify({ manifest: card.data.manifest, registry: card.data.registry }, null, 2)}
          </pre>
        )}
      </section>

      <section className="rounded-xl border border-white/10 bg-white/5 p-6 space-y-4">
        <h2 className="font-display text-xl">Feature glossary</h2>
        {glossary.isLoading && <p className="text-sm text-scout-chalk/60">Loading…</p>}
        {glossary.data && (
          <dl className="space-y-3 text-sm">
            {Object.entries(glossary.data).map(([k, v]) => (
              <div key={k} className="border-b border-white/5 pb-2">
                <dt className="font-mono text-scout-clay">{k}</dt>
                <dd className="text-scout-chalk/80 mt-1">{v}</dd>
              </div>
            ))}
          </dl>
        )}
      </section>
    </div>
  );
}

function DataModelsClerk() {
  const { getToken, isLoaded } = useAuth();
  if (!isLoaded) {
    return <p className="p-6 text-scout-chalk/60">Loading…</p>;
  }
  return <DataModelsInner getToken={() => getToken()} />;
}

export default function DataModelsPage() {
  if (clerkEnabled()) {
    return <DataModelsClerk />;
  }
  return <DataModelsInner />;
}
