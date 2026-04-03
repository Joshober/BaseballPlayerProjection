import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { useAuth } from "@clerk/clerk-react";
import { apiFetch } from "../lib/api";
import { clerkEnabled } from "../lib/clerk";

function CompareInner({ getToken }: { getToken?: () => Promise<string | null> }) {
  const [a, setA] = useState("660670");
  const [b, setB] = useState("592450");

  const cmp = useMutation({
    mutationFn: async () => {
      const t = getToken ? await getToken() : undefined;
      const body = JSON.stringify({
        mlbam_id_a: parseInt(a, 10),
        mlbam_id_b: parseInt(b, 10),
        feature_version: "v3",
      });
      return apiFetch("/api/compare/players", { method: "POST", body }, t ?? undefined) as Promise<Record<string, unknown>>;
    },
  });

  return (
    <div className="max-w-4xl mx-auto px-4 py-8 space-y-8 text-scout-chalk">
      <div>
        <h1 className="font-display text-3xl mb-2">Compare players</h1>
        <p className="text-scout-chalk/70 text-sm">
          Side-by-side engineered features (v3) and short explanations. Requires both players ingested and{" "}
          <code className="text-scout-clay">build_features</code> for v3.
        </p>
      </div>

      <form
        className="flex flex-wrap gap-4 items-end"
        onSubmit={(e) => {
          e.preventDefault();
          cmp.mutate();
        }}
      >
        <label className="flex flex-col gap-1 text-sm">
          MLBAM id A
          <input
            className="rounded-md bg-white/10 border border-white/20 px-3 py-2 text-scout-chalk"
            value={a}
            onChange={(e) => setA(e.target.value)}
          />
        </label>
        <label className="flex flex-col gap-1 text-sm">
          MLBAM id B
          <input
            className="rounded-md bg-white/10 border border-white/20 px-3 py-2 text-scout-chalk"
            value={b}
            onChange={(e) => setB(e.target.value)}
          />
        </label>
        <button
          type="submit"
          className="rounded-md bg-scout-clay/90 px-4 py-2 text-scout-ink font-medium hover:opacity-90 disabled:opacity-50"
          disabled={cmp.isPending}
        >
          Compare
        </button>
      </form>

      {cmp.error && <p className="text-sm text-red-300">{(cmp.error as Error).message}</p>}

      {cmp.data && (
        <div className="rounded-xl border border-white/10 bg-white/5 p-6 space-y-4">
          <h2 className="font-display text-lg">Results</h2>
          {Array.isArray(cmp.data.why_a_vs_b) && (cmp.data.why_a_vs_b as string[]).length > 0 && (
            <ul className="list-disc pl-5 text-sm text-scout-chalk/90">
              {(cmp.data.why_a_vs_b as string[]).map((line) => (
                <li key={line}>{line}</li>
              ))}
            </ul>
          )}
          <pre className="text-xs text-emerald-200/90 overflow-x-auto whitespace-pre-wrap">{JSON.stringify(cmp.data, null, 2)}</pre>
        </div>
      )}
    </div>
  );
}

function CompareClerk() {
  const { getToken, isLoaded } = useAuth();
  if (!isLoaded) {
    return <p className="p-6 text-scout-chalk/60">Loading…</p>;
  }
  return <CompareInner getToken={() => getToken()} />;
}

export default function ComparePage() {
  if (clerkEnabled()) {
    return <CompareClerk />;
  }
  return <CompareInner />;
}
