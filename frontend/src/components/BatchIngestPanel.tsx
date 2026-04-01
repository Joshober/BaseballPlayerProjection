import { useCallback, useEffect, useState } from "react";
import { apiFetch } from "../lib/api";

type BatchStatus = {
  running: boolean;
  log: string[];
  stats: Record<string, unknown> | null;
  error: string | null;
};

export default function BatchIngestPanel() {
  const [targetIngests, setTargetIngests] = useState(50);
  const [targetMilbRows, setTargetMilbRows] = useState(0);
  const [delaySeconds, setDelaySeconds] = useState(3);
  const [extraQueries, setExtraQueries] = useState("");
  const [status, setStatus] = useState<BatchStatus | null>(null);
  const [startError, setStartError] = useState<string | null>(null);

  const poll = useCallback(async () => {
    try {
      const data = (await apiFetch("/api/scrape/batch-status")) as BatchStatus;
      setStatus(data);
    } catch {
      /* ignore transient errors while API restarts */
    }
  }, []);

  useEffect(() => {
    void poll();
  }, [poll]);

  useEffect(() => {
    if (!status?.running) return;
    const id = window.setInterval(() => void poll(), 2000);
    return () => window.clearInterval(id);
  }, [status?.running, poll]);

  async function startBatch() {
    setStartError(null);
    try {
      const extra = extraQueries
        .split("\n")
        .map((s) => s.trim())
        .filter((s) => s.length >= 2);
      await apiFetch("/api/scrape/batch-start", {
        method: "POST",
        body: JSON.stringify({
          target_new_ingests: targetIngests,
          target_milb_rows: targetMilbRows,
          delay_seconds: delaySeconds,
          extra_queries: extra,
        }),
      });
      await poll();
    } catch (e) {
      setStartError((e as Error).message);
    }
  }

  const running = status?.running ?? false;
  const logLines = status?.log ?? [];

  return (
    <section className="rounded-xl border border-amber-500/25 bg-amber-950/15 p-5 space-y-4">
      <div>
        <h2 className="text-base font-medium text-scout-chalk mb-1">Automated dataset build</h2>
        <p className="text-sm text-scout-chalk/60">
          Runs the same steps as manual Tools flow: MLB search → BBRef register URL → ingest, using search terms from{" "}
          <code className="text-scout-chalk/80">data/batch_search_queries.txt</code> plus optional lines below. Skips
          players who already have MiLB rows. Keep this tab open; progress polls every 2s. For long runs you can also use:{" "}
          <code className="text-scout-chalk/80">python -m ml.batch_ingest_discovery --target-ingests 100</code>
        </p>
      </div>

      <div className="grid sm:grid-cols-3 gap-3">
        <label className="block text-xs text-scout-chalk/60">
          Target new ingests
          <input
            type="number"
            min={1}
            max={2000}
            className="mt-1 w-full rounded-lg border border-white/15 bg-scout-ink/80 px-3 py-2 text-sm text-scout-chalk"
            value={targetIngests}
            onChange={(e) => setTargetIngests(Number(e.target.value))}
          />
        </label>
        <label className="block text-xs text-scout-chalk/60">
          Stop at MiLB rows (0 = off)
          <input
            type="number"
            min={0}
            className="mt-1 w-full rounded-lg border border-white/15 bg-scout-ink/80 px-3 py-2 text-sm text-scout-chalk"
            value={targetMilbRows}
            onChange={(e) => setTargetMilbRows(Number(e.target.value))}
          />
        </label>
        <label className="block text-xs text-scout-chalk/60">
          Delay between ingests (s)
          <input
            type="number"
            min={1}
            max={120}
            className="mt-1 w-full rounded-lg border border-white/15 bg-scout-ink/80 px-3 py-2 text-sm text-scout-chalk"
            value={delaySeconds}
            onChange={(e) => setDelaySeconds(Number(e.target.value))}
          />
        </label>
      </div>

      <label className="block text-xs text-scout-chalk/60">
        Extra search terms (one per line, optional)
        <textarea
          className="mt-1 w-full min-h-[72px] rounded-lg border border-white/15 bg-scout-ink/80 px-3 py-2 text-sm text-scout-chalk placeholder:text-scout-chalk/30"
          value={extraQueries}
          onChange={(e) => setExtraQueries(e.target.value)}
          placeholder={"One search term per line"}
        />
      </label>

      <div className="flex flex-wrap gap-2 items-center">
        <button
          type="button"
          disabled={running}
          onClick={() => void startBatch()}
          className="rounded-lg bg-amber-600 hover:bg-amber-500 px-4 py-2 text-sm font-medium text-scout-ink disabled:opacity-50"
        >
          {running ? "Batch running…" : "Start batch ingest"}
        </button>
        <button
          type="button"
          onClick={() => void poll()}
          className="rounded-lg border border-white/15 px-3 py-2 text-sm text-scout-chalk hover:bg-white/10"
        >
          Refresh status
        </button>
      </div>

      {startError && <p className="text-sm text-red-300">{startError}</p>}
      {status?.error && <p className="text-sm text-red-300">Job error: {status.error}</p>}
      {status?.stats && !running && (
        <pre className="text-xs text-emerald-200/90 overflow-x-auto bg-black/25 p-2 rounded max-h-32">
          {JSON.stringify(status.stats, null, 2)}
        </pre>
      )}

      <div>
        <h3 className="text-xs font-medium text-scout-chalk/60 mb-1">Log</h3>
        <pre className="text-xs text-scout-chalk/80 font-mono overflow-auto max-h-56 bg-black/30 p-2 rounded border border-white/10">
          {logLines.length === 0 ? "(no messages yet)" : logLines.join("\n")}
        </pre>
      </div>
    </section>
  );
}
