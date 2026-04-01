import { useState } from "react";

import { apiFetch } from "../lib/api";



const EXAMPLE_BBREF =

  "https://www.baseball-reference.com/register/player.fcgi?id=cabrer003jos";



export type BbrefCandidate = { url: string; bbref_id: string; label: string };



type Props = {

  getToken?: () => Promise<string | null>;

  /** When set, sent as ``mlb_id`` on ingest (links Stats API player to DB row). */

  mlbId?: number | null;

  /** Controlled register URL from parent pipeline; omit for standalone default. */

  registerUrl?: string;

  onRegisterUrlChange?: (url: string) => void;

  /** BBRef matches from ``/api/scrape/register-search`` — user can pick if several. */

  bbrefCandidates?: BbrefCandidate[];

};



export default function ScrapeIngestPanel({

  getToken,

  mlbId = null,

  registerUrl: controlledUrl,

  onRegisterUrlChange,

  bbrefCandidates,

}: Props) {

  const [internalUrl, setInternalUrl] = useState(EXAMPLE_BBREF);

  const isControlled = controlledUrl !== undefined;

  const url = isControlled ? controlledUrl! : internalUrl;

  const setUrl = onRegisterUrlChange ?? setInternalUrl;



  const [delay] = useState(2);

  const [buildFeatures, setBuildFeatures] = useState(false);

  const [loading, setLoading] = useState<"preview" | "ingest" | null>(null);

  const [previewJson, setPreviewJson] = useState<unknown>(null);

  const [ingestJson, setIngestJson] = useState<unknown>(null);

  const [error, setError] = useState<string | null>(null);



  async function tokenOrUndefined() {

    if (!getToken) {

      return undefined;

    }

    try {

      return await getToken();

    } catch {

      return undefined;

    }

  }



  async function runPreview() {

    setError(null);

    setIngestJson(null);

    setLoading("preview");

    try {

      const t = await tokenOrUndefined();

      const q = new URLSearchParams({

        url: url.trim(),

        delay: String(delay),

        include_tables: "true",

        table_limit: "800",

      });

      const data = await apiFetch(`/api/scrape/preview?${q.toString()}`, {}, t);

      setPreviewJson(data);

    } catch (e) {

      setPreviewJson(null);

      setError((e as Error).message);

    } finally {

      setLoading(null);

    }

  }



  async function runIngest() {

    setError(null);

    setLoading("ingest");

    try {

      const t = await tokenOrUndefined();

      const q = new URLSearchParams({

        url: url.trim(),

        delay: String(delay),

        build_features: buildFeatures ? "true" : "false",

        feature_version: "v1",

      });

      if (mlbId != null && Number.isFinite(mlbId)) {

        q.set("mlb_id", String(mlbId));

      }

      const data = await apiFetch(`/api/scrape/ingest?${q.toString()}`, { method: "POST" }, t);

      setIngestJson(data);

    } catch (e) {

      setIngestJson(null);

      setError((e as Error).message);

    } finally {

      setLoading(null);

    }

  }



  return (

    <section className="rounded-lg border border-white/10 p-4 bg-white/5 space-y-4">

      <div>

        <h2 className="text-sm font-medium text-scout-chalk/80 mb-1">Baseball-Reference ingest</h2>

        <p className="text-xs text-scout-chalk/50 max-w-xl">

          After you pick a player from MLB search above, we search BBRef for the MiLB register URL and pass your{" "}

          <code className="text-scout-chalk/70">mlb_id</code> on ingest. Preview JSON, then write to Postgres (requires{" "}

          <code className="text-scout-chalk/70">DATABASE_URL</code>). Optional feature rebuild runs for all players.

        </p>

      </div>



      {bbrefCandidates != null && bbrefCandidates.length > 1 && (

        <label className="block">

          <span className="block text-xs text-scout-chalk/60 mb-1">Register page (choose match)</span>

          <select

            className="w-full rounded border border-white/15 bg-scout-ink/80 px-3 py-2 text-sm text-scout-chalk"

            value={url}

            onChange={(e) => setUrl(e.target.value)}

          >

            {bbrefCandidates.map((c) => (

              <option key={c.bbref_id} value={c.url}>

                {c.label} ({c.bbref_id})

              </option>

            ))}

          </select>

        </label>

      )}



      <label className="block text-xs text-scout-chalk/60 mb-1">Register player URL</label>

      <input

        type="url"

        className="w-full rounded border border-white/15 bg-scout-ink/80 px-3 py-2 text-sm text-scout-chalk placeholder:text-scout-chalk/30"

        value={url}

        onChange={(e) => setUrl(e.target.value)}

        placeholder="https://www.baseball-reference.com/register/player.fcgi?id=..."

      />

      {mlbId != null && (

        <p className="text-xs text-emerald-200/70">

          Ingest will include MLB Stats API id <code className="text-scout-chalk/80">{mlbId}</code>.

        </p>

      )}

      <label className="flex items-center gap-2 text-sm text-scout-chalk/80 cursor-pointer">

        <input

          type="checkbox"

          checked={buildFeatures}

          onChange={(e) => setBuildFeatures(e.target.checked)}

          className="rounded border-white/20"

        />

        Rebuild engineered features after ingest (full table)

      </label>

      <div className="flex flex-wrap gap-2">

        <button

          type="button"

          disabled={loading !== null}

          onClick={() => void runPreview()}

          className="rounded-md bg-white/10 px-4 py-2 text-sm text-scout-chalk hover:bg-white/15 disabled:opacity-50"

        >

          {loading === "preview" ? "Preview…" : "Preview scrape"}

        </button>

        <button

          type="button"

          disabled={loading !== null}

          onClick={() => void runIngest()}

          className="rounded-md bg-scout-clay px-4 py-2 text-sm font-medium text-scout-ink hover:opacity-90 disabled:opacity-50"

        >

          {loading === "ingest" ? "Ingesting…" : "Ingest to database"}

        </button>

      </div>

      {error && <p className="text-sm text-red-300 whitespace-pre-wrap">{error}</p>}

      {previewJson != null && (

        <div>

          <h3 className="text-xs font-medium text-scout-chalk/60 mb-1">Preview (sample)</h3>

          <pre className="text-xs text-emerald-200/80 overflow-x-auto max-h-48 overflow-y-auto bg-black/20 p-2 rounded">

            {JSON.stringify(previewJson, null, 2).slice(0, 12000)}

            {(JSON.stringify(previewJson).length > 12000 ? "\n…" : "")}

          </pre>

        </div>

      )}

      {ingestJson != null && (

        <div>

          <h3 className="text-xs font-medium text-scout-chalk/60 mb-1">Ingest result</h3>

          <pre className="text-xs text-amber-100/90 overflow-x-auto max-h-40 overflow-y-auto bg-black/20 p-2 rounded">

            {JSON.stringify(ingestJson, null, 2)}

          </pre>

        </div>

      )}

    </section>

  );

}

