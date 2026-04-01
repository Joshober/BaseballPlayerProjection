/**

 * MLB search → pick player → profile + BBRef register URL + MiLB scrape ingest (one pipeline).

 */

import { useState } from "react";

import { Link } from "react-router-dom";

import DataTable from "../components/DataTable";

import KeyValueGrid from "../components/KeyValueGrid";

import BatchIngestPanel from "../components/BatchIngestPanel";

import ScrapeIngestPanel, { type BbrefCandidate } from "../components/ScrapeIngestPanel";

import { apiFetch } from "../lib/api";



const API_ROOT = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";



type MlbSearchRow = {

  id?: number;

  full_name?: string;

  primary_position?: string;

  mlb_debut_date?: string;

  active?: boolean;

};



export default function ToolsPage() {

  const [searchName, setSearchName] = useState("Mike Trout");

  const [searchStatus, setSearchStatus] = useState("");

  const [searchResults, setSearchResults] = useState<MlbSearchRow[] | null>(null);



  const [selectedMlbId, setSelectedMlbId] = useState<number | null>(null);

  const [selectedLabel, setSelectedLabel] = useState("");

  const [playerStatus, setPlayerStatus] = useState("");

  const [profile, setProfile] = useState<Record<string, unknown> | null>(null);

  const [hitting, setHitting] = useState<Record<string, unknown>[] | null>(null);

  const [pitching, setPitching] = useState<Record<string, unknown>[] | null>(null);



  const [bbrefStatus, setBbrefStatus] = useState("");

  const [registerUrl, setRegisterUrl] = useState("");

  const [bbrefCandidates, setBbrefCandidates] = useState<BbrefCandidate[] | undefined>(undefined);



  async function runSearch(e: React.FormEvent) {

    e.preventDefault();

    setSearchStatus("Searching…");

    setSearchResults(null);

    setSelectedMlbId(null);

    setSelectedLabel("");

    setProfile(null);

    setHitting(null);

    setPitching(null);

    setBbrefCandidates(undefined);

    setRegisterUrl("");

    setBbrefStatus("");

    try {

      const q = new URLSearchParams({ name: searchName.trim() });

      const data = (await apiFetch(`/mlb/search?${q}`)) as { count?: number; results?: MlbSearchRow[] };

      setSearchResults(data.results ?? []);

      setSearchStatus(`Found ${data.count ?? 0} result(s). Pick a row to load profile and BBRef register URL.`);

    } catch (err) {

      setSearchStatus(`Error: ${(err as Error).message}`);

    }

  }



  async function loadProfileForId(id: number) {

    const data = (await apiFetch(`/mlb/player/${encodeURIComponent(String(id))}`)) as {

      profile?: Record<string, unknown>;

      career_hitting?: Record<string, unknown>[];

      career_pitching?: Record<string, unknown>[];

    };

    setProfile(data.profile ?? null);

    setHitting(data.career_hitting ?? []);

    setPitching(data.career_pitching ?? []);

  }



  async function loadBbrefCandidates(name: string) {

    setBbrefStatus("Searching Baseball-Reference for MiLB register page…");

    setBbrefCandidates(undefined);

    setRegisterUrl("");

    try {

      const q = new URLSearchParams({ name: name.trim() });

      const data = (await apiFetch(`/api/scrape/register-search?${q}`)) as {

        count?: number;

        candidates?: BbrefCandidate[];

      };

      const c = data.candidates ?? [];

      setBbrefCandidates(c);

      if (c.length === 1) {

        setRegisterUrl(c[0].url);

        setBbrefStatus(`Matched 1 register page (${c[0].bbref_id}).`);

      } else if (c.length > 1) {

        setRegisterUrl(c[0].url);

        setBbrefStatus(`Found ${c.length} register links — choose the right one in the dropdown below.`);

      } else {

        setBbrefStatus("No MiLB register links from BBRef search — paste a register URL manually if needed.");

      }

    } catch (err) {

      setBbrefCandidates([]);

      setBbrefStatus(`BBRef search error: ${(err as Error).message}`);

    }

  }



  async function selectPlayer(row: MlbSearchRow) {

    const id = row.id;

    const name = row.full_name?.trim() ?? "";

    if (id == null || !Number.isFinite(Number(id))) {

      setPlayerStatus("Invalid row: missing MLB Stats API id.");

      return;

    }

    const nid = Number(id);

    setSelectedMlbId(nid);

    setSelectedLabel(name || `Player ${nid}`);

    setPlayerStatus("Loading profile…");

    setProfile(null);

    setHitting(null);

    setPitching(null);

    try {

      await loadProfileForId(nid);

      setPlayerStatus("Profile loaded.");

    } catch (err) {

      setPlayerStatus(`Error: ${(err as Error).message}`);

      return;

    }

    if (name) {

      await loadBbrefCandidates(name);

    } else {

      setBbrefStatus("No name on record — enter a BBRef register URL manually for ingest.");

      setBbrefCandidates([]);

    }

  }



  return (

    <div className="min-h-screen bg-gradient-to-b from-scout-ink to-scout-field/30">

      <main className="max-w-4xl mx-auto px-4 md:px-6 py-8 space-y-6 pb-16">

        <section className="rounded-xl border border-white/10 bg-white/5 p-5 shadow-lg space-y-5">

          <div>

            <h2 className="text-base font-medium text-scout-chalk mb-1">MLB player pipeline</h2>

            <p className="text-sm text-scout-chalk/60">

              For recruiting, use <Link to="/search" className="text-scout-clay underline">Search</Link>. This section

              loads the MLB profile, resolves the MiLB register URL on Baseball-Reference, and passes your MLB id into

              ingest.

            </p>

            <p className="text-xs text-scout-chalk/45 mt-2">

              <a href={`${API_ROOT}/docs`} target="_blank" rel="noreferrer" className="underline hover:text-scout-chalk/70">

                API docs

              </a>

            </p>

          </div>



          <form onSubmit={runSearch} className="flex flex-wrap gap-2 items-end">

            <input

              className="flex-1 min-w-[200px] rounded-lg border border-white/15 bg-scout-ink/80 px-3 py-2 text-sm text-scout-chalk"

              value={searchName}

              onChange={(e) => setSearchName(e.target.value)}

              placeholder="Player name (e.g. Mike Trout)"

              required

            />

            <button

              type="submit"

              className="rounded-lg bg-blue-600 hover:bg-blue-500 px-4 py-2 text-sm font-medium text-white"

            >

              Search

            </button>

          </form>

          {searchStatus && <p className="text-sm text-scout-chalk/70">{searchStatus}</p>}



          {searchResults != null && searchResults.length > 0 && (

            <div className="overflow-auto border border-white/10 rounded-lg">

              <table className="w-full text-xs text-left border-collapse min-w-[520px]">

                <thead className="bg-scout-field/90">

                  <tr>

                    <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">Select</th>

                    <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">id</th>

                    <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">full_name</th>

                    <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">Pos</th>

                    <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">active</th>

                  </tr>

                </thead>

                <tbody>

                  {searchResults.map((row, i) => {

                    const rid = row.id;

                    const active = row.active;

                    return (

                      <tr key={`${rid ?? i}-${i}`} className="border-b border-white/5 hover:bg-white/5">

                        <td className="px-2 py-2">

                          <button

                            type="button"

                            onClick={() => void selectPlayer(row)}

                            className="rounded-md bg-white/10 px-2 py-1 text-scout-chalk hover:bg-white/20"

                          >

                            Select

                          </button>

                        </td>

                        <td className="px-2 py-2 whitespace-nowrap text-scout-chalk/90">{rid ?? ""}</td>

                        <td className="px-2 py-2 text-scout-chalk/90">{row.full_name ?? ""}</td>

                        <td className="px-2 py-2 whitespace-nowrap text-scout-chalk/90">{row.primary_position ?? ""}</td>

                        <td className="px-2 py-2 whitespace-nowrap text-scout-chalk/90">{String(active ?? "")}</td>

                      </tr>

                    );

                  })}

                </tbody>

              </table>

            </div>

          )}



          {selectedMlbId != null && (

            <div className="flex flex-wrap items-center gap-3 text-sm text-scout-chalk/80">

              <span>

                Selected: <strong className="text-scout-chalk">{selectedLabel}</strong> (MLB id {selectedMlbId})

              </span>

              <Link

                to={`/player/${selectedMlbId}`}

                className="rounded-lg border border-white/15 px-3 py-1 text-scout-chalk hover:border-scout-clay/60"

              >

                Open profile

              </Link>

            </div>

          )}



          {playerStatus && <p className="text-sm text-scout-chalk/70">{playerStatus}</p>}

          {bbrefStatus && <p className="text-sm text-scout-chalk/70">{bbrefStatus}</p>}



          {profile && (

            <div>

              <h3 className="text-sm font-medium text-scout-chalk mb-2">MLB profile</h3>

              <KeyValueGrid data={profile} />

            </div>

          )}

          {(hitting != null && hitting.length > 0) || (pitching != null && pitching.length > 0) ? (

            <div className="grid md:grid-cols-2 gap-4">

              <div>

                <h3 className="text-sm text-scout-chalk/80 mb-2">Career hitting</h3>

                <DataTable rows={hitting ?? []} />

              </div>

              <div>

                <h3 className="text-sm text-scout-chalk/80 mb-2">Career pitching</h3>

                <DataTable rows={pitching ?? []} />

              </div>

            </div>

          ) : null}

        </section>

        <BatchIngestPanel />

        <ScrapeIngestPanel

          mlbId={selectedMlbId}

          registerUrl={registerUrl}

          onRegisterUrlChange={setRegisterUrl}

          bbrefCandidates={bbrefCandidates}

        />

      </main>

    </div>

  );

}

