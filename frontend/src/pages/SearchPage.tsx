import { useState } from "react";
import { Link } from "react-router-dom";
import { apiFetch } from "../lib/api";

type MlbSearchRow = {
  id?: number;
  full_name?: string;
  primary_position?: string;
  mlb_debut_date?: string;
  active?: boolean;
};

export default function SearchPage() {
  const [searchName, setSearchName] = useState("");
  const [searchStatus, setSearchStatus] = useState("");
  const [searchResults, setSearchResults] = useState<MlbSearchRow[] | null>(null);

  async function runSearch(e: React.FormEvent) {
    e.preventDefault();
    setSearchStatus("Searching…");
    setSearchResults(null);
    try {
      const q = new URLSearchParams({ name: searchName.trim() });
      const data = (await apiFetch(`/mlb/search?${q}`)) as { count?: number; results?: MlbSearchRow[] };
      setSearchResults(data.results ?? []);
      setSearchStatus(`Found ${data.count ?? 0} result(s). Open a profile to see stats and projections.`);
    } catch (err) {
      setSearchStatus(`Error: ${(err as Error).message}`);
    }
  }

  return (
    <div className="max-w-4xl mx-auto px-4 md:px-6 py-8 space-y-6 pb-16">
      <div>
        <h1 className="font-display text-2xl text-scout-chalk">Search prospects</h1>
        <p className="text-sm text-scout-chalk/60 mt-1">
          Search the MLB Stats API by name, then open a player to view career numbers and ScoutPro projections.
        </p>
      </div>

      <section className="rounded-xl border border-white/10 bg-white/5 p-5 space-y-4">
        <form onSubmit={runSearch} className="flex flex-wrap gap-2 items-end">
          <input
            className="flex-1 min-w-[200px] rounded-lg border border-white/15 bg-scout-ink/80 px-3 py-2 text-sm text-scout-chalk"
            value={searchName}
            onChange={(e) => setSearchName(e.target.value)}
            placeholder="Player name (e.g. Jackson Holliday)"
            required
            minLength={2}
          />
          <button
            type="submit"
            className="rounded-lg bg-scout-clay px-4 py-2 text-sm font-medium text-scout-ink hover:opacity-90"
          >
            Search
          </button>
        </form>

        {searchStatus && <p className="text-sm text-scout-chalk/70">{searchStatus}</p>}

        {searchResults != null && searchResults.length > 0 && (
          <div className="overflow-auto border border-white/10 rounded-lg">
            <table className="w-full text-xs text-left border-collapse min-w-[480px]">
              <thead className="bg-scout-field/90">
                <tr>
                  <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">Name</th>
                  <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">Pos</th>
                  <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">MLB ID</th>
                  <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">Active</th>
                  <th className="border-b border-white/10 px-2 py-2 font-medium text-scout-chalk/80">Profile</th>
                </tr>
              </thead>
              <tbody>
                {searchResults.map((row, i) => {
                  const rid = row.id;
                  return (
                    <tr key={`${rid ?? i}-${i}`} className="border-b border-white/5 hover:bg-white/5">
                      <td className="px-2 py-2 text-scout-chalk/90">{row.full_name ?? ""}</td>
                      <td className="px-2 py-2 whitespace-nowrap text-scout-chalk/90">{row.primary_position ?? ""}</td>
                      <td className="px-2 py-2 whitespace-nowrap text-scout-chalk/90">{rid ?? ""}</td>
                      <td className="px-2 py-2 whitespace-nowrap text-scout-chalk/90">{String(row.active ?? "")}</td>
                      <td className="px-2 py-2">
                        {rid != null && Number.isFinite(Number(rid)) ? (
                          <Link
                            to={`/player/${rid}`}
                            className="rounded-md bg-white/10 px-2 py-1 text-scout-clay hover:bg-white/20"
                          >
                            Open
                          </Link>
                        ) : (
                          "—"
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {searchResults != null && searchResults.length === 0 && (
          <p className="text-sm text-scout-chalk/60">No players matched. Try a different spelling.</p>
        )}
      </section>
    </div>
  );
}
