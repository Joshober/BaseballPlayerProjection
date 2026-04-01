import { Link } from "react-router-dom";
import { useWatchlist } from "../context/WatchlistContext";

type Props = {
  /** Resolved display names from parent queries (optional). */
  nameById?: Record<number, string>;
};

export default function WatchlistSidebar({ nameById }: Props) {
  const { ids, remove } = useWatchlist();

  return (
    <aside className="w-full md:w-56 shrink-0 border-b md:border-b-0 md:border-r border-white/10 p-4 bg-black/20">
      <h2 className="text-xs font-semibold uppercase tracking-wide text-scout-chalk/60 mb-3">Watchlist</h2>
      {ids.length === 0 && <p className="text-sm text-scout-chalk/50">No players saved yet.</p>}
      <ul className="space-y-2">
        {ids.map((id) => {
          const label = nameById?.[id] ?? `MLB ${id}`;
          return (
            <li key={id} className="flex items-center justify-between gap-2 text-sm">
              <Link to={`/player/${id}`} className="text-scout-clay hover:underline truncate" title={String(id)}>
                {label}
              </Link>
              <button
                type="button"
                className="text-scout-chalk/40 hover:text-red-300 shrink-0"
                onClick={() => void remove(id)}
                aria-label={`Remove ${label}`}
              >
                ×
              </button>
            </li>
          );
        })}
      </ul>
    </aside>
  );
}
