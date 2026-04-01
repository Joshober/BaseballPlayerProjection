/** Generic table for array of row objects (legacy static/app.js parity). */
type Props = {
  rows: Record<string, unknown>[] | null | undefined;
  className?: string;
};

export default function DataTable({ rows, className = "" }: Props) {
  if (!rows || rows.length === 0) {
    return <div className="text-scout-chalk/50 text-sm py-3 px-2">No data</div>;
  }
  const cols = Object.keys(rows[0]);
  return (
    <div className={`overflow-auto max-h-[300px] border border-white/10 rounded-lg ${className}`}>
      <table className="w-full text-xs text-left border-collapse">
        <thead className="sticky top-0 bg-scout-field/90">
          <tr>
            {cols.map((c) => (
              <th key={c} className="border-b border-white/10 px-2 py-1.5 font-medium text-scout-chalk/80 whitespace-nowrap">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-b border-white/5 hover:bg-white/5">
              {cols.map((c) => (
                <td key={c} className="px-2 py-1.5 whitespace-nowrap text-scout-chalk/90">
                  {formatCell(r[c])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(v: unknown): string {
  if (v === null || v === undefined) {
    return "";
  }
  if (typeof v === "object") {
    return JSON.stringify(v);
  }
  return String(v);
}
