type Props = {
  data: Record<string, unknown> | null | undefined;
};

export default function KeyValueGrid({ data }: Props) {
  const keys = data ? Object.keys(data) : [];
  if (keys.length === 0) {
    return <p className="text-scout-chalk/50 text-sm">No data</p>;
  }
  return (
    <div className="grid grid-cols-[repeat(auto-fit,minmax(160px,1fr))] gap-2">
      {keys.map((k) => (
        <div key={k} className="rounded-lg border border-white/10 bg-black/20 px-2 py-1.5">
          <div className="text-[0.65rem] uppercase tracking-wide text-scout-chalk/50">{k}</div>
          <div className="text-sm text-scout-chalk mt-0.5 break-words">{formatVal(data?.[k])}</div>
        </div>
      ))}
    </div>
  );
}

function formatVal(v: unknown): string {
  if (v === null || v === undefined) {
    return "";
  }
  if (typeof v === "object") {
    return JSON.stringify(v);
  }
  return String(v);
}
