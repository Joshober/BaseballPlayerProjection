import { useState } from "react";
import { apiFetch } from "../lib/api";

const tiers = [
  { id: "starter", name: "Starter", price: "$49/mo", blurb: "10 reports / month" },
  { id: "pro", name: "Pro", price: "$149/mo", blurb: "Full reports + exports" },
  { id: "agency", name: "Agency", price: "$499/mo", blurb: "Multi-seat, white-label" },
];

export default function PricingPage() {
  const [msg, setMsg] = useState<string | null>(null);

  async function checkout(plan: string) {
    setMsg(null);
    try {
      const data = await apiFetch(`/api/subscriptions/create-checkout?plan=${encodeURIComponent(plan)}`, {
        method: "POST",
      });
      if (data && typeof data === "object" && "url" in data && (data as { url?: string }).url) {
        window.location.href = (data as { url: string }).url;
      } else {
        setMsg(JSON.stringify(data));
      }
    } catch (e) {
      setMsg((e as Error).message);
    }
  }

  return (
    <div className="min-h-screen bg-scout-ink px-6 py-12 max-w-4xl mx-auto">
      <h1 className="font-display text-3xl text-scout-chalk mb-8">Pricing</h1>
      <div className="grid gap-6 md:grid-cols-3">
        {tiers.map((t) => (
          <div key={t.id} className="rounded-xl border border-white/10 bg-white/5 p-6 flex flex-col gap-3">
            <h2 className="text-xl text-scout-chalk">{t.name}</h2>
            <p className="text-scout-clay font-semibold">{t.price}</p>
            <p className="text-scout-chalk/70 text-sm flex-1">{t.blurb}</p>
            <button
              type="button"
              onClick={() => checkout(t.id)}
              className="rounded-md bg-scout-field px-4 py-2 text-scout-chalk text-sm hover:opacity-90"
            >
              Choose {t.name}
            </button>
          </div>
        ))}
      </div>
      {msg && <p className="mt-6 text-sm text-amber-200/90">{msg}</p>}
    </div>
  );
}
