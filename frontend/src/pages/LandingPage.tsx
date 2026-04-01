import { Link } from "react-router-dom";

export default function LandingPage() {
  return (
    <div className="min-h-[calc(100vh-4rem)] bg-gradient-to-b from-scout-ink to-scout-field/40">
      <main className="px-6 py-12 max-w-3xl mx-auto space-y-8">
        <div>
          <h1 className="font-display text-3xl md:text-4xl text-scout-chalk tracking-tight">Recruiting intelligence</h1>
          <p className="text-scout-chalk/70 mt-2 text-lg">
            Search prospects, track your board, and review MLB and MiLB stats with ScoutPro projections — built for
            recruiters and scouting staffs.
          </p>
        </div>
        <p className="text-scout-chalk/90 leading-relaxed">
          Sign in to save players to your dashboard, see arrival probability and reports, and keep tabs on how
          prospects are performing over time.
        </p>
        <div className="flex flex-wrap gap-3">
          <Link
            to="/search"
            className="inline-block rounded-md bg-scout-clay px-5 py-2.5 text-scout-ink font-medium hover:opacity-90"
          >
            Search prospects
          </Link>
          <Link
            to="/dashboard"
            className="inline-block rounded-md border border-white/20 px-5 py-2.5 text-scout-chalk hover:bg-white/5"
          >
            Your dashboard
          </Link>
          <Link
            to="/tools"
            className="inline-block rounded-md border border-white/10 px-5 py-2.5 text-scout-chalk/80 hover:bg-white/5 text-sm"
          >
            Data pipeline tools
          </Link>
        </div>
      </main>
    </div>
  );
}
