import { Link } from "react-router-dom";
import { SignedIn, SignedOut, SignInButton, UserButton } from "@clerk/clerk-react";
import { clerkEnabled } from "../lib/clerk";

type Props = {
  children: React.ReactNode;
};

export default function AppShell({ children }: Props) {
  const clerkOn = clerkEnabled();

  return (
    <div className="min-h-screen flex flex-col bg-scout-ink">
      <header className="sticky top-0 z-20 border-b border-white/10 bg-scout-ink/95 backdrop-blur-sm">
        <div className="max-w-6xl mx-auto px-4 md:px-6 py-3 flex flex-wrap items-center justify-between gap-3">
          <Link to="/" className="font-display text-xl text-scout-chalk tracking-tight hover:text-scout-clay transition-colors">
            ScoutPro
          </Link>
          <nav className="flex flex-wrap items-center gap-x-5 gap-y-2 text-sm">
            <Link className="text-scout-chalk/80 hover:text-scout-chalk" to="/search">
              Search
            </Link>
            <Link className="text-scout-chalk/80 hover:text-scout-chalk" to="/dashboard">
              Dashboard
            </Link>
            <Link className="text-scout-chalk/80 hover:text-scout-chalk" to="/pricing">
              Pricing
            </Link>
            <Link className="text-scout-chalk/60 hover:text-scout-chalk" to="/tools">
              Data tools
            </Link>
            <Link className="text-scout-chalk/60 hover:text-scout-chalk" to="/data-models">
              Data &amp; models
            </Link>
            <Link className="text-scout-chalk/60 hover:text-scout-chalk" to="/compare">
              Compare
            </Link>
            {clerkOn ? (
              <>
                <SignedOut>
                  <SignInButton mode="modal">
                    <button
                      type="button"
                      className="rounded-md border border-white/20 px-3 py-1.5 text-scout-chalk hover:bg-white/5 text-sm"
                    >
                      Sign in
                    </button>
                  </SignInButton>
                </SignedOut>
                <SignedIn>
                  <UserButton afterSignOutUrl="/" />
                </SignedIn>
              </>
            ) : (
              <Link to="/dashboard" className="text-sm text-scout-clay hover:underline">
                Dev dashboard
              </Link>
            )}
          </nav>
        </div>
      </header>
      <main className="flex-1">{children}</main>
    </div>
  );
}
