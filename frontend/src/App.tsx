import { Routes, Route, Navigate, Link, Outlet } from "react-router-dom";
import { SignedIn, SignedOut } from "@clerk/clerk-react";
import AppShell from "./components/AppShell";
import LandingPage from "./pages/LandingPage";
import SearchPage from "./pages/SearchPage";
import ToolsPage from "./pages/ToolsPage";
import DashboardPage from "./pages/DashboardPage";
import PlayerPage from "./pages/PlayerPage";
import PricingPage from "./pages/PricingPage";
import DataModelsPage from "./pages/DataModelsPage";
import ComparePage from "./pages/ComparePage";
import { ClerkSignIn, ClerkSignUp } from "./pages/ClerkAuthPages";
import { clerkEnabled } from "./lib/clerk";

function Protected({ children }: { children: React.ReactNode }) {
  if (!clerkEnabled()) {
    return <>{children}</>;
  }
  return (
    <>
      <SignedIn>{children}</SignedIn>
      <SignedOut>
        <div className="min-h-screen flex flex-col items-center justify-center gap-4 bg-scout-ink p-6 text-center">
          <p className="text-scout-chalk/80">Sign in to continue.</p>
          <Link className="text-scout-clay underline" to="/sign-in">
            Sign in
          </Link>
        </div>
      </SignedOut>
    </>
  );
}

function AppShellLayout() {
  return (
    <>
      {!clerkEnabled() && (
        <div className="bg-amber-900/40 text-amber-100 text-center text-sm py-2 px-4">
          Dev mode: Clerk not configured. API calls use SCOUTPRO_DEV_AUTH on the backend.
        </div>
      )}
      <AppShell>
        <Outlet />
      </AppShell>
    </>
  );
}

export default function App() {
  return (
    <Routes>
      <Route path="/sign-in/*" element={<ClerkSignIn />} />
      <Route path="/sign-up/*" element={<ClerkSignUp />} />
      <Route element={<AppShellLayout />}>
        <Route path="/" element={<LandingPage />} />
        <Route path="/search" element={<SearchPage />} />
        <Route path="/tools" element={<ToolsPage />} />
        <Route path="/data-models" element={<DataModelsPage />} />
        <Route path="/compare" element={<ComparePage />} />
        <Route path="/pricing" element={<PricingPage />} />
        <Route
          path="/dashboard"
          element={
            <Protected>
              <DashboardPage />
            </Protected>
          }
        />
        <Route
          path="/player/:mlbamId"
          element={
            <Protected>
              <PlayerPage />
            </Protected>
          }
        />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
