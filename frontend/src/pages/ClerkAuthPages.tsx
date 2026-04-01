import { SignIn, SignUp } from "@clerk/clerk-react";
import { Link } from "react-router-dom";

export function ClerkSignIn() {
  const k = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY || "";
  if (!k.startsWith("pk_")) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center gap-4 p-8">
        <p className="text-scout-chalk/80">Set VITE_CLERK_PUBLISHABLE_KEY for hosted Clerk.</p>
        <Link to="/dashboard" className="text-scout-clay underline">
          Continue to dashboard (dev)
        </Link>
      </div>
    );
  }
  return (
    <div className="min-h-screen bg-scout-ink">
      <div className="p-4 max-w-md mx-auto">
        <Link to="/" className="text-sm text-scout-clay hover:underline">
          ← ScoutPro home
        </Link>
      </div>
      <SignIn routing="path" path="/sign-in" />
    </div>
  );
}

export function ClerkSignUp() {
  const k = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY || "";
  if (!k.startsWith("pk_")) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center gap-4 p-8">
        <p className="text-scout-chalk/80">Set VITE_CLERK_PUBLISHABLE_KEY for hosted Clerk.</p>
        <Link to="/dashboard" className="text-scout-clay underline">
          Continue to dashboard (dev)
        </Link>
      </div>
    );
  }
  return (
    <div className="min-h-screen bg-scout-ink">
      <div className="p-4 max-w-md mx-auto">
        <Link to="/" className="text-sm text-scout-clay hover:underline">
          ← ScoutPro home
        </Link>
      </div>
      <SignUp routing="path" path="/sign-up" />
    </div>
  );
}
