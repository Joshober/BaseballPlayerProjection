export function clerkEnabled(): boolean {
  const k = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY || "";
  return k.startsWith("pk_");
}
