const BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export async function apiFetch(path: string, init: RequestInit = {}, token?: string | null) {
  const headers = new Headers(init.headers);
  const method = (init.method || "GET").toUpperCase();
  if (method !== "GET" && method !== "HEAD") {
    headers.set("Content-Type", "application/json");
  }
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  const res = await fetch(`${BASE.replace(/\/$/, "")}${path}`, { ...init, headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status}: ${text}`);
  }
  const ct = res.headers.get("content-type");
  if (ct?.includes("application/json")) {
    return res.json();
  }
  return res.text();
}

export function healthUrl() {
  return `${BASE}/health`;
}
