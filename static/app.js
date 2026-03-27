function setStatus(id, message, kind = "") {
  const el = document.getElementById(id);
  el.className = `status ${kind}`.trim();
  el.textContent = message;
}

async function parseApiResponse(res) {
  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return await res.json();
  }
  const text = await res.text();
  return { detail: text || `HTTP ${res.status}` };
}

function toTable(rows) {
  if (!rows || rows.length === 0) {
    return "<div style='padding:10px;color:#8fa3c0;'>No data</div>";
  }
  const cols = Object.keys(rows[0]);
  const thead = `<thead><tr>${cols.map((c) => `<th>${c}</th>`).join("")}</tr></thead>`;
  const bodyRows = rows
    .map((r) => `<tr>${cols.map((c) => `<td>${r[c] ?? ""}</td>`).join("")}</tr>`)
    .join("");
  return `<table>${thead}<tbody>${bodyRows}</tbody></table>`;
}

function toKeyValueGrid(obj) {
  const keys = Object.keys(obj || {});
  if (keys.length === 0) return "<div class='kv'><div class='k'>Info</div><div class='v'>No data</div></div>";
  return keys
    .map((k) => `<div class="kv"><div class="k">${k}</div><div class="v">${obj[k] ?? ""}</div></div>`)
    .join("");
}

async function runScrapeFromForm() {
  const url = document.getElementById("scrape-url").value.trim();
  const delay = Number(document.getElementById("scrape-delay").value || 2.0);
  const includeTables = document.getElementById("scrape-tables").checked;
  if (!url) return;
  setStatus("scrape-status", "Scraping player page (may take several seconds)...", "");

  const params = new URLSearchParams({
    url,
    delay: String(delay),
    include_tables: includeTables ? "true" : "false",
    table_limit: "500",
  });

  try {
    const res = await fetch(`/scrape?${params.toString()}`);
    const data = await parseApiResponse(res);
    if (!res.ok) throw new Error(data.detail || "Request failed");
    document.getElementById("scrape-summary").innerHTML = toKeyValueGrid({
      ...data.metadata,
      batting_rows: data.batting_rows,
      pitching_rows: data.pitching_rows,
    });
    document.getElementById("scrape-batting").innerHTML = toTable(data.batting || []);
    document.getElementById("scrape-pitching").innerHTML = toTable(data.pitching || []);
    setStatus("scrape-status", "Scrape completed.", "ok");
  } catch (err) {
    setStatus("scrape-status", `Error: ${err.message}`, "error");
  }
}

document.getElementById("search-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const name = document.getElementById("search-name").value.trim();
  if (!name) return;
  setStatus("search-status", "Searching...", "");
  try {
    const res = await fetch(`/mlb/search?name=${encodeURIComponent(name)}`);
    const data = await parseApiResponse(res);
    if (!res.ok) throw new Error(data.detail || "Request failed");
    document.getElementById("search-results").innerHTML = toTable(data.results || []);
    setStatus("search-status", `Found ${data.count} result(s).`, "ok");
  } catch (err) {
    setStatus("search-status", `Error: ${err.message}`, "error");
  }
});

document.getElementById("player-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const playerId = document.getElementById("player-id").value.trim();
  if (!playerId) return;
  setStatus("player-status", "Loading player profile...", "");
  try {
    const res = await fetch(`/mlb/player/${encodeURIComponent(playerId)}`);
    const data = await parseApiResponse(res);
    if (!res.ok) throw new Error(data.detail || "Request failed");
    document.getElementById("player-profile").innerHTML = toKeyValueGrid(data.profile || {});
    document.getElementById("player-hitting").innerHTML = toTable(data.career_hitting || []);
    document.getElementById("player-pitching").innerHTML = toTable(data.career_pitching || []);
    setStatus("player-status", "Player loaded successfully.", "ok");
  } catch (err) {
    setStatus("player-status", `Error: ${err.message}`, "error");
  }
});

document.getElementById("scrape-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  await runScrapeFromForm();
});
