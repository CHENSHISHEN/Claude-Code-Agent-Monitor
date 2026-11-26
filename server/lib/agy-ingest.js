/**
 * @file agy-ingest.js
 * @description Best-effort, read-only ingestion of Google Antigravity CLI
 * (agy) sessions from `~/.gemini/antigravity-cli/conversation_summaries.db`.
 * Antigravity has no hook system, so periodic polling mirrors its summary
 * store into dashboard sessions tagged provider='agy'. Antigravity stays the
 * only writer: the store opens read-only with a short busy timeout, per-row
 * fingerprints make repeated passes no-ops, and any failure simply skips the
 * pass. `syncAgySessions` also accepts explicit paths plus a `source` tag so
 * remote mirrors (remote-sync.js) can reuse the same importer.
 * @author Son Nguyen <hoangson091104@gmail.com>
 */

const fs = require("fs");
const os = require("os");
const path = require("path");

// Antigravity is the only writer; never wait on its locks during a poll.
const BUSY_TIMEOUT_MS = 250;
const MAX_PROMPT_PREVIEW = 500;

const SUMMARY_QUERY = `
  SELECT conversation_id, title, preview, step_count, last_modified_time,
         workspace_uris, status, agent_name, parent_conversation_id,
         nesting_depth, killed, not_fully_idle, last_user_input_time
  FROM conversation_summaries
`;

/** Default Antigravity data home (override: DASHBOARD_AGY_HOME). */
function getAgyHome() {
  const override = process.env.DASHBOARD_AGY_HOME;
  const home = os.homedir();
  if (override) return path.resolve(override.replace(/^~(?=$|[\\/])/, home));
  return path.join(home, ".gemini", "antigravity-cli");
}

/** Summary store path (override: DASHBOARD_AGY_STATE_DB). */
function getAgySummaryDbPath() {
  const override = process.env.DASHBOARD_AGY_STATE_DB;
  const home = os.homedir();
  if (override) return path.resolve(override.replace(/^~(?=$|[\\/])/, home));
  return path.join(getAgyHome(), "conversation_summaries.db");
}

/** Shared user-input history (override: DASHBOARD_AGY_HISTORY). */
function getAgyHistoryPath() {
  const override = process.env.DASHBOARD_AGY_HISTORY;
  const home = os.homedir();
  if (override) return path.resolve(override.replace(/^~(?=$|[\\/])/, home));
  return path.join(getAgyHome(), "history.jsonl");
}

let openReadOnly;

/** Pick a SQLite driver once, preferring better-sqlite3 like server/db.js. */
function readOnlyOpener() {
  if (openReadOnly !== undefined) return openReadOnly;
  openReadOnly = null;
  try {
    const Database = require("better-sqlite3");
    // The native addon loads lazily; probe it so an ABI mismatch falls back.
    new Database(":memory:").close();
    openReadOnly = (filePath) =>
      new Database(filePath, { readonly: true, fileMustExist: true, timeout: BUSY_TIMEOUT_MS });
  } catch {
    try {
      const { DatabaseSync } = require("node:sqlite");
      openReadOnly = (filePath) => {
        if (!fs.statSync(filePath).isFile()) throw new Error("not a file");
        return new DatabaseSync(filePath, { readOnly: true, timeout: BUSY_TIMEOUT_MS });
      };
    } catch {
      // No SQLite driver: agy sessions stay unavailable.
    }
  }
  return openReadOnly;
}

/**
 * Antigravity stores SQLite datetimes like `2026-11-25 10:14:36.121718+00:00`
 * (space separator, up to six fractional digits). Normalize to ISO-8601
 * milliseconds so Date.parse stays deterministic across drivers.
 */
function parseAgyTime(value) {
  if (typeof value !== "string" || !value.trim()) return null;
  const match = value.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?\s*(.*)$/);
  if (!match) {
    const fallback = Date.parse(value);
    return Number.isFinite(fallback) ? new Date(fallback).toISOString() : null;
  }
  const [, date, time, fraction = "0", offset = "Z"] = match;
  // Antigravity stamps never-touched rows with the SQLite epoch placeholder
  // `0001-01-01 ...`; treat anything before 1970 as "no timestamp".
  if (Number(date.slice(0, 4)) < 1970) return null;
  const ms = fraction.slice(0, 3).padEnd(3, "0");
  const parsed = Date.parse(`${date}T${time}.${ms}${offset || "Z"}`);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

/**
 * Map Antigravity's cascade status onto the dashboard lifecycle. Only
 * CASCADE_RUN_STATUS_IDLE is expected on an idle machine; anything else
 * (RUNNING, future values) stays visible as active work, `killed` marks
 * error, and `not_fully_idle` keeps background/swarm work lit even when the
 * top-level status already reports idle.
 */
function mapAgyStatus(rawStatus, killed, notFullyIdle) {
  if (killed) return { session: "error", agent: "error" };
  const status = String(rawStatus || "").toUpperCase();
  if (!status || (status.includes("IDLE") && !notFullyIdle)) {
    return { session: "completed", agent: "completed" };
  }
  return { session: "active", agent: "working" };
}

/** First `file://` workspace URI → filesystem path for the sessions cwd. */
function agyWorkspaceToCwd(workspaceUris) {
  if (typeof workspaceUris !== "string" || !workspaceUris.trim()) return null;
  try {
    const uris = JSON.parse(workspaceUris);
    const first = Array.isArray(uris) ? uris.find((u) => typeof u === "string" && u) : null;
    if (!first) return null;
    let raw = first.startsWith("file://") ? first.slice("file://".length) : first;
    try {
      raw = decodeURIComponent(raw);
    } catch {
      // Keep the raw path when it is not valid percent-encoding.
    }
    return raw || null;
  } catch {
    return null;
  }
}

/**
 * Index `history.jsonl` (one JSON object per user input) by conversation id:
 * the earliest timestamp approximates `started_at` for summaries that do not
 * store one, and the newest human input keeps cards informative when the
 * summary preview is empty.
 */
function readAgyHistoryPrompts(historyPath) {
  const byConversation = new Map();
  if (!historyPath || !fs.existsSync(historyPath)) return byConversation;
  let content;
  try {
    content = fs.readFileSync(historyPath, "utf8");
  } catch {
    return byConversation;
  }
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let entry;
    try {
      entry = JSON.parse(trimmed);
    } catch {
      continue;
    }
    const conversationId = typeof entry.conversationId === "string" ? entry.conversationId : null;
    const text = typeof entry.display === "string" ? entry.display.trim() : "";
    const ts = Number(entry.timestamp);
    if (!conversationId || !text || !Number.isFinite(ts)) continue;
    const record = byConversation.get(conversationId) || { firstAt: ts, lastPrompt: null };
    if (ts < record.firstAt) record.firstAt = ts;
    // Only real prompts become card previews; slash commands such as `/resume`
    // say nothing about what the session was doing.
    if (entry.type !== "slash_command") record.lastPrompt = text;
    byConversation.set(conversationId, record);
  }
  return byConversation;
}

// conversation_id → fingerprint, so unchanged rows cost one string compare.
const rowFingerprints = new Map();

function agyRowFingerprint(row) {
  return [
    row.last_modified_time || "",
    row.last_user_input_time || "",
    row.step_count ?? 0,
    row.status || "",
    row.killed ? 1 : 0,
    row.not_fully_idle ? 1 : 0,
    row.title || "",
  ].join("|");
}

function agySessionDisplayName(row, history) {
  const title = typeof row.title === "string" ? row.title.trim() : "";
  if (title) return title;
  const preview = typeof row.preview === "string" ? row.preview.trim() : "";
  if (preview) return preview.length > 80 ? `${preview.slice(0, 77)}…` : preview;
  const prompt = history?.lastPrompt;
  if (prompt) return prompt.length > 80 ? `${prompt.slice(0, 77)}…` : prompt;
  return null;
}

function upsertAgySession(dbModule, row, id, source, prompts) {
  const history = prompts.get(id);
  const mapped = mapAgyStatus(row.status, row.killed, row.not_fully_idle);
  const startedAt =
    (history ? new Date(history.firstAt).toISOString() : null) ||
    parseAgyTime(row.last_user_input_time) ||
    parseAgyTime(row.last_modified_time) ||
    new Date().toISOString();
  const endedAt = mapped.session === "active" ? null : parseAgyTime(row.last_modified_time);
  const displayName = agySessionDisplayName(row, history);
  const name = displayName || `AGY · ${id.slice(0, 8)}`;
  const cwd = agyWorkspaceToCwd(row.workspace_uris);
  const promptPreview =
    (
      (typeof row.preview === "string" ? row.preview.trim() : "") ||
      history?.lastPrompt ||
      ""
    ).slice(0, MAX_PROMPT_PREVIEW) || null;
  const metadata = JSON.stringify({
    agy: {
      status: row.status || null,
      step_count: row.step_count ?? 0,
      agent_name: row.agent_name || null,
      parent_conversation_id: row.parent_conversation_id || null,
      nesting_depth: row.nesting_depth ?? 0,
      killed: !!row.killed,
      not_fully_idle: !!row.not_fully_idle,
      last_modified_time: row.last_modified_time || null,
      last_user_input_time: row.last_user_input_time || null,
    },
  });

  const existing = dbModule.stmts.getSession.get(id);
  const agentId = `agy:${id}`;
  if (!existing) {
    dbModule.stmts.insertAgySession.run(
      id,
      name,
      mapped.session,
      cwd,
      null,
      source,
      startedAt,
      endedAt,
      metadata,
      promptPreview
    );
    dbModule.stmts.insertAgent.run(
      agentId,
      id,
      "AGY",
      "main",
      null,
      mapped.agent,
      displayName,
      null,
      metadata
    );
    return {
      changed: true,
      created: true,
      session: dbModule.stmts.getSession.get(id),
      agent: dbModule.stmts.getAgent.get(agentId),
      events: [],
    };
  }

  // Reactivation must clear a stored ended_at; updateSession's COALESCE
  // cannot null a column by design, so clear first, then refresh fields.
  if (mapped.session === "active") dbModule.stmts.reactivateSession.run(id);
  dbModule.stmts.updateSession.run(
    name,
    mapped.session,
    mapped.session === "active" ? null : endedAt,
    metadata,
    id
  );
  dbModule.stmts.updateSessionCardPromptPreview.run(promptPreview, id, promptPreview);

  const agent = dbModule.stmts.getAgent.get(agentId);
  if (!agent) {
    dbModule.stmts.insertAgent.run(
      agentId,
      id,
      "AGY",
      "main",
      null,
      mapped.agent,
      displayName,
      null,
      metadata
    );
  } else if (agent.status !== mapped.agent) {
    dbModule.stmts.updateAgent.run(
      null,
      mapped.agent,
      null,
      null,
      mapped.agent === "completed" || mapped.agent === "error" ? endedAt : null,
      null,
      agentId
    );
  }
  return {
    changed: true,
    created: false,
    session: dbModule.stmts.getSession.get(id),
    agent: dbModule.stmts.getAgent.get(agentId),
    events: [],
  };
}

/**
 * Mirror the Antigravity summary store into dashboard sessions.
 * @param {object} dbModule - server/db module (or test harness equivalent).
 * @param {{dbPath?: string, historyPath?: string|null, source?: string, force?: boolean}} options
 * @returns {Array<{changed: boolean, created: boolean, session: object,
 *   agent: object, events: never[]}>} rows that changed in this pass.
 */
function syncAgySessions(dbModule = require("../db"), options = {}) {
  const summaryPath = options.dbPath || getAgySummaryDbPath();
  if (!summaryPath || !fs.existsSync(summaryPath)) return [];
  const source = options.source || "local";
  const historyPath =
    options.historyPath === null ? null : options.historyPath || getAgyHistoryPath();
  const prompts = readAgyHistoryPrompts(historyPath);

  let store;
  try {
    const open = readOnlyOpener();
    if (!open) return [];
    store = open(summaryPath);
    const rows = store.prepare(SUMMARY_QUERY).all();
    const changed = [];
    for (const row of rows) {
      const id = typeof row.conversation_id === "string" ? row.conversation_id : "";
      if (!id) continue;
      const fingerprint = agyRowFingerprint(row);
      const key = `${source}:${id}`;
      if (!options.force && rowFingerprints.get(key) === fingerprint) continue;
      rowFingerprints.set(key, fingerprint);
      const result = upsertAgySession(dbModule, row, id, source, prompts);
      if (result) changed.push(result);
    }
    return changed;
  } catch {
    // The summary store is optional and owned by Antigravity; a locked or
    // evolving schema must never affect dashboard startup or polling.
    return [];
  } finally {
    try {
      store?.close();
    } catch {
      // Read-only access must never affect Antigravity.
    }
  }
}

module.exports = {
  getAgyHome,
  getAgyHistoryPath,
  getAgySummaryDbPath,
  mapAgyStatus,
  parseAgyTime,
  syncAgySessions,
};
