"use strict";

const express = require("express");
const { google } = require("googleapis");

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// ── ENV ──────────────────────────────────────────────────────────────────────
const HUBSPOT_TOKEN    = process.env.HUBSPOT_TOKEN;
const SPREADSHEET_ID   = process.env.SPREADSHEET_ID;
const FILLOUT_API_KEY  = process.env.FILLOUT_API_KEY;
const FILLOUT_FORM_ID  = process.env.FILLOUT_FORM_ID;

// ── PIPELINE / STAGE MAP (baked in from confirmed API response) ──────────────
// Structure: stageId → { label, pipelineLabel }
const STAGE_MAP = {};

const PIPELINES_DATA = [
  {
    id: "3078420705",
    label: "Planning Engagement",
    stages: [
      { id: "5163833551", label: "Stage 1: Ready for Nitrogen + Plan Build (Not Started)" },
      { id: "5165603035", label: "Stage 2: Nitrogen + Base Plan Build (In-Process)" },
      { id: "5165576441", label: "Stage 3: Draft Base Plan Done, Waiting on Requested Data from Client" },
      { id: "5165603036", label: "Stage 4: HFP Quality Assurance" },
      { id: "5165603037", label: "Stage 5: Make Plan Updates, Create Scenarios, & Create LOOM for Advisor" },
      { id: "5165603038", label: "Stage 6: Ready for Plan Proposal" },
      { id: "5165603039", label: "Stage 7: Make Plan Updates & Choose Scenario (Post-Plan Proposal)" },
      { id: "5171109092", label: "Stage 8: Ready for Opt/TEW/Invest 1 (Not Started)" },
      { id: "5165603040", label: "Stage 9: Optimizations & Withdrawals (TEW)" },
      { id: "5165603041", label: "Stage 10: Investments 1" },
      { id: "5165603042", label: "Stage 11: HFP Quality Assurance" },
      { id: "5165603043", label: "Stage 12: Make Optimizations/TEW Updates, Send WA LOOM" },
      { id: "5165603044", label: "Stage 13: Planning Process Completed" },
      { id: "4214753508", label: "OLD Stage 2 (archived)" },
      { id: "4805782765", label: "OLD Stage 4 (archived)" },
      { id: "4225526006", label: "OLD Stage 5 (archived)" },
    ],
  },
];

// Build flat stageId → { label, pipelineId, pipelineLabel } lookup
const PIPELINE_MAP = {}; // pipelineId → label
for (const pipeline of PIPELINES_DATA) {
  PIPELINE_MAP[pipeline.id] = pipeline.label;
  for (const stage of pipeline.stages) {
    STAGE_MAP[stage.id] = {
      label:         stage.label,
      pipelineId:    pipeline.id,
      pipelineLabel: pipeline.label,
    };
  }
}

function stageLabel(stageId) {
  return stageId ? (STAGE_MAP[stageId]?.label || stageId) : "";
}
function pipelineLabel(pipelineId) {
  return pipelineId ? (PIPELINE_MAP[pipelineId] || pipelineId) : "";
}

// ── GOOGLE AUTH ──────────────────────────────────────────────────────────────
function getGoogleAuth() {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON not set");
  const creds = JSON.parse(raw);
  return new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
}

// ── SHEET HELPERS ─────────────────────────────────────────────────────────────

// Ensure a tab exists; if not, create it
async function ensureTab(sheets, tabName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const exists = meta.data.sheets.some(s => s.properties.title === tabName);
  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{ addSheet: { properties: { title: tabName } } }],
      },
    });
    console.log(`Created tab: ${tabName}`);
  }
}

// Read all rows from a tab (returns array of arrays)
async function readTab(sheets, tabName) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${tabName}!A:Z`,
    });
    return res.data.values || [];
  } catch {
    return [];
  }
}

// Clear a tab and write fresh rows
async function writeTab(sheets, tabName, rows) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range: `${tabName}!A:Z`,
  });
  if (rows.length) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${tabName}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: rows },
    });
  }
}

// Append rows to a tab
async function appendRows(sheets, tabName, rows) {
  if (!rows.length) return;
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${tabName}!A1`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rows },
  });
}

// ── FILLOUT SYNC ──────────────────────────────────────────────────────────────
const FILLOUT_LOG_TAB     = "Fillout";
const FILLOUT_LOG_HEADERS = [
    "Timestamp", "Form Name", "Form ID", "Status", "Submission ID", "Month",
  "Before Continuing - Did You Watch The Video Above?",
  "First Name", "Last Name", "Email", "Mobile Phone Number",
  "Will the retirement planning be just yourself or include a spouse/partner",
  "About how much have you saved for retirement?",
  "Are you retired, looking to retire in the next 5 years, or looking to retire in the next 10 years?",
  "How many of our educational YouTube videos would you guess you've watched?",
  "How did you hear about Peak Financial Planning?",
  "(OPTIONAL) Please share any additional information related to your goals or pain points you think would be helpful",
];

async function ensureFilloutHeaders(sheets) {
  const rows = await readTab(sheets, FILLOUT_LOG_TAB);
  if (!rows.length || rows[0][0] !== "Timestamp") {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${FILLOUT_LOG_TAB}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [FILLOUT_LOG_HEADERS] },
    });
  }
}

function extractFilloutField(questions, ...names) {
  for (const q of questions) {
    if (names.some(n => q.name?.toLowerCase().includes(n.toLowerCase()))) {
      const val = q.value;
      if (Array.isArray(val)) return val.join(", ");
      if (val && typeof val === "object") return JSON.stringify(val);
      return val || "";
    }
  }
  return "";
}

async function batchLogSubmissions(sheets, submissions) {
  const rows = submissions.map(sub => {
    const q   = sub.questions || [];
    const now = sub.timestamp || new Date().toISOString();
    const month = new Date(now).toLocaleString("default", { month: "long", year: "numeric" });
    return [
      now,
      sub.formName || "",
      sub.formId || "",
      (() => {
  if (sub.status) return sub.status;
  const scheduling = sub.scheduling || [];
  const meeting = scheduling[0]?.value;
  return (meeting?.eventStartTime) ? "Completed" : "In Progress";
})(),
      sub.submissionId || "",
      month,
      extractFilloutField(q, "Before Continuing"),
      extractFilloutField(q, "First Name", "firstname"),
      extractFilloutField(q, "Last Name", "lastname"),
      extractFilloutField(q, "Email"),
      extractFilloutField(q, "Mobile Phone", "phone"),
      extractFilloutField(q, "spouse", "partner"),
      extractFilloutField(q, "how much have you saved"),
      extractFilloutField(q, "retired", "looking to retire"),
      extractFilloutField(q, "youtube videos"),
      extractFilloutField(q, "how did you hear"),
      extractFilloutField(q, "additional information", "goals or pain points"),
    ];
  });
  await appendRows(sheets, FILLOUT_LOG_TAB, rows);
}


async function syncInProgress() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, FILLOUT_LOG_TAB);
    await ensureFilloutHeaders(sheets);

    if (!FILLOUT_API_KEY || !FILLOUT_FORM_ID) return;

    // Read existing submission IDs to prevent duplicates
    const existingRows = await readTab(sheets, FILLOUT_LOG_TAB);
    const existingIds = new Set(existingRows.slice(1).map(r => r[4]).filter(Boolean));

    let offset = 0;
    const limit = 150;
    let total   = Infinity;
    const all   = [];

    while (offset < total) {
      const url = `https://api.fillout.com/v1/api/forms/${FILLOUT_FORM_ID}/submissions?limit=${limit}&offset=${offset}&sort=desc&includePartial=true`;
      const res = await fetch(url, { headers: { Authorization: `Bearer ${FILLOUT_API_KEY}` } });
      const data = await res.json();

      if (offset === 0) total = data.totalResponses ?? 0;

      const responses = data.responses || [];
      if (!responses.length) break;

      for (const sub of responses) {
        if (existingIds.has(sub.submissionId)) break;
if (new Date(sub.submissionTime) < new Date("2026-03-01T00:00:00.000Z")) { break; }
        all.push({
  formId: FILLOUT_FORM_ID, formName: "Fillout Form",
  submissionId: sub.submissionId,
  timestamp: sub.submissionTime || sub.lastUpdatedAt || new Date().toISOString(),
  questions: sub.questions || [],
  scheduling: sub.scheduling || [],
});
      }
      offset += limit;
    }

    if (all.length) await batchLogSubmissions(sheets, all);
    console.log(`[Fillout sync] ${all.length} submissions synced`);
  } catch (err) {
    console.error("[Fillout sync error]", err.message);
  }
}

// ── HUBSPOT PROJECTS SYNC ─────────────────────────────────────────────────────
const PROJECTS_TAB     = "HubSpot Projects";
const PROJECTS_HEADERS = [
  "Project ID", "Project Name", "Pipeline", "Stage",
  "FP Owner", "WA Owner", "HubSpot Owner ID",
  "Card Due Date", "Target Due Date",
  "Last Modified", "Created Date",
  "Date Entered Stage 1", "Date Entered Stage 2", "Date Entered Stage 3",
  "Date Entered Stage 4", "Date Entered Stage 5", "Date Entered Stage 6",
  "Date Entered Stage 7", "Date Entered Stage 8", "Date Entered Stage 9",
  "Date Entered Stage 10", "Date Entered Stage 11", "Date Entered Stage 12",
  "Date Entered Stage 13",
];

const STAGE_CHANGE_TAB     = "Stage Change Log";
const STAGE_CHANGE_HEADERS = [
  "Timestamp", "Month", "Project ID", "Project Name",
  "Pipeline", "From Stage", "To Stage", "Source",
];

// In-memory cache of last known stages: projectId → stageId
const lastKnownStage = {};

async function fetchAllProjects() {
 const properties = [
    "hs_name", "hs_pipeline", "hs_pipeline_stage",
    "hubspot_owner_id", "fp_owner", "wa_owner",
    "card_due_date_", "hs_target_due_date",
    "hs_lastmodifieddate", "createdate",
    "hs_date_entered_5163833551",
    "hs_date_entered_5165603035",
    "hs_date_entered_5165576441",
    "hs_date_entered_5165603036",
    "hs_date_entered_5165603037",
    "hs_date_entered_5165603038",
    "hs_date_entered_5165603039",
    "hs_date_entered_5171109092",
    "hs_date_entered_5165603040",
    "hs_date_entered_5165603041",
    "hs_date_entered_5165603042",
    "hs_date_entered_5165603043",
    "hs_date_entered_5165603044",
  ].join(",");

  const projects = [];
  let after = null;

  while (true) {
    const url = `https://api.hubapi.com/crm/v3/objects/projects?limit=100&properties=${properties}${after ? `&after=${after}` : ""}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}` },
    });
    if (!res.ok) {
      const err = await res.text();
      throw new Error(`HubSpot projects API error ${res.status}: ${err}`);
    }
    const data = await res.json();
    projects.push(...(data.results || []));
    if (data.paging?.next?.after) {
      after = data.paging.next.after;
    } else {
      break;
    }
  }
  return projects;
}

// ── PERSISTENT STAGE CACHE (replaces in-memory lastKnownStage) ───────────────
// Uses a hidden "_stage_cache" tab in Google Sheets to persist stage snapshots
// across server restarts. Structure: col A = projectId, col B = stageId

const CACHE_TAB = "HubSpot Cache";

async function loadStageCache(sheets) {
  const rows = await readTab(sheets, CACHE_TAB);
  const cache = {};
  for (const row of rows) {
    if (row[0] && row[1]) cache[row[0]] = row[1];
  }
  return cache;
}

async function saveStageCache(sheets, cache) {
  const rows = Object.entries(cache).map(([id, stageId]) => [id, stageId]);
  await writeTab(sheets, CACHE_TAB, rows);
}
function msToDate(ms) {
  if (!ms || ms === "0" || parseInt(ms) === 0) return "";
  const d = new Date(parseInt(ms));
  if (d.getFullYear() < 2020) return "";
  if (d.getFullYear() === 2026 && d.getMonth() === 3 && d.getDate() === 12) return "";
  return `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
}
// ── UPDATED syncHubSpotProjects() ─────────────────────────────────────────────
// Replace your existing syncHubSpotProjects() function with this one entirely.


async function syncHubSpotProjects() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, PROJECTS_TAB);
    await ensureTab(sheets, STAGE_CHANGE_TAB);
    await ensureTab(sheets, CACHE_TAB);

    const stageCache = await loadStageCache(sheets);
    const projects   = await fetchAllProjects();
    console.log(`[Projects sync] Fetched ${projects.length} projects`);

    const now   = new Date().toISOString();
    const month = new Date().toLocaleString("default", { month: "long", year: "numeric" });

    const projectRows  = [PROJECTS_HEADERS];
    const stageChanges = [];

    for (const p of projects) {
      const props = p.properties || {};
      if (props.hs_pipeline !== "3078420705") continue;

      const id       = p.id;
      const name     = props.hs_name || "";
      const pipeline = pipelineLabel(props.hs_pipeline);
      const stageId  = props.hs_pipeline_stage || "";
      const stage    = stageLabel(stageId);

      projectRows.push([
        id, name, pipeline, stage,
        props.fp_owner || "",
        props.wa_owner || "",
        props.hubspot_owner_id || "",
        props.card_due_date_ || "",
        props.hs_target_due_date || "",
        props.hs_lastmodifieddate || "",
        props.createdate || "",
        msToDate(props.hs_date_entered_5163833551),
        msToDate(props.hs_date_entered_5165603035),
        msToDate(props.hs_date_entered_5165576441),
        msToDate(props.hs_date_entered_5165603036),
        msToDate(props.hs_date_entered_5165603037),
        msToDate(props.hs_date_entered_5165603038),
        msToDate(props.hs_date_entered_5165603039),
        msToDate(props.hs_date_entered_5171109092),
        msToDate(props.hs_date_entered_5165603040),
        msToDate(props.hs_date_entered_5165603041),
        msToDate(props.hs_date_entered_5165603042),
        msToDate(props.hs_date_entered_5165603043),
        msToDate(props.hs_date_entered_5165603044),
      ]);

      const cachedStageId = stageCache[id];
      if (cachedStageId !== undefined && cachedStageId !== stageId) {
        stageChanges.push([
          now, month, id, name,
          pipeline,
          stageLabel(cachedStageId),
          stage,
          "API Sync",
        ]);
        console.log(`[Stage change] ${name}: ${stageLabel(cachedStageId)} → ${stage}`);
      }
      stageCache[id] = stageId;
    }

    await writeTab(sheets, PROJECTS_TAB, projectRows);
    console.log(`[Projects sync] Wrote ${projectRows.length - 1} rows to "${PROJECTS_TAB}"`);

    const existingChanges = await readTab(sheets, STAGE_CHANGE_TAB);
    if (!existingChanges.length || existingChanges[0][0] !== "Timestamp") {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${STAGE_CHANGE_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [STAGE_CHANGE_HEADERS] },
      });
    }

    if (stageChanges.length) {
      await appendRows(sheets, STAGE_CHANGE_TAB, stageChanges);
      console.log(`[Stage Change Log] Logged ${stageChanges.length} new change(s)`);
    }

    await saveStageCache(sheets, stageCache);
    console.log(`[Stage cache] Saved ${Object.keys(stageCache).length} project stages`);

  } catch (err) {
    console.error("[Projects sync error]", err.message);
  }
}

// ── HUBSPOT WEBHOOK (real-time stage changes) ─────────────────────────────────

// ── HUBSPOT WEBHOOK (real-time stage changes) ─────────────────────────────────
app.post("/webhook/hubspot", async (req, res) => {
  res.sendStatus(200); // acknowledge immediately

  try {
    const events = Array.isArray(req.body) ? req.body : [req.body];
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, STAGE_CHANGE_TAB);

    const rows = [];

    for (const event of events) {
      const objectId    = String(event.objectId || event.id || "");
      const propertyName = event.propertyName || event.property || "";

      // Only process pipeline stage changes
      if (!propertyName.includes("pipeline_stage") && propertyName !== "hs_pipeline_stage") continue;

      const newStageId = event.propertyValue || event.value || "";
      const oldStageId = event.previousPropertyValue || event.previousValue || lastKnownStage[objectId] || "";

      const now   = new Date().toISOString();
      const month = new Date().toLocaleString("default", { month: "long", year: "numeric" });

      // Try to get project name from HubSpot
      let projectName = "";
      let pipelineId  = "";
      try {
        const res2 = await fetch(
          `https://api.hubapi.com/crm/v3/objects/projects/${objectId}?properties=hs_name,hs_pipeline`,
          { headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}` } }
        );
        if (res2.ok) {
          const d    = await res2.json();
          projectName = d.properties?.hs_name || "";
          pipelineId  = d.properties?.hs_pipeline || "";
        }
      } catch { /* non-fatal */ }

      rows.push([
        now, month, objectId, projectName,
        pipelineLabel(pipelineId),
        stageLabel(oldStageId),
        stageLabel(newStageId),
        "Webhook",
      ]);

      // Update in-memory cache
      lastKnownStage[objectId] = newStageId;
    }

    // Ensure headers
    const existing = await readTab(sheets, STAGE_CHANGE_TAB);
    if (!existing.length || existing[0][0] !== "Timestamp") {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${STAGE_CHANGE_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [STAGE_CHANGE_HEADERS] },
      });
    }

    if (rows.length) {
      await appendRows(sheets, STAGE_CHANGE_TAB, rows);
      console.log(`[Webhook] Logged ${rows.length} stage change(s)`);
    }
  } catch (err) {
    console.error("[Webhook error]", err.message);
  }
});

// ── FILLOUT WEBHOOK ───────────────────────────────────────────────────────────
app.post("/webhook/fillout", async (req, res) => {
  res.json({ success: true });
  try {
    const event        = req.body;
    const eventType    = event.eventType || "submission.completed";
    const submissionId = event.submissionId || event.submission_id || "";

    const status = (eventType === "submission.partial" || eventType === "submission.in_progress")
      ? "In Progress"
      : "Completed";

    // Fetch full submission data from Fillout API
    let questions = event.questions || event.data?.questions || [];
    if (submissionId && (!questions.length)) {
      const apiRes = await fetch(
        `https://api.fillout.com/v1/api/forms/${FILLOUT_FORM_ID}/submissions/${submissionId}`,
        { headers: { Authorization: `Bearer ${FILLOUT_API_KEY}` } }
      );
      if (apiRes.ok) {
        const data = await apiRes.json();
        questions = data.questions || [];
      }
    }

    const formId   = event.formId || event.form_id || FILLOUT_FORM_ID || "unknown-form";
    const formName = event.formName || event.form_name || formId;

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, FILLOUT_LOG_TAB);
    await ensureFilloutHeaders(sheets);

    // Skip if already exists
    const existingRows = await readTab(sheets, FILLOUT_LOG_TAB);
    const existingIds = new Set(existingRows.slice(1).map(r => r[4]).filter(Boolean));
    if (existingIds.has(submissionId)) {
      console.log(`[Fillout webhook] Skipping duplicate: ${submissionId}`);
      return;
    }

    await batchLogSubmissions(sheets, [{
      formId, formName, status, submissionId,
      timestamp: new Date().toISOString(), questions,
    }]);

    console.log(`[Fillout webhook] ${status} | ${formName} | ${submissionId}`);
  } catch (err) {
    console.error("[Fillout webhook error]", err.message);
  }
});

// ── MANUAL ENDPOINTS ──────────────────────────────────────────────────────────
app.get("/sync", async (req, res) => {
  syncInProgress();
  res.json({ success: true, message: "Fillout sync started in background" });
});

app.get("/sync-projects", async (req, res) => {
  syncHubSpotProjects();
  res.json({ success: true, message: "HubSpot Projects sync started in background" });
});

app.get("/health", (_, res) => res.json({ status: "ok" }));

// ── SCHEDULES ─────────────────────────────────────────────────────────────────
// Delay startup syncs so Render health check passes first
setTimeout(() => {
  syncInProgress();
  syncHubSpotProjects();
}, 10_000);

setInterval(syncInProgress,       60 * 60 * 1000); // every hour
setInterval(syncHubSpotProjects,  60 * 60 * 1000); // every hour

app.listen(PORT, () => console.log(`Webhook server running on port ${PORT}`));