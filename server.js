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
const WEBINAR_FORM_ID  = process.env.WEBINAR_FORM_ID;
const WEBINAR_FORM_HS_ID = process.env.WEBINAR_FORM_HS_ID;
const LP_TAB     = "HS Page Visits";
const LP_PAGE_ID = "403572489414";
const LP_HEADERS = [
  "Date", "Page Views", "New Visitors", "Entrances", "Exits",
  "Bounce Rate", "Avg Time on Page (s)", "Submissions", "Contacts", "Customers",
];

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
{
  id: "3364165843",
  label: "Operations Tickets",
  stages: [
    { id: "5272068333", label: "Suggestions" },
    { id: "4609840367", label: "To Do" },
    { id: "4609840368", label: "Doing" },
    { id: "4609840369", label: "Review" },
    { id: "4609840370", label: "Completed" },
    { id: "4609840371", label: "Blocked" },
    { id: "4609840372", label: "Backlog" },
  ],
},
];

const WEBINAR_HS_TAB     = "Webinar HS";
const WEBINAR_HS_HEADERS = [
  "Submission ID", "Submitted At", "Month",
  "First Name", "Email", "UTM Source", "UTM Content", "Page URL",
];

const OPS_TAB = "Operations Tickets";
const OPS_HEADERS = [
  "Project ID", "Project Name", "Pipeline", "Stage",
  "Owner", "Created Date", "Last Modified", "Due Date",
  "Date Entered Suggestions", "Date Entered To Do", "Date Entered Doing",
  "Date Entered Review", "Date Entered Completed", "Date Entered Blocked",
  "Date Entered Backlog",
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

const WEBINAR_TAB     = "Webinar";
const WEBINAR_HEADERS = [
  "Timestamp", "Form Name", "Form ID", "Status", "Submission ID", "Month",
  "First Name", "Last Name", "Email", "Mobile Phone Number",
  "utm_source", "utm_content",
  "Will the retirement planning be just yourself or include a spouse/partner",
  "About how much have you saved for retirement?",
  "Are you retired, looking to retire in the next 5 years, or looking to retire in the next 10 years?",
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
async function ensureWebinarHeaders(sheets) {
  const rows = await readTab(sheets, WEBINAR_TAB);
  if (!rows.length || rows[0][0] !== "Timestamp") {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${WEBINAR_TAB}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [WEBINAR_HEADERS] },
    });
  }
}

async function batchLogWebinar(sheets, submissions) {
  const rows = submissions.map(sub => {
    const q   = sub.questions || [];
    const now = sub.timestamp || new Date().toISOString();
    const month = new Date(now).toLocaleString("default", { month: "long", year: "numeric" });
    const scheduling = sub.scheduling || [];
    const meeting = scheduling[0]?.value;
    const status = sub.status || (meeting?.eventStartTime ? "Completed" : "In Progress");
    return [
      now,
      sub.formName || "",
      sub.formId || "",
      status,
      sub.submissionId || "",
      month,
      extractFilloutField(q, "First Name", "firstname"),
      extractFilloutField(q, "Last Name", "lastname"),
      extractFilloutField(q, "Email"),
      extractFilloutField(q, "Mobile Phone", "phone"),
      extractFilloutField(q, "utm_source"),
      extractFilloutField(q, "utm_content"),
      extractFilloutField(q, "spouse", "partner"),
      extractFilloutField(q, "how much have you saved"),
      extractFilloutField(q, "retired", "looking to retire"),
    ];
  });
  await appendRows(sheets, WEBINAR_TAB, rows);
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

async function syncWebinar() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, WEBINAR_TAB);
    await ensureWebinarHeaders(sheets);

    if (!FILLOUT_API_KEY || !WEBINAR_FORM_ID) return;

    const existingRows = await readTab(sheets, WEBINAR_TAB);
    const existingIds = new Set(existingRows.slice(1).map(r => r[4]).filter(Boolean));

    let offset = 0;
    const limit = 150;
    let total   = Infinity;
    const all   = [];

    while (offset < total) {
      const url = `https://api.fillout.com/v1/api/forms/${WEBINAR_FORM_ID}/submissions?limit=${limit}&offset=${offset}&sort=desc&includePartial=true`;
      const res = await fetch(url, { headers: { Authorization: `Bearer ${FILLOUT_API_KEY}` } });
      const data = await res.json();

      if (offset === 0) total = data.totalResponses ?? 0;

      const responses = data.responses || [];
      if (!responses.length) break;

      for (const sub of responses) {
        if (existingIds.has(sub.submissionId)) break;
        all.push({
          formId: WEBINAR_FORM_ID, formName: "Webinar Form",
          submissionId: sub.submissionId,
          timestamp: sub.submissionTime || sub.lastUpdatedAt || new Date().toISOString(),
          questions: sub.questions || [],
          scheduling: sub.scheduling || [],
        });
      }
      offset += limit;
    }

    if (all.length) await batchLogWebinar(sheets, all);
    console.log(`[Webinar sync] ${all.length} submissions synced`);
  } catch (err) {
    console.error("[Webinar sync error]", err.message);
  }
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

async function fetchAllOpsTickets() {
  const properties = [
    "hs_name", "hs_pipeline", "hs_pipeline_stage",
    "hubspot_owner_id", "createdate", "hs_lastmodifieddate",
    "hs_due_date",
    "hs_date_entered_5272068333",
    "hs_date_entered_4609840367",
    "hs_date_entered_4609840368",
    "hs_date_entered_4609840369",
    "hs_date_entered_4609840370",
    "hs_date_entered_4609840371",
    "hs_date_entered_4609840372",
  ].join(",");

  const tickets = [];
  let after = null;

  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: "hs_pipeline", operator: "EQ", value: "3364165843" }] }],
      properties: properties.split(","),
      limit: 100,
      ...(after ? { after } : {}),
    };

    const res = await fetch("https://api.hubapi.com/crm/v3/objects/projects/search", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${HUBSPOT_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.text();
      throw new Error(`HubSpot ops tickets API error ${res.status}: ${err}`);
    }

    const data = await res.json();
    tickets.push(...(data.results || []));
    if (data.paging?.next?.after) {
      after = data.paging.next.after;
    } else {
      break;
    }
  }
  return tickets;
}

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

async function syncOperationsTickets() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, OPS_TAB);

    const tickets = await fetchAllOpsTickets();
    console.log(`[Ops sync] Fetched ${tickets.length} tickets`);

    const rows = [OPS_HEADERS];

    for (const t of tickets) {
      const props = t.properties || {};
      rows.push([
        t.id,
        props.hs_name || "",
        "Operations Tickets",
        stageLabel(props.hs_pipeline_stage),
        props.hubspot_owner_id || "",
        props.createdate ? msToDate(new Date(props.createdate).getTime()) : "",
        props.hs_lastmodifieddate ? msToDate(new Date(props.hs_lastmodifieddate).getTime()) : "",
        props.hs_due_date || "",
        msToDate(props.hs_date_entered_5272068333),
        msToDate(props.hs_date_entered_4609840367),
        msToDate(props.hs_date_entered_4609840368),
        msToDate(props.hs_date_entered_4609840369),
        msToDate(props.hs_date_entered_4609840370),
        msToDate(props.hs_date_entered_4609840371),
        msToDate(props.hs_date_entered_4609840372),
      ]);
    }

    await writeTab(sheets, OPS_TAB, rows);
    console.log(`[Ops sync] Wrote ${tickets.length} rows to "${OPS_TAB}"`);

  } catch (err) {
    console.error("[Ops sync error]", err.message);
  }
}

async function syncWebinarForm() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, WEBINAR_HS_TAB);

    // Load existing submission IDs to avoid duplicates
    const existing = await readTab(sheets, WEBINAR_HS_TAB);
    const existingIds = new Set(existing.slice(1).map(r => r[0]).filter(Boolean));

    // Ensure headers
    if (!existing.length || existing[0][0] !== "Submission ID") {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${WEBINAR_HS_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [WEBINAR_HS_HEADERS] },
      });
    }

    let after     = null;
    const newRows = [];

    while (true) {
      const url = `https://api.hubapi.com/form-integrations/v1/submissions/forms/${WEBINAR_FORM_HS_ID}?limit=50${after ? `&after=${after}` : ""}`;
      const res = await fetch(url, {
        headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}` },
      });
      if (!res.ok) {
        const err = await res.text();
        throw new Error(`HubSpot form submissions error ${res.status}: ${err}`);
      }
      const data = await res.json();

      for (const sub of data.results || []) {
        const id = sub.conversionId;
        if (existingIds.has(id)) continue;

        const fields = {};
        for (const v of sub.values || []) fields[v.name] = v.value;

        const submittedAt = new Date(sub.submittedAt).toISOString();
        const month = new Date(sub.submittedAt).toLocaleString("default", { month: "long", year: "numeric" });

        newRows.push([
          id,
          submittedAt,
          month,
          fields.firstname || "",
          fields.email || "",
          fields.utm_source || "",
          fields.utm_content || "",
          sub.pageUrl || "",
        ]);
      }

      if (data.paging?.next?.after) {
        after = data.paging.next.after;
      } else {
        break;
      }
    }

    if (newRows.length) {
      await appendRows(sheets, WEBINAR_HS_TAB, newRows);
      console.log(`[Webinar sync] Appended ${newRows.length} new submissions`);
    } else {
      console.log(`[Webinar sync] No new submissions`);
    }

  } catch (err) {
    console.error("[Webinar sync error]", err.message);
  }
}

async function syncLandingPageAnalytics() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, LP_TAB);

    // Load existing dates to avoid duplicates
    const existing = await readTab(sheets, LP_TAB);
    const existingDates = new Set(existing.slice(1).map(r => r[0]).filter(Boolean));

    // Ensure headers
    if (!existing.length || existing[0][0] !== "Date") {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${LP_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [LP_HEADERS] },
      });
    }

    // Fetch last 30 days
    const end   = new Date();
    const start = new Date();
    start.setDate(start.getDate() - 30);

    const fmt = d => d.toISOString().split("T")[0].replace(/-/g, "");
    const url = `https://api.hubapi.com/analytics/v2/reports/landing-pages/summarize/daily?start=${fmt(start)}&end=${fmt(end)}&f=${LP_PAGE_ID}`;

    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}` },
    });
    if (!res.ok) {
      const err = await res.text();
      throw new Error(`HubSpot analytics error ${res.status}: ${err}`);
    }

    const data = await res.json();
    const newRows = [];

    for (const [date, values] of Object.entries(data)) {
      if (existingDates.has(date)) continue;
      const d = values[0] || {};
      newRows.push([
        date,
        d.rawViews || 0,
        d.newVisitorRawViews || 0,
        d.entrances || 0,
        d.exits || 0,
        d.pageBounceRate ? Math.round(d.pageBounceRate * 100) + "%" : "0%",
        d.timePerPageview ? Math.round(d.timePerPageview) : 0,
        d.submissions || 0,
        d.contacts || 0,
        d.customers || 0,
      ]);
    }

    // Sort rows by date ascending
    newRows.sort((a, b) => a[0].localeCompare(b[0]));

    if (newRows.length) {
      await appendRows(sheets, LP_TAB, newRows);
      console.log(`[LP Analytics] Appended ${newRows.length} new days`);
    } else {
      console.log(`[LP Analytics] No new data`);
    }

  } catch (err) {
    console.error("[LP Analytics error]", err.message);
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
if (!submissionId) {
  console.log(`[Fillout webhook] Skipping - no submission ID`);
  return;
}

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

     // Skip if already exists as Completed
    const existingRows = await readTab(sheets, FILLOUT_LOG_TAB);
    const existingCompleted = new Set(
      existingRows.slice(1)
        .filter(r => r[3] === "Completed")
        .map(r => r[4])
        .filter(Boolean)
    );
    if (existingCompleted.has(submissionId)) {
      console.log(`[Fillout webhook] Skipping already completed: ${submissionId}`);
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

app.post("/webhook/webinar", async (req, res) => {
  res.json({ success: true });
  try {
    const event        = req.body;
    const eventType    = event.eventType || "submission.completed";
    const submissionId = event.submissionId || event.submission_id || "";

    if (!submissionId) return;

    const status = (eventType === "submission.partial" || eventType === "submission.in_progress")
      ? "In Progress"
      : "Completed";

    const formId   = event.formId || WEBINAR_FORM_ID || "unknown-form";
    const formName = event.formName || "Webinar Form";
    const questions = event.questions || [];
    const scheduling = event.scheduling || [];

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, WEBINAR_TAB);
    await ensureWebinarHeaders(sheets);

    const existingRows = await readTab(sheets, WEBINAR_TAB);
    const existingCompleted = new Set(
      existingRows.slice(1)
        .filter(r => r[3] === "Completed")
        .map(r => r[4])
        .filter(Boolean)
    );
    if (existingCompleted.has(submissionId)) return;

    await batchLogWebinar(sheets, [{
      formId, formName, status, submissionId,
      timestamp: new Date().toISOString(), questions, scheduling,
    }]);

    console.log(`[Webinar webhook] ${status} | ${submissionId}`);
  } catch (err) {
    console.error("[Webinar webhook error]", err.message);
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

app.get("/sync-ops", async (req, res) => {
  syncOperationsTickets();
  res.json({ success: true, message: "Operations Tickets sync started in background" });
});

app.get("/sync-webinar", async (req, res) => {
  syncWebinar();
  res.json({ success: true, message: "Webinar sync started in background" });
});

app.get("/sync-webinar-hs", async (req, res) => {
  syncWebinarForm();
  res.json({ success: true, message: "Webinar HS form sync started in background" });
});

app.get("/health", (_, res) => res.json({ status: "ok" }));

app.get("/sync-lp", async (req, res) => {
  syncLandingPageAnalytics();
  res.json({ success: true, message: "Landing page analytics sync started" });
});

// ── SCHEDULES ─────────────────────────────────────────────────────────────────
// Delay startup syncs so Render health check passes first
setTimeout(() => {
  syncInProgress();
  syncHubSpotProjects();
  syncOperationsTickets();
  syncWebinar();
  syncWebinarForm(); 
  syncLandingPageAnalytics();
}, 10_000);

setInterval(syncInProgress,       60 * 60 * 1000); // every hour
setInterval(syncHubSpotProjects,  60 * 60 * 1000); // every hour
setInterval(syncOperationsTickets,  60 * 60 * 1000); // every hour  ← ADD THIS
setInterval(syncWebinar, 60 * 60 * 1000); // every hour
setInterval(syncWebinarForm, 60 * 60 * 1000); // every hour
setInterval(syncLandingPageAnalytics, 60 * 60 * 1000); // every hour



app.listen(PORT, () => console.log(`Webhook server running on port ${PORT}`));