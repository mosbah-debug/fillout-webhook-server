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
const VIDALYTICS_API_KEY = process.env.VIDALYTICS_API_KEY;
const VIDALYTICS_VIDEO_ID = process.env.VIDALYTICS_VIDEO_ID;

const LP_TAB     = "HS Page Visits";
const LP_PAGE_ID = "403572489414";
const LP_HEADERS = [
  "Date", "Page Views", "New Visitors", "Entrances", "Exits",
  "Bounce Rate", "Avg Time on Page (s)", "Submissions", "Contacts", "Customers",
];

// ── PIPELINE / STAGE MAP ─────────────────────────────────────────────────────
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

const PIPELINE_MAP = {};
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
async function ensureTab(sheets, tabName) {
  try {
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
  } catch (err) {
    console.error(`[ensureTab error] ${tabName}:`, err.message);
  }
}

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

    const existing = await readTab(sheets, WEBINAR_HS_TAB);
    const existingIds = new Set(existing.slice(1).map(r => r[0]).filter(Boolean));

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
          id, submittedAt, month,
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

    const existing = await readTab(sheets, LP_TAB);
    const existingDates = new Set(existing.slice(1).map(r => r[0]).filter(Boolean));

    if (!existing.length || existing[0][0] !== "Date") {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${LP_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [LP_HEADERS] },
      });
    }

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

// ── VIDALYTICS SYNC ───────────────────────────────────────────────────────────
const VIDALYTICS_TAB         = "Vidalytics";
const VIDALYTICS_TAGS_TAB    = "Vidalytics Tags";
const VIDALYTICS_CONTENT_TAB = "Vidalytics Content";
const VIDALYTICS_CONFIG_TAB  = "Vidalytics Config";

const VIDALYTICS_HEADERS = [
  "Date",
  "Plays", "Impressions", "Unique Viewers", "Play Rate (%)", "Unmute Rate (%)",
  "Avg % Watched", "Avg Watch Duration (s)", "Conversions", "Conversion Rate (%)", "Bounce Rate (%)", "CTA Clicks",
];

const VIDALYTICS_TAGS_HEADERS = [
  "Date", "Tag",
  "Plays", "Impressions", "Unique Viewers", "Play Rate (%)", "Unmute Rate (%)",
  "Avg % Watched", "Conversions", "Conversion Rate (%)", "Bounce Rate (%)", "CTA Clicks",
];

const VIDALYTICS_CONTENT_HEADERS = [
  "Date", "UTM Content", "Plays",
];

// Calculate avg watch duration in seconds from drop-off data
function calcAvgWatchDuration(dropOffData, totalPlays) {
  if (!dropOffData || !totalPlays || totalPlays === 0) return 0;
  const seconds = Object.keys(dropOffData).map(Number).sort((a, b) => a - b);
  if (seconds.length === 0) return 0;
  let weightedSum = 0;
  for (let i = 0; i < seconds.length; i++) {
    const t       = seconds[i];
    const tNext   = seconds[i + 1];
    const viewers = dropOffData[String(t)] || 0;
    if (tNext !== undefined) {
      const dropped = viewers - (dropOffData[String(tNext)] || 0);
      if (dropped > 0) weightedSum += dropped * t;
    } else {
      weightedSum += viewers * t;
    }
  }
  return Math.round(weightedSum / totalPlays);
}

// Read active utm_content values from "Vidalytics Config" tab
async function readVidalyticsConfig(sheets) {
  const rows = await readTab(sheets, VIDALYTICS_CONFIG_TAB);
  if (rows.length < 2) return [];
  // Skip header row, return active entries
  return rows.slice(1)
    .filter(r => r[0] && r[1]?.toLowerCase() === "yes")
    .map(r => r[0].trim());
}

// Fetch timeline data filtered by url_param for a single utm_content value
// Returns: { "YYYY-MM-DD": { plays, impressions, ... } }
async function fetchVidalyticsContentTimeline(dateFrom, dateTo, utmContent, vidHeaders) {
  const url = `https://api.vidalytics.com/public/v1/stats/videos/timeline`
            + `?videoGuids=${VIDALYTICS_VIDEO_ID}&segment=segment.all`
            + `&dateFrom=${dateFrom}&dateTo=${dateTo}`
            + `&metrics=plays`
            + `&filter.url_params.utm_content=${encodeURIComponent(utmContent)}`;

  const res = await fetch(url, { headers: vidHeaders });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Vidalytics content timeline error ${res.status}: ${err}`);
  }
  const json = await res.json();
  const byDate = {};
  for (const item of json.content?.data || []) {
    for (const entry of item.data || []) {
      const d = entry.date?.split(" ")[0];
      if (!d) continue;
      if (!byDate[d]) byDate[d] = {};
      for (const vidEntry of entry.data || []) {
        Object.assign(byDate[d], vidEntry.metrics || {});
      }
    }
  }
  return byDate;
}


async function fetchVidalyticsTags(dateFrom, dateTo, vidHeaders) {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const base  = `https://api.vidalytics.com/public/v1/stats/videos/timeline`
              + `?videoGuids=${VIDALYTICS_VIDEO_ID}&segment=segment.tags`
              + `&dateFrom=${dateFrom}&dateTo=${dateTo}`;

  const BATCH1 = "plays,impressions,unique_viewers,play_rate,unmute_rate";
  const BATCH2 = "avg_watched,conversions,conversion_rate,bounce_rate,cta_clicks";

  async function fetchBatch(metrics) {
    const res = await fetch(`${base}&metrics=${metrics}`, { headers: vidHeaders });
    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Vidalytics tags error ${res.status}: ${err}`);
    }
    const json = await res.json();
    const byDate = {};
    for (const item of json.content?.data || []) {
      const tagName = item.segment;
      for (const entry of item.data || []) {
        const d = entry.date?.split(" ")[0];
        if (!d) continue;
        const mergedMetrics = {};
        for (const vidEntry of entry.data || []) {
          Object.assign(mergedMetrics, vidEntry.metrics || {});
        }
        if (!byDate[d]) byDate[d] = {};
        if (!byDate[d][tagName]) byDate[d][tagName] = {};
        Object.assign(byDate[d][tagName], mergedMetrics);
      }
    }
    return byDate;
  }

  const b1 = await fetchBatch(BATCH1);
  await sleep(3000);
  const b2 = await fetchBatch(BATCH2);

  const merged = {};
  const allDates = new Set([...Object.keys(b1), ...Object.keys(b2)]);
  for (const d of allDates) {
    merged[d] = {};
    const allTags = new Set([...Object.keys(b1[d] || {}), ...Object.keys(b2[d] || {})]);
    for (const tag of allTags) {
      merged[d][tag] = { ...(b1[d]?.[tag] || {}), ...(b2[d]?.[tag] || {}) };
    }
  }
  return merged;
}

async function syncVidalytics() {
  try {
    if (!VIDALYTICS_API_KEY || !VIDALYTICS_VIDEO_ID) {
      console.log("[Vidalytics sync] Skipping — VIDALYTICS_API_KEY or VIDALYTICS_VIDEO_ID not set");
      return;
    }

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });

    await ensureTab(sheets, VIDALYTICS_TAB);
    await ensureTab(sheets, VIDALYTICS_TAGS_TAB);
    await ensureTab(sheets, VIDALYTICS_CONTENT_TAB);

    // ── Ensure headers ──
    const existingOverall = await readTab(sheets, VIDALYTICS_TAB);
    if (!existingOverall.length || existingOverall[0].join(",") !== VIDALYTICS_HEADERS.join(",")) {
      await sheets.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: `${VIDALYTICS_TAB}!A:Z` });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${VIDALYTICS_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [VIDALYTICS_HEADERS] },
      });
    }

    const existingTags = await readTab(sheets, VIDALYTICS_TAGS_TAB);
    if (!existingTags.length || existingTags[0].join(",") !== VIDALYTICS_TAGS_HEADERS.join(",")) {
      await sheets.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: `${VIDALYTICS_TAGS_TAB}!A:Z` });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${VIDALYTICS_TAGS_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [VIDALYTICS_TAGS_HEADERS] },
      });
    }

    const existingContent = await readTab(sheets, VIDALYTICS_CONTENT_TAB);
    if (!existingContent.length || existingContent[0].join(",") !== VIDALYTICS_CONTENT_HEADERS.join(",")) {
      await sheets.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: `${VIDALYTICS_CONTENT_TAB}!A:Z` });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${VIDALYTICS_CONTENT_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [VIDALYTICS_CONTENT_HEADERS] },
      });
    }

    // Re-read after potential header rewrite
    const overallRows  = await readTab(sheets, VIDALYTICS_TAB);
    const tagsRows     = await readTab(sheets, VIDALYTICS_TAGS_TAB);
    const contentRows  = await readTab(sheets, VIDALYTICS_CONTENT_TAB);

    const existingOverallDates = new Set(overallRows.slice(1).map(r => r[0]).filter(Boolean));
    const existingTagKeys      = new Set(
      tagsRows.slice(1).map(r => r[0] && r[1] ? `${r[0]}__${r[1]}` : null).filter(Boolean)
    );
    const existingContentKeys  = new Set(
      contentRows.slice(1).map(r => r[0] && r[1] ? `${r[0]}__${r[1]}` : null).filter(Boolean)
    );

    // Read active utm_content values from config tab
    const activeContents = await readVidalyticsConfig(sheets);
    console.log(`[Vidalytics sync] Active UTM contents: ${activeContents.join(", ") || "none"}`);

    // Build date range from launch through yesterday
    const LAUNCH_DATE  = "2026-05-17";
    const yesterday    = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split("T")[0];

    const datesToFetch = [];
    const cursor = new Date(LAUNCH_DATE);
    const end    = new Date(yesterdayStr);
    while (cursor <= end) {
      const d = cursor.toISOString().split("T")[0];
      datesToFetch.push(d);
      cursor.setDate(cursor.getDate() + 1);
    }

    if (datesToFetch.length === 0) {
      console.log("[Vidalytics sync] No dates in range");
      return;
    }

    const vidHeaders = {
      "X-API-Key": VIDALYTICS_API_KEY,
      "Accept": "application/json",
    };

    const sleep          = ms => new Promise(r => setTimeout(r, ms));
    const newOverallRows = [];
    const newTagRows     = [];
    const newContentRows = [];
    const round2         = v => v != null ? Math.round(v * 100) / 100 : 0;

    // ── Overall tab: original working approach ──
    for (const dateStr of datesToFetch) {
      if (existingOverallDates.has(dateStr)) continue;

      const statsUrl = `https://api.vidalytics.com/public/v1/stats/video/${VIDALYTICS_VIDEO_ID}?dateFrom=${dateStr}&dateTo=${dateStr}`;
      const statsRes = await fetch(statsUrl, { headers: vidHeaders });
      if (!statsRes.ok) {
        console.warn(`[Vidalytics] Stats fetch failed for ${dateStr}: ${statsRes.status}`);
        await sleep(2000);
        continue;
      }
      const statsData = await statsRes.json();
      const s = statsData.content || {};

      const plays       = s.plays       ?? 0;
      const playsUnique = s.playsUnique ?? 0;
      const playRate    = round2(s.playRate);
      const unmuteRate  = round2(s.unmuteRate);

      let avgWatchDuration = 0;
      if (plays > 0) {
        await sleep(2000);
        const dropUrl = `https://api.vidalytics.com/public/v1/stats/video/${VIDALYTICS_VIDEO_ID}/drop-off?dateFrom=${dateStr}&dateTo=${dateStr}`;
        const dropRes = await fetch(dropUrl, { headers: vidHeaders });
        if (dropRes.ok) {
          const dropData = await dropRes.json();
          const watches  = dropData.content?.all?.watches || {};
          avgWatchDuration = calcAvgWatchDuration(watches, plays);
        }
      }

      await sleep(2000);
      let impressions = 0, avgWatched = 0, conversions = 0, convRate = 0, bounceRate = 0, ctaClicks = 0;
      try {
        const tlUrl = `https://api.vidalytics.com/public/v1/stats/videos/timeline`
                    + `?videoGuids=${VIDALYTICS_VIDEO_ID}&segment=segment.all`
                    + `&dateFrom=${dateStr}&dateTo=${dateStr}`
                    + `&metrics=impressions,avg_watched,conversions,conversion_rate,bounce_rate,cta_clicks`;
        const tlRes = await fetch(tlUrl, { headers: vidHeaders });
        if (tlRes.ok) {
          const tlData = await tlRes.json();
          for (const item of tlData.content?.data || []) {
            for (const entry of item.data || []) {
              for (const vidEntry of entry.data || []) {
                const m = vidEntry.metrics || {};
                impressions = m.impressions     ?? impressions;
                avgWatched  = m.avg_watched     ?? avgWatched;
                conversions = m.conversions     ?? conversions;
                convRate    = m.conversion_rate ?? convRate;
                bounceRate  = m.bounce_rate     ?? bounceRate;
                ctaClicks   = m.cta_clicks      ?? ctaClicks;
              }
            }
          }
        }
      } catch(e) {
        console.warn(`[Vidalytics] Timeline fetch failed for ${dateStr}: ${e.message}`);
      }

      newOverallRows.push([
        dateStr, plays, impressions, playsUnique, playRate, unmuteRate,
        round2(avgWatched), avgWatchDuration, conversions, round2(convRate), round2(bounceRate), ctaClicks,
      ]);
      console.log(`[Vidalytics] ${dateStr} plays=${plays} impressions=${impressions} avgWatched=${round2(avgWatched)}% avgDuration=${avgWatchDuration}s`);
      await sleep(2000);
    }

    // ── Tags tab ──
    const chunks = [];
    let chunkStart = 0;
    while (chunkStart < datesToFetch.length) {
      const from  = datesToFetch[chunkStart];
      const limit = new Date(from);
      limit.setMonth(limit.getMonth() + 1);
      limit.setDate(limit.getDate() - 1);
      const limitStr   = limit.toISOString().split("T")[0];
      const chunkDates = datesToFetch.slice(chunkStart).filter(d => d <= limitStr);
      chunks.push({ from, to: chunkDates[chunkDates.length - 1], dates: chunkDates });
      chunkStart += chunkDates.length;
    }

    for (const chunk of chunks) {
      const tagsByDate = await fetchVidalyticsTags(chunk.from, chunk.to, vidHeaders);
      await sleep(5000);

      for (const dateStr of chunk.dates) {
        const tagData = tagsByDate[dateStr] || {};
        for (const [tag, m] of Object.entries(tagData)) {
          if (tag === "Non-Tagged" || tag === "All" || tag === "all") continue;
          const key = `${dateStr}__${tag}`;
          if (!existingTagKeys.has(key)) {
            newTagRows.push([
              dateStr, tag,
              m.plays            ?? 0,
              m.impressions      ?? 0,
              m.unique_viewers   ?? 0,
              round2(m.play_rate),
              round2(m.unmute_rate),
              round2(m.avg_watched),
              m.conversions      ?? 0,
              round2(m.conversion_rate),
              round2(m.bounce_rate),
              m.cta_clicks       ?? 0,
            ]);
            existingTagKeys.add(key);
          }
        }
      }
    }

    // ── Content tab: filter by utm_content per active entry ──
    if (activeContents.length > 0) {
      for (const utmContent of activeContents) {
        console.log(`[Vidalytics] Fetching content filter: ${utmContent}`);

        for (const chunk of chunks) {
          const byDate = await fetchVidalyticsContentTimeline(chunk.from, chunk.to, utmContent, vidHeaders);
          await sleep(5000);

          for (const dateStr of chunk.dates) {
            const key = `${dateStr}__${utmContent}`;
            if (existingContentKeys.has(key)) continue;

            const m = byDate[dateStr] || {};
            newContentRows.push([
              dateStr, utmContent, m.plays ?? 0,
            ]);
            existingContentKeys.add(key);
          }
        }

        await sleep(3000);
      }
    } else {
      console.log("[Vidalytics sync] No active UTM contents in config tab — skipping content sync");
    }

    // ── Write all new rows ──
    if (newOverallRows.length) {
      await appendRows(sheets, VIDALYTICS_TAB, newOverallRows);
      console.log(`[Vidalytics sync] Overall: appended ${newOverallRows.length} row(s)`);
    } else {
      console.log(`[Vidalytics sync] Overall: nothing new`);
    }

    if (newTagRows.length) {
      await appendRows(sheets, VIDALYTICS_TAGS_TAB, newTagRows);
      console.log(`[Vidalytics sync] Tags: appended ${newTagRows.length} row(s)`);
    } else {
      console.log(`[Vidalytics sync] Tags: nothing new`);
    }

    if (newContentRows.length) {
      await appendRows(sheets, VIDALYTICS_CONTENT_TAB, newContentRows);
      console.log(`[Vidalytics sync] Content: appended ${newContentRows.length} row(s)`);
    } else {
      console.log(`[Vidalytics sync] Content: nothing new`);
    }

  } catch (err) {
    console.error("[Vidalytics sync error]", err.message);
  }
}

// ── HUBSPOT WEBHOOK ───────────────────────────────────────────────────────────
app.post("/webhook/hubspot", async (req, res) => {
  res.sendStatus(200);

  try {
    const events = Array.isArray(req.body) ? req.body : [req.body];
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, STAGE_CHANGE_TAB);

    const rows = [];

    for (const event of events) {
      const objectId    = String(event.objectId || event.id || "");
      const propertyName = event.propertyName || event.property || "";

      if (!propertyName.includes("pipeline_stage") && propertyName !== "hs_pipeline_stage") continue;

      const newStageId = event.propertyValue || event.value || "";
      const oldStageId = event.previousPropertyValue || event.previousValue || lastKnownStage[objectId] || "";

      const now   = new Date().toISOString();
      const month = new Date().toLocaleString("default", { month: "long", year: "numeric" });

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

      lastKnownStage[objectId] = newStageId;
    }

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

app.get("/sync-lp", async (req, res) => {
  syncLandingPageAnalytics();
  res.json({ success: true, message: "Landing page analytics sync started" });
});

app.get("/sync-vidalytics", async (req, res) => {
  syncVidalytics();
  res.json({ success: true, message: "Vidalytics sync started in background" });
});

app.get("/health", (_, res) => res.json({ status: "ok" }));

// ── SCHEDULES ─────────────────────────────────────────────────────────────────
setTimeout(() => {
  syncInProgress();
  syncHubSpotProjects();
  syncOperationsTickets();
  syncWebinar();
  syncWebinarForm();
  syncLandingPageAnalytics();
}, 10_000);

// Vidalytics delayed separately to avoid rate limit conflicts on startup
setTimeout(() => {
  syncVidalytics();
}, 60_000);

setInterval(syncInProgress,           60 * 60 * 1000); // every hour
setInterval(syncHubSpotProjects,      60 * 60 * 1000); // every hour
setInterval(syncOperationsTickets,    60 * 60 * 1000); // every hour
setInterval(syncWebinar,              60 * 60 * 1000); // every hour
setInterval(syncWebinarForm,          60 * 60 * 1000); // every hour
setInterval(syncLandingPageAnalytics, 60 * 60 * 1000); // every hour
setInterval(syncVidalytics,           24 * 60 * 60 * 1000); // every 24 hours (daily)

app.listen(PORT, () => console.log(`Webhook server running on port ${PORT}`));