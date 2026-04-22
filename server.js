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
    id: "139663aa-09ee-418e-b67d-c8cfcd3e5ce3",
    label: "Advising Dept Projects",
    stages: [
      { id: "5177858273", label: "Suggestions" },
      { id: "acc364b5-d367-49f4-a957-cc4fbf7e8e4b", label: "To Do" },
      { id: "6480a063-31a5-4beb-bce8-12e2edb48f83", label: "Doing" },
      { id: "d742ec1d-2e4d-4e7f-81e7-d33314c0074e", label: "Review" },
      { id: "f70c1fd3-a302-4ee2-be4f-33dc703631e7", label: "Completed" },
      { id: "7cb8b001-0085-44a7-9ff1-9540614e98f0", label: "Blocked" },
      { id: "5174323426", label: "Backlog" },
    ],
  },
  {
    id: "3719938274",
    label: "Servicing Dept Projects",
    stages: [
      { id: "5177866467", label: "Suggestions" },
      { id: "5177866468", label: "To Do" },
      { id: "5177866469", label: "Doing" },
      { id: "5177866470", label: "Review" },
      { id: "5177866471", label: "Completed" },
      { id: "5177866472", label: "Blocked" },
      { id: "5177866473", label: "Backlog" },
    ],
  },
  {
    id: "3078420704",
    label: "Plan Proposal Onboarding",
    stages: [
      { id: "4214753501", label: "Getting Started" },
      { id: "4214753503", label: "Uploading Documents" },
      { id: "4214753504", label: "Completing Expense Worksheet" },
      { id: "4214753502", label: "Linking Your Accounts" },
      { id: "4214753505", label: "Next Steps" },
      { id: "4214753507", label: "Onboarding Complete" },
    ],
  },
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
      // archived/old stages kept for historical lookups
      { id: "4214753508", label: "OLD Stage 2 (archived)" },
      { id: "4805782765", label: "OLD Stage 4 (archived)" },
      { id: "4225526006", label: "OLD Stage 5 (archived)" },
    ],
  },
  {
    id: "3708472569",
    label: "Transition to Ongoing Relationship",
    stages: [
      { id: "5147872474", label: "Stage 1: Initiation & Client Agreement" },
      { id: "5147872475", label: "Stage 2: Statements, Account Opening & Transfers" },
      { id: "5147872476", label: "Stage 3: Transfers Settling" },
      { id: "5147872477", label: "Stage 4: Software Updates & Portfolio Prep" },
      { id: "5147872478", label: "Stage 5: Kickoff Ready" },
      { id: "5147872479", label: "Stage 6: Post-Kickoff Execution" },
    ],
  },
  {
    id: "3612744890",
    label: "Employee Onboarding Pipeline",
    stages: [
      { id: "4964938987", label: "New Hire Administration" },
      { id: "4964938988", label: "Licenses" },
      { id: "4964938989", label: "Equipment" },
      { id: "4964938990", label: "Pre-Orientation Prep" },
      { id: "4965263590", label: "Orientation" },
      { id: "4965263591", label: "New Hire Onboarding" },
      { id: "5036707027", label: "Hubspot Onboarding" },
      { id: "4965263598", label: "GDrive + Sales Message" },
      { id: "4965263592", label: "Compliance" },
      { id: "4965263593", label: "Marketing/Online Presence" },
      { id: "4965263594", label: "About PeakFP" },
      { id: "4965263599", label: "Roles Overview" },
      { id: "4965263595", label: "The Prospect Experience" },
      { id: "4965263596", label: "The Project Plan Client Experience" },
      { id: "4965263597", label: "The Ongoing Client Experience" },
      { id: "5037349111", label: "New Client Onboarding" },
      { id: "5085859056", label: "Baseline Financial Plan Build Training" },
      { id: "5085836532", label: "QA + Scenarios (Pre-Plan Proposal)" },
      { id: "5085859057", label: "Tax Efficient Withdrawal (TEW) Worksheet, Optimizations, Investments 1" },
      { id: "5085952220", label: "Transition to Ongoing Relationship" },
      { id: "5085859058", label: "Ongoing Client Servicing Meetings + Cadence" },
    ],
  },
  {
    id: "3338260692",
    label: "Amrit To Do",
    stages: [
      { id: "4572506361", label: "To Do" },
      { id: "4572507322", label: "Doing" },
      { id: "4572507323", label: "Review" },
      { id: "4572507324", label: "Completed" },
      { id: "4572507325", label: "Blocked" },
      { id: "4572507326", label: "Backlog" },
    ],
  },
  {
    id: "3364165843",
    label: "Operations Tickets",
    stages: [
      { id: "4609840367", label: "To Do" },
      { id: "4609840368", label: "Doing" },
      { id: "4609840369", label: "Review" },
      { id: "4609840370", label: "Completed" },
      { id: "4609840371", label: "Blocked" },
      { id: "4609840372", label: "Backlog" },
    ],
  },
  {
    id: "3707748541",
    label: "Planning Dept Projects",
    stages: [
      { id: "5176451267", label: "Suggestions" },
      { id: "5161814255", label: "To Do" },
      { id: "5161814256", label: "Doing" },
      { id: "5161814257", label: "Review" },
      { id: "5161814258", label: "Completed" },
      { id: "5161814259", label: "Blocked" },
      { id: "5161814260", label: "Backlog" },
    ],
  },
  {
    id: "3708463303",
    label: "Leadership Projects",
    stages: [
      { id: "5177862346", label: "Suggestions" },
      { id: "5148141796", label: "To Do" },
      { id: "5148141797", label: "Doing" },
      { id: "5148141798", label: "Review" },
      { id: "5148141799", label: "Completed" },
      { id: "5148141800", label: "Blocked" },
      { id: "5148141801", label: "Backlog" },
    ],
  },
  {
    id: "3353726189",
    label: "Mosby To Do",
    stages: [
      { id: "4594572486", label: "To Do" },
      { id: "4594572487", label: "Doing" },
      { id: "4594572488", label: "Review" },
      { id: "4594572489", label: "Completed" },
      { id: "4594572490", label: "Backlog" },
      { id: "4594572491", label: "Blocked" },
    ],
  },
  {
    id: "3254971611",
    label: "Chris - To Do",
    stages: [
      { id: "4456657143", label: "Backlog" },
      { id: "4456657144", label: "Checkbox Exercise" },
      { id: "4456657145", label: "CFIQ - This Week" },
      { id: "4456658106", label: "Peak - This Week" },
      { id: "4579452136", label: "Done This Week" },
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
const FILLOUT_LOG_TAB     = "Fillout Log";
const FILLOUT_LOG_HEADERS = [
  "Timestamp", "Month", "Form Name", "Status", "Submission ID",
  "First Name", "Last Name", "Email", "Phone", "Raw Questions JSON",
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
      return q.value || "";
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
      now, month, sub.formName || "", sub.status || "",
      sub.submissionId || "",
      extractFilloutField(q, "first name", "firstname"),
      extractFilloutField(q, "last name", "lastname"),
      extractFilloutField(q, "email"),
      extractFilloutField(q, "phone"),
      JSON.stringify(q),
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

    let offset = 0;
    const limit = 150;
    let total   = Infinity;
    const all   = [];

    while (offset < total) {
      const url = `https://api.fillout.com/v1/api/forms/${FILLOUT_FORM_ID}/submissions?limit=${limit}&offset=${offset}&status=in_progress`;
      const res = await fetch(url, { headers: { Authorization: `Bearer ${FILLOUT_API_KEY}` } });
      const data = await res.json();
      total = data.totalResponses ?? 0;
      for (const sub of data.responses || []) {
        all.push({
          formId: FILLOUT_FORM_ID, formName: "Fillout Form",
          status: "In Progress", submissionId: sub.submissionId,
          timestamp: sub.submittedAt, questions: sub.questions || [],
        });
      }
      offset += limit;
      if (!(data.responses?.length)) break;
    }
    if (all.length) await batchLogSubmissions(sheets, all);
    console.log(`[Fillout sync] ${all.length} in-progress submissions synced`);
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

async function syncHubSpotProjects() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, PROJECTS_TAB);
    await ensureTab(sheets, STAGE_CHANGE_TAB);

    const projects = await fetchAllProjects();
    console.log(`[Projects sync] Fetched ${projects.length} projects`);

    const now   = new Date().toISOString();
    const month = new Date().toLocaleString("default", { month: "long", year: "numeric" });

    // Build rows for HubSpot Projects tab
    const projectRows = [PROJECTS_HEADERS];
    const stageChanges = [];

    for (const p of projects) {
      const props     = p.properties || {};
      const id        = p.id;
      const name      = props.hs_name || "";
      const pipeline  = pipelineLabel(props.hs_pipeline);
      const stageId   = props.hs_pipeline_stage || "";
      const stage     = stageLabel(stageId);

      projectRows.push([
        id, name, pipeline, stage,
        props.fp_owner || "",
        props.wa_owner || "",
        props.hubspot_owner_id || "",
        props.card_due_date_ || "",
        props.hs_target_due_date || "",
        props.hs_lastmodifieddate || "",
        props.createdate || "",
      ]);

      // Stage change detection
      if (lastKnownStage[id] !== undefined && lastKnownStage[id] !== stageId) {
        stageChanges.push([
          now, month, id, name,
          pipeline,
          stageLabel(lastKnownStage[id]),
          stage,
          "Hourly Sync",
        ]);
        console.log(`[Stage change] ${name}: ${stageLabel(lastKnownStage[id])} → ${stage}`);
      }
      lastKnownStage[id] = stageId;
    }

    // Write projects tab (full refresh)
    await writeTab(sheets, PROJECTS_TAB, projectRows);
    console.log(`[Projects sync] Wrote ${projects.length} rows to "${PROJECTS_TAB}"`);

    // Ensure Stage Change Log headers exist
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
      console.log(`[Stage Change Log] Logged ${stageChanges.length} changes`);
    }
  } catch (err) {
    console.error("[Projects sync error]", err.message);
  }
}

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
  try {
    const event        = req.body;
    const eventType    = event.eventType || "submission.completed";
    const formId       = event.formId || event.form_id || FILLOUT_FORM_ID || "unknown-form";
    const formName     = event.formName || event.form_name || formId;
    const submissionId = event.submissionId || event.submission_id || "";
    const questions    = event.questions || event.data?.questions || [];

    const status = (eventType === "submission.partial" || eventType === "submission.in_progress")
      ? "In Progress"
      : "Completed";

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, FILLOUT_LOG_TAB);
    await ensureFilloutHeaders(sheets);
    await batchLogSubmissions(sheets, [{
      formId, formName, status, submissionId,
      timestamp: new Date().toISOString(), questions,
    }]);

    console.log(`[Fillout webhook] ${status} | ${formName}`);
    res.json({ success: true, status, formName });
  } catch (err) {
    console.error("[Fillout webhook error]", err.message);
    res.status(500).json({ error: err.message });
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