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
const VIDALYTICS_VIDEO_ID_2 = process.env.VIDALYTICS_VIDEO_ID_2;

const LP_TAB     = "HS Page Visits";
const LP_PAGE_ID = "403572489414";
const LP_HEADERS = [
  "Date", "Page Views", "New Visitors", "Entrances", "Exits",
  "Bounce Rate", "Avg Time on Page (s)", "Submissions", "Contacts", "Customers",
];

// Known webinar UTM content values to track
const LT_UTM_FILTERS  = [
  "3.13_m_all_pretax",
  "webinar_title",
  "more_than_2m_to_retire",
  "retiring_married",
];

const MEETINGS_TAB     = "Meetings";
const MEETINGS_HEADERS = [
  "Date", "Total Booked", "Completed", "No Show", "Canceled", "Rescheduled", "Other/Unknown", "No Show Rate",
];

const MEETING_OWNERS = [
  { id: "30558812", name: "Sean Stevenson"  },
  { id: "30556626", name: "Joseph Perez"    },
  { id: "83394428", name: "Amrit Khatkar"   },
  { id: "31931535", name: "Adam Haynes"     },
  { id: "32930333", name: "Cody Emerson"    },
  { id: "33160574", name: "Edward Strube"   },
];
const MEETING_HEADERS = ["Date", ...MEETING_OWNERS.map(o => o.name), "Total"];

const INV2_TAB     = "Investments 2 Meetings";
const INV2_OWNERS  = MEETING_OWNERS;
const INV2_HEADERS = MEETING_HEADERS;

const INV1_TAB     = "Investments 1 Meetings";
const INV1_HEADERS = MEETING_HEADERS;

const PKO_TAB      = "Partnership Kickoff Meetings";
const PKO_HEADERS  = MEETING_HEADERS;

const UTM_CONTENT_TAB     = "Page views/UTM content";
const UTM_CONFIG_TAB      = "UTM Config";
const UTM_CONTENT_HEADERS = [
  "Date", "UTM Content", "Sessions", "Page Views", "Pages/Session",
  "Bounce Rate", "Avg Time on Page", "New Visitors", "Contacts", "Customers",
];
// Fallback hardcoded list — used only if UTM Config tab is empty
const UTM_CONTENT_FILTERS = [
  "3.13_m_all_pretax",
  "webinar_title",
  "more_than_2m_to_retire",
  "retiring_married",
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

const OPS_V2_TAB       = "Operations Tickets (New)";
const OPS_V2_START     = "2026-06-08"; // Only show tickets created/modified on or after this date
const OPS_V2_HEADERS   = [
  "Project ID", "Project Name", "Pipeline", "Stage",
  "Owner", "Created Date", "Last Modified", "Due Date",
  "Date Entered Suggestions", "Date Entered To Do", "Date Entered Doing",
  "Date Entered Ops Review", "Date Entered Dept. Review", "Date Entered Completed",
  "Date Entered Push For Rollout", "Date Entered Bug Fixed",
  "Date Entered Blocked", "Date Entered Backlog",
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
    // Suppress harmless "already exists" race condition errors
    if (!err.message?.includes('already exists')) {
      console.error(`[ensureTab error] ${tabName}:`, err.message);
    }
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
  "(OPTIONAL) Please share any additional information related to your goals or pain points you think would be helpful", "Meeting Date"
];

const WEBINAR_TAB     = "Webinar";
const WEBINAR_HEADERS = [
  "Timestamp", "Form Name", "Form ID", "Status", "Submission ID", "Month",
  "First Name", "Last Name", "Email", "Mobile Phone Number",
  "utm_source", "utm_content",
  "Will the retirement planning be just yourself or include a spouse/partner",
  "About how much have you saved for retirement?",
  "Are you retired, looking to retire in the next 5 years, or looking to retire in the next 10 years?", "Meeting Date"
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
      meeting?.eventStartTime || "",
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
      (() => { const s = sub.scheduling || []; return s[0]?.value?.eventStartTime || ""; })(),
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

    const projects   = await fetchAllProjects();
    console.log(`[Projects sync] Fetched ${projects.length} projects`);

    const projectRows  = [PROJECTS_HEADERS];

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

    }

    await writeTab(sheets, PROJECTS_TAB, projectRows);
    console.log(`[Projects sync] Wrote ${projectRows.length - 1} rows to "${PROJECTS_TAB}"`);



  } catch (err) {
    console.error("[Projects sync error]", err.message);
  }
}

async function fetchAllOpsV2Tickets() {
  const properties = [
    "hs_name", "hs_pipeline", "hs_pipeline_stage",
    "hubspot_owner_id", "createdate", "hs_lastmodifieddate",
    "hs_due_date",
    "hs_date_entered_5272068333",
    "hs_date_entered_4609840367",
    "hs_date_entered_4609840368",
    "hs_date_entered_4609840369",
    "hs_date_entered_5471594709",
    "hs_date_entered_4609840370",
    "hs_date_entered_5470463189",
    "hs_date_entered_5471604984",
    "hs_date_entered_4609840371",
    "hs_date_entered_4609840372",
  ];

  const tickets = [];
  let after = null;

  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: "hs_pipeline", operator: "EQ", value: "3364165843" }] }],
      properties,
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
      throw new Error(`HubSpot Ops v2 fetch error ${res.status}: ${err}`);
    }

    const data = await res.json();
    tickets.push(...(data.results || []));
    after = data.paging?.next?.after || null;
    if (!after) break;
  }

  return tickets;
}

async function syncOperationsTicketsV2() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, OPS_V2_TAB);

    const tickets = await fetchAllOpsV2Tickets();
    console.log(`[Ops v2 sync] Fetched ${tickets.length} tickets`);

    const cutoff = new Date(OPS_V2_START).getTime();
    const rows   = [OPS_V2_HEADERS];

    for (const t of tickets) {
      const props = t.properties || {};
      // Only include tickets created on or after June 8
      const created = props.createdate ? new Date(props.createdate).getTime() : 0;
      if (created < cutoff) continue;

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
        msToDate(props.hs_date_entered_5471594709),
        msToDate(props.hs_date_entered_4609840370),
        msToDate(props.hs_date_entered_5470463189),
        msToDate(props.hs_date_entered_5471604984),
        msToDate(props.hs_date_entered_4609840371),
        msToDate(props.hs_date_entered_4609840372),
      ]);
    }

    await writeTab(sheets, OPS_V2_TAB, rows);
    console.log(`[Ops v2 sync] Wrote ${rows.length - 1} rows to "${OPS_V2_TAB}"`);

  } catch (err) {
    console.error("[Ops v2 sync error]", err.message);
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

    // Read all existing rows so we can upsert (update zeros, append new dates)
    const existing = await readTab(sheets, LP_TAB);
    if (!existing.length || existing[0][0] !== "Date") {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${LP_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [LP_HEADERS] },
      });
    }

    // Build a map of date -> row index (1-based, excluding header)
    const dateToRowIndex = {};
    for (let i = 1; i < existing.length; i++) {
      if (existing[i][0]) dateToRowIndex[existing[i][0]] = i + 1; // +1 for 1-based sheet row
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
    const toAppend  = [];
    let   updated   = 0;

    for (const [date, values] of Object.entries(data)) {
      const d = values[0] || {};
      const newRow = [
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
      ];

      if (dateToRowIndex[date] !== undefined) {
        // Date already exists — overwrite it so zero rows get corrected
        const rowNum = dateToRowIndex[date];
        await sheets.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: `${LP_TAB}!A${rowNum}`,
          valueInputOption: "RAW",
          requestBody: { values: [newRow] },
        });
        updated++;
      } else {
        // Brand new date — queue for append
        toAppend.push(newRow);
      }
    }

    toAppend.sort((a, b) => a[0].localeCompare(b[0]));
    if (toAppend.length) {
      await appendRows(sheets, LP_TAB, toAppend);
    }

    console.log(`[LP Analytics] Updated ${updated} existing rows, appended ${toAppend.length} new days`);

  } catch (err) {
    console.error("[LP Analytics error]", err.message);
  }
}

const LT_PAGE_ID      = "405603080417";
const LT_PV_TAB       = "LT Page Visits";
const LT_PV_HEADERS   = [
  "Date", "Page Views", "New Visitors", "Entrances", "Exits",
  "Bounce Rate", "Avg Time on Page (s)",
];

async function syncLiveTrainingPage() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });

    // ── Page Visits ───────────────────────────────────────────────────────────
    await ensureTab(sheets, LT_PV_TAB);

    const existing = await readTab(sheets, LT_PV_TAB);
    if (!existing.length || existing[0][0] !== "Date") {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${LT_PV_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [LT_PV_HEADERS] },
      });
    }
    const pvDateToRow = {};
    for (let i = 1; i < existing.length; i++) {
      if (existing[i][0]) pvDateToRow[existing[i][0]] = i + 1;
    }

    const end   = new Date();
    const start = new Date();
    start.setDate(start.getDate() - 30);
    const fmt = d => d.toISOString().split("T")[0].replace(/-/g, "");

    const pvUrl = `https://api.hubapi.com/analytics/v2/reports/landing-pages/summarize/daily`
                + `?start=${fmt(start)}&end=${fmt(end)}&f=${LT_PAGE_ID}`;

    const pvRes = await fetch(pvUrl, {
      headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}` },
    });
    if (!pvRes.ok) {
      const errText = await pvRes.text();
      throw new Error(`HubSpot LT page visits error ${pvRes.status}: ${errText}`);
    }
    const pvData  = await pvRes.json();
    const pvBatch = [];
    const pvAppend = [];

    for (const [date, values] of Object.entries(pvData)) {
      const d = values[0] || {};
      const row = [
        date,
        d.rawViews || 0,
        d.newVisitorRawViews || 0,
        d.entrances || 0,
        d.exits || 0,
        d.pageBounceRate ? Math.round(d.pageBounceRate * 100) + "%" : "0%",
        d.timePerPageview ? Math.round(d.timePerPageview) : 0,
      ];
      if (pvDateToRow[date] !== undefined) {
        pvBatch.push({ range: `${LT_PV_TAB}!A${pvDateToRow[date]}`, values: [row] });
      } else {
        pvAppend.push(row);
      }
    }

    if (pvBatch.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { valueInputOption: "RAW", data: pvBatch },
      });
    }
    pvAppend.sort((a, b) => a[0].localeCompare(b[0]));
    if (pvAppend.length) {
      await appendRows(sheets, LT_PV_TAB, pvAppend);
    }
    console.log(`[LT Page Visits] Updated ${pvBatch.length} rows, appended ${pvAppend.length} new days`);



  } catch (err) {
    console.error("[LT Analytics error]", err.message);
  }
}

// ── VIDALYTICS SYNC ───────────────────────────────────────────────────────────
const VIDALYTICS_TAB         = "Vidalytics";
const VIDALYTICS_CONTENT_TAB = "Vidalytics Content";
const VIDALYTICS_CONFIG_TAB  = "Vidalytics Config";

const VIDALYTICS_HEADERS = [
  "Date", "Video",
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

// Read active UTM content values from "UTM Config" tab
// Tab columns: UTM Content | Active (yes/no)
async function readUTMConfig(sheets) {
  await ensureTab(sheets, UTM_CONFIG_TAB);
  const rows = await readTab(sheets, UTM_CONFIG_TAB);

  // If tab is empty or only has header, seed it with the hardcoded defaults
  if (rows.length < 2) {
    const headerRow  = ["UTM Content", "Active"];
    const defaultRows = UTM_CONTENT_FILTERS.map(v => [v, "yes"]);
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${UTM_CONFIG_TAB}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [headerRow, ...defaultRows] },
    });
    return UTM_CONTENT_FILTERS;
  }

  // Skip header, return values where Active = "yes"
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

    // Re-read after potential header rewrite
    const overallRows  = await readTab(sheets, VIDALYTICS_TAB);

    // Key on date+video label to support multiple videos per date
    const existingOverallDates = new Set(overallRows.slice(1).map(r => r[0] && r[1] ? `${r[0]}__${r[1]}` : null).filter(Boolean));


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
    const round2         = v => v != null ? Math.round(v * 100) / 100 : 0;

    // ── Overall tab: loop over both videos ──
    const videoTargets = [
      { id: VIDALYTICS_VIDEO_ID,   label: "Original" },
      ...(VIDALYTICS_VIDEO_ID_2 ? [{ id: VIDALYTICS_VIDEO_ID_2, label: "Variant" }] : []),
    ];

    for (const { id: videoId, label: videoLabel } of videoTargets) {
      for (const dateStr of datesToFetch) {
        // Check if this date+video combo already exists
        const rowKey = `${dateStr}__${videoLabel}`;
        if (existingOverallDates.has(rowKey)) continue;

        const statsUrl = `https://api.vidalytics.com/public/v1/stats/video/${videoId}?dateFrom=${dateStr}&dateTo=${dateStr}`;
        const statsRes = await fetch(statsUrl, { headers: vidHeaders });
        if (!statsRes.ok) {
          console.warn(`[Vidalytics] Stats fetch failed for ${videoLabel} ${dateStr}: ${statsRes.status}`);
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
          const dropUrl = `https://api.vidalytics.com/public/v1/stats/video/${videoId}/drop-off?dateFrom=${dateStr}&dateTo=${dateStr}`;
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
          // Fetch one metric at a time — non-Enterprise only allows 1 metric per request
          const extraMetrics = [
            ["impressions",     (v) => { impressions = v; }],
            ["avg_watched",     (v) => { avgWatched  = v; }],
            ["conversions",     (v) => { conversions = v; }],
            ["conversion_rate", (v) => { convRate    = v; }],
            ["bounce_rate",     (v) => { bounceRate  = v; }],
            ["cta_clicks",      (v) => { ctaClicks   = v; }],
          ];
          for (const [metric, assign] of extraMetrics) {
            const tlUrl = `https://api.vidalytics.com/public/v1/stats/videos/timeline`
                        + `?videoGuids=${videoId}&segment=segment.all`
                        + `&dateFrom=${dateStr}&dateTo=${dateStr}&metrics=${metric}`;
            const tlRes = await fetch(tlUrl, { headers: vidHeaders });
            if (tlRes.ok) {
              const tlData = await tlRes.json();
              for (const item of tlData.content?.data || []) {
                for (const entry of item.data || []) {
                  for (const vidEntry of entry.data || []) {
                    const val = vidEntry.metrics?.[metric];
                    if (val != null) assign(val);
                  }
                }
              }
            }
            await sleep(1500);
          }
        } catch(e) {
          console.warn(`[Vidalytics] Extra metrics fetch failed for ${videoLabel} ${dateStr}: ${e.message}`);
        }

        newOverallRows.push([
          dateStr, videoLabel, plays, impressions, playsUnique, playRate, unmuteRate,
          round2(avgWatched), avgWatchDuration, conversions, round2(convRate), round2(bounceRate), ctaClicks,
        ]);
        existingOverallDates.add(rowKey); // prevent duplicates within same sync run
        console.log(`[Vidalytics] ${videoLabel} ${dateStr} plays=${plays} impressions=${impressions} avgWatched=${round2(avgWatched)}% avgDuration=${avgWatchDuration}s`);
        await sleep(2000);
      }
    }

    // ── Write all new rows ──
    if (newOverallRows.length) {
      await appendRows(sheets, VIDALYTICS_TAB, newOverallRows);
      console.log(`[Vidalytics sync] Overall: appended ${newOverallRows.length} row(s)`);
    } else {
      console.log(`[Vidalytics sync] Overall: nothing new`);
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
    // Stage change logging removed
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
async function syncUTMContent() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, UTM_CONTENT_TAB);

    // Read existing rows to build upsert map keyed by "date__utmValue"
    const existing = await readTab(sheets, UTM_CONTENT_TAB);
    if (!existing.length || existing[0][0] !== "Date") {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${UTM_CONTENT_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [UTM_CONTENT_HEADERS] },
      });
    }
    // Map "date__utm" -> sheet row number (1-based)
    const keyToRowIndex = {};
    for (let i = 1; i < existing.length; i++) {
      const row = existing[i];
      if (row[0] && row[1]) keyToRowIndex[`${row[0]}__${row[1]}`] = i + 1;
    }

    const end   = new Date();
    const start = new Date();
    start.setDate(start.getDate() - 30);
    const fmt = d => d.toISOString().split("T")[0].replace(/-/g, "");

    const toAppend   = [];
    const utmBatch   = [];

    // Read active UTM values dynamically from the "UTM Config" sheet tab
    const activeUTMs = await readUTMConfig(sheets);
    console.log(`[UTM Content] Tracking ${activeUTMs.length} UTM values: ${activeUTMs.join(", ")}`);

    // Query each UTM value using the daily breakdown endpoint.
    // The /summarize/daily endpoint returns per-UTM rows per day.
    for (const utmValue of activeUTMs) {
      const url = `https://api.hubapi.com/analytics/v2/reports/utm-contents/summarize/daily`
                + `?start=${fmt(start)}&end=${fmt(end)}&f=${encodeURIComponent(utmValue)}`;

      const res = await fetch(url, {
        headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}` },
      });
      if (!res.ok) {
        console.warn(`[UTM Content] Failed for "${utmValue}": ${res.status} ${await res.text()}`);
        continue;
      }
      const data = await res.json();
      console.log(`[UTM Content] "${utmValue}" keys: ${Object.keys(data).slice(0,10).join(", ")}`);

      for (const [date, values] of Object.entries(data)) {
        // Skip aggregate keys like breakdowns, offset, total, totals
        // API returns dates as "2026-05-17" (YYYY-MM-DD) or "20260517" (YYYYMMDD)
        if (!/^\d{4}-\d{2}-\d{2}$/.test(date) && !/^\d{8}$/.test(date)) continue;
        const d = Array.isArray(values) ? (values[0] || {}) : (values || {});

        const row = [
          date,
          utmValue,
          d.visits             || 0,                          // Sessions
          d.rawViews           || 0,                          // Page Views
          d.pageviewsPerSession ? Math.round(d.pageviewsPerSession * 100) / 100 : 0, // Pages/Session
          d.bounceRate         ? Math.round(d.bounceRate * 100) + "%" : "0%",        // Bounce Rate
          d.timePerSession     ? Math.round(d.timePerSession) : 0,                   // Avg Time on Page
          d.visitors           || 0,                          // New Visitors (unique)
          d.contacts           || 0,                          // Contacts
          d.customers          || 0,                          // Customers
        ];

        const key = `${date}__${utmValue}`;
        if (keyToRowIndex[key] !== undefined) {
          utmBatch.push({ range: `${UTM_CONTENT_TAB}!A${keyToRowIndex[key]}`, values: [row] });
        } else {
          toAppend.push(row);
          keyToRowIndex[key] = -1;
        }
      }
    }

    if (utmBatch.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { valueInputOption: "RAW", data: utmBatch },
      });
    }

    toAppend.sort((a, b) => a[0].localeCompare(b[0]) || a[1].localeCompare(b[1]));
    if (toAppend.length) {
      await appendRows(sheets, UTM_CONTENT_TAB, toAppend);
    }

    console.log(`[UTM Content] Updated ${utmBatch.length} rows, appended ${toAppend.length} new rows`);

  } catch (err) {
    console.error("[UTM Content sync error]", err.message);
  }
}

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

app.get("/sync-ops-v2", async (req, res) => {
  syncOperationsTicketsV2();
  res.json({ success: true, message: "Operations Tickets v2 sync started in background" });
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

async function syncMeetings() {
  try {
    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureTab(sheets, MEETINGS_TAB);

    // Read existing rows to build upsert map (date -> sheet row number)
    const existing = await readTab(sheets, MEETINGS_TAB);
    if (!existing.length || existing[0][0] !== "Date") {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${MEETINGS_TAB}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [MEETINGS_HEADERS] },
      });
    }
    const dateToRowIndex = {};
    for (let i = 1; i < existing.length; i++) {
      if (existing[i][0]) dateToRowIndex[existing[i][0]] = i + 1; // 1-based sheet row
    }

    // Fetch all meetings from HubSpot (paginated)
    const allMeetings = [];
    let after = undefined;
    do {
      const body = {
        filterGroups: [],
        properties: ["hs_meeting_outcome", "hs_meeting_start_time", "hs_timestamp"],
        limit: 100,
        ...(after ? { after } : {}),
      };
      const res = await fetch("https://api.hubapi.com/crm/v3/objects/meetings/search", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${HUBSPOT_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const err = await res.text();
        throw new Error(`HubSpot meetings API error ${res.status}: ${err}`);
      }
      const data = await res.json();
      allMeetings.push(...(data.results || []));
      after = data.paging?.next?.after;
    } while (after);

    console.log(`[Meetings] Fetched ${allMeetings.length} total meetings`);

    // Group by date using start time, fallback to created timestamp
    const byDate = {};
    for (const m of allMeetings) {
      const ts = m.properties.hs_meeting_start_time || m.properties.hs_timestamp;
      if (!ts) continue;
      const date = new Date(ts).toISOString().split("T")[0];
      if (!byDate[date]) byDate[date] = { total: 0, completed: 0, noShow: 0, canceled: 0, rescheduled: 0, other: 0 };
      const outcome = (m.properties.hs_meeting_outcome || "").toUpperCase();
      byDate[date].total++;
      if      (outcome === "COMPLETED")   byDate[date].completed++;
      else if (outcome === "NO_SHOW")     byDate[date].noShow++;
      else if (outcome === "CANCELED")    byDate[date].canceled++;
      else if (outcome === "RESCHEDULED") byDate[date].rescheduled++;
      else                                byDate[date].other++;
    }

    const toAppend      = [];
    const meetingsBatch = [];

    for (const [date, counts] of Object.entries(byDate)) {
      const noShowRate = counts.total > 0
        ? Math.round((counts.noShow / counts.total) * 100) + "%"
        : "0%";
      const row = [
        date,
        counts.total,
        counts.completed,
        counts.noShow,
        counts.canceled,
        counts.rescheduled,
        counts.other,
        noShowRate,
      ];

      if (dateToRowIndex[date] !== undefined) {
        meetingsBatch.push({ range: `${MEETINGS_TAB}!A${dateToRowIndex[date]}`, values: [row] });
      } else {
        toAppend.push(row);
      }
    }

    if (meetingsBatch.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { valueInputOption: "RAW", data: meetingsBatch },
      });
    }

    toAppend.sort((a, b) => a[0].localeCompare(b[0]));
    if (toAppend.length) {
      await appendRows(sheets, MEETINGS_TAB, toAppend);
    }

    console.log(`[Meetings] Updated ${meetingsBatch.length} rows, appended ${toAppend.length} new days`);

  } catch (err) {
    console.error("[Meetings sync error]", err.message);
  }
}

app.get("/sync-lt", async (req, res) => {
  syncLiveTrainingPage();
  res.json({ success: true, message: "Live training page analytics sync started" });
});

app.get("/sync-utm", async (req, res) => {
  syncUTMContent();
  res.json({ success: true, message: "UTM content sync started" });
});

// Generic meeting sync — fetches meetings by title keyword, groups by date using the given timestamp field
async function syncMeetingsByType({ tab, headers, titleKeyword, excludeKeyword, timestampField }) {
  const auth   = getGoogleAuth();
  const sheets = google.sheets({ version: "v4", auth });
  await ensureTab(sheets, tab);

  const existing = await readTab(sheets, tab);
  if (!existing.length || existing[0][0] !== "Date") {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${tab}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [headers] },
    });
  }
  const dateToRowIndex = {};
  for (let i = 1; i < existing.length; i++) {
    if (existing[i][0]) dateToRowIndex[existing[i][0]] = i + 1;
  }

  const allMeetings = [];
  let after = undefined;
  do {
    // Build filters: must contain titleKeyword, optionally must NOT contain excludeKeyword
    const filters = [{ propertyName: "hs_meeting_title", operator: "CONTAINS_TOKEN", value: titleKeyword }];
    if (excludeKeyword) {
      filters.push({ propertyName: "hs_meeting_title", operator: "NOT_CONTAINS_TOKEN", value: excludeKeyword });
    }
    const body = {
      filterGroups: [{ filters }],
      properties: ["hs_meeting_title", "hs_meeting_start_time", "hs_createdate", "hs_timestamp", "hubspot_owner_id"],
      limit: 100,
      ...(after ? { after } : {}),
    };
    const res = await fetch("https://api.hubapi.com/crm/v3/objects/meetings/search", {
      method: "POST",
      headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`HubSpot meetings error ${res.status}: ${await res.text()}`);
    const data = await res.json();
    allMeetings.push(...(data.results || []));
    after = data.paging?.next?.after;
  } while (after);

  console.log(`[${tab}] Fetched ${allMeetings.length} meetings`);

  const byDate = {};
  for (const m of allMeetings) {
    const ts = m.properties[timestampField] || m.properties.hs_timestamp;
    if (!ts) continue;
    const date    = new Date(ts).toISOString().split("T")[0];
    const ownerId = m.properties.hubspot_owner_id || "unknown";
    if (!byDate[date]) byDate[date] = {};
    byDate[date][ownerId] = (byDate[date][ownerId] || 0) + 1;
  }

  const toAppend  = [];
  const batchData = [];

  for (const [date, ownerCounts] of Object.entries(byDate)) {
    const counts = MEETING_OWNERS.map(o => ownerCounts[o.id] || 0);
    const total  = counts.reduce((a, b) => a + b, 0);
    const row    = [date, ...counts, total];
    if (dateToRowIndex[date] !== undefined) {
      batchData.push({ range: `${tab}!A${dateToRowIndex[date]}`, values: [row] });
    } else {
      toAppend.push(row);
    }
  }

  if (batchData.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { valueInputOption: "RAW", data: batchData },
    });
  }
  toAppend.sort((a, b) => a[0].localeCompare(b[0]));
  if (toAppend.length) await appendRows(sheets, tab, toAppend);

  console.log(`[${tab}] Updated ${batchData.length} rows, appended ${toAppend.length} new days`);
}

async function syncInvestments1Meetings() {
  try {
    await syncMeetingsByType({
      tab: INV1_TAB, headers: INV1_HEADERS,
      titleKeyword: "Investments Meeting",
      excludeKeyword: "Meeting 2",
      timestampField: "hs_meeting_start_time",
    });
  } catch (err) { console.error("[Inv1 Meetings sync error]", err.message); }
}

async function syncPartnershipKickoffMeetings() {
  try {
    await syncMeetingsByType({
      tab: PKO_TAB, headers: PKO_HEADERS,
      titleKeyword: "Partnership Kick Off",
      timestampField: "hs_createdate",
    });
  } catch (err) { console.error("[PKO Meetings sync error]", err.message); }
}

async function syncInvestments2Meetings() {
  try {
    await syncMeetingsByType({
      tab: INV2_TAB, headers: INV2_HEADERS,
      titleKeyword: "Investments Meeting 2",
      timestampField: "hs_meeting_start_time",
    });
  } catch (err) { console.error("[Inv2 Meetings sync error]", err.message); }
}

app.get("/sync-meetings", async (req, res) => {
  syncMeetings();
  res.json({ success: true, message: "Meetings sync started in background" });
});

app.get("/sync-inv2", async (req, res) => {
  syncInvestments2Meetings();
  res.json({ success: true, message: "Investments 2 meetings sync started" });
});

app.get("/sync-inv1", async (req, res) => {
  syncInvestments1Meetings();
  res.json({ success: true, message: "Investments 1 meetings sync started" });
});

app.get("/sync-pko", async (req, res) => {
  syncPartnershipKickoffMeetings();
  res.json({ success: true, message: "Partnership Kickoff meetings sync started" });
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
  syncOperationsTicketsV2();
  syncWebinar();
  syncWebinarForm();
  syncLandingPageAnalytics();
  syncLiveTrainingPage();
  syncMeetings();
  syncInvestments2Meetings();
  syncInvestments1Meetings();
  syncPartnershipKickoffMeetings();
  syncUTMContent();
}, 10_000);

// Vidalytics delayed separately to avoid rate limit conflicts on startup
setTimeout(() => {
  syncVidalytics();
}, 60_000);

setInterval(syncInProgress,           60 * 60 * 1000); // every hour
setInterval(syncHubSpotProjects,      60 * 60 * 1000); // every hour
setInterval(syncOperationsTickets,    60 * 60 * 1000); // every hour
setInterval(syncOperationsTicketsV2,  60 * 60 * 1000); // every hour
setInterval(syncWebinar,              60 * 60 * 1000); // every hour
setInterval(syncWebinarForm,          60 * 60 * 1000); // every hour
setInterval(syncLandingPageAnalytics, 60 * 60 * 1000); // every hour
setInterval(syncLiveTrainingPage,     60 * 60 * 1000); // every hour
setInterval(syncMeetings,             60 * 60 * 1000); // every hour
setInterval(syncInvestments2Meetings,      60 * 60 * 1000); // every hour
setInterval(syncInvestments1Meetings,      60 * 60 * 1000); // every hour
setInterval(syncPartnershipKickoffMeetings,60 * 60 * 1000); // every hour
setInterval(syncUTMContent,                60 * 60 * 1000); // every hour
setInterval(syncVidalytics,           24 * 60 * 60 * 1000); // every 24 hours (daily)

app.listen(PORT, () => console.log(`Webhook server running on port ${PORT}`));