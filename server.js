const express = require("express");
const { google } = require("googleapis");
const fs = require("fs");

const app = express();
app.use(express.json());

// ── CONFIG ──────────────────────────────────────────────────────────────────
const SPREADSHEET_ID  = process.env.SPREADSHEET_ID;
const SHEET_NAME      = process.env.SHEET_NAME || "Fillout Log";
const FILLOUT_API_KEY = process.env.FILLOUT_API_KEY;
const FILLOUT_FORM_ID = process.env.FILLOUT_FORM_ID;
const HUBSPOT_TOKEN   = process.env.HUBSPOT_TOKEN;
const PROJECTS_SHEET  = "HubSpot Projects";
const PORT            = process.env.PORT || 3000;

// Fixed question order based on your form
const QUESTION_NAMES = [
  "Before Continuing - Did You Watch The Video Above?",
  "First Name",
  "Last Name",
  "Email",
  "Mobile Phone Number",
  "Will the retirement planning be just yourself or include a spouse/partner",
  "About how much have you saved for retirement?",
  "Are you retired, looking to retire in the next 5 years, or looking to retire in the next 10 years?",
  "How many of our educational YouTube videos would you guess you've watched?",
  "How did you hear about Peak Financial Planning?",
  "(OPTIONAL) Please share any additional information related to your goals or pain points you think would be helpful",
  "I agree to receive recurring automated text messages at the phone number provided. Msg & data rates may apply. Msg frequency varies. Reply HELP for help and STOP to cancel. View our Terms of Service and Privacy Policy."
];
// ────────────────────────────────────────────────────────────────────────────

function getGoogleAuth() {
  const credentials = JSON.parse(
    process.env.GOOGLE_SERVICE_ACCOUNT_JSON ||
    fs.readFileSync("service-account.json", "utf8")
  );
  return new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
}

// Ensure the header row exists with all question columns
async function ensureHeaders(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!1:1`,
  });
  if (!res.data.values?.length) {
    const headers = [
      "Timestamp", "Form Name", "Form ID", "Status", "Submission ID", "Month",
      ...QUESTION_NAMES
    ];
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!1:1`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [headers] },
    });
  }
}

// Get all logged submission IDs to avoid duplicates
async function getLoggedIds(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!E:E`,
  });
  const rows = res.data.values || [];
  return new Set(rows.flat().filter(Boolean));
}

// Build a row from submission data including all question answers
function buildRow(data) {
  const now = new Date(data.timestamp || Date.now());
  const timestamp = now.toLocaleString("en-US", { timeZone: "Asia/Beirut" });
  const month = now.toLocaleString("en-US", { month: "long", year: "numeric", timeZone: "Asia/Beirut" });

  const answerMap = {};
  if (data.questions && Array.isArray(data.questions)) {
    for (const q of data.questions) {
      const val = Array.isArray(q.value) ? q.value.join(", ") : (q.value ?? "");
      answerMap[q.name] = val;
    }
  }

  const answers = QUESTION_NAMES.map(name => answerMap[name] ?? "");

  return [
    timestamp,
    data.formName,
    data.formId,
    data.status,
    data.submissionId || "",
    month,
    ...answers
  ];
}

// Batch write rows
async function batchLogSubmissions(sheets, submissions) {
  if (submissions.length === 0) return;
  const rows = submissions.map(buildRow);
  const chunkSize = 500;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A:A`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: chunk },
    });
    console.log(`Wrote rows ${i + 1} to ${i + chunk.length}`);
    if (i + chunkSize < rows.length) {
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}

// Fetch ALL in-progress submissions from Fillout API with pagination
async function syncInProgress() {
  try {
    console.log("Syncing in-progress submissions from Fillout...");

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureHeaders(sheets);
    const loggedIds = await getLoggedIds(sheets);

    let offset = 0;
    let allNew = [];
    let hasMore = true;

    while (hasMore) {
      const response = await fetch(
        `https://api.fillout.com/v1/api/forms/${FILLOUT_FORM_ID}/submissions?status=in_progress&limit=100&offset=${offset}`,
        { headers: { "Authorization": `Bearer ${FILLOUT_API_KEY}` } }
      );

      if (!response.ok) {
        console.error("Fillout API error:", await response.text());
        break;
      }

      const data = await response.json();
      const submissions = data.responses || [];
      if (submissions.length === 0) { hasMore = false; break; }

      for (const sub of submissions) {
        if (loggedIds.has(sub.submissionId)) continue;
        allNew.push({
          formId:       FILLOUT_FORM_ID,
          formName:     "Peak Fillout (vChris)",
          status:       "In Progress",
          submissionId: sub.submissionId,
          timestamp:    sub.lastUpdatedAt || sub.startedAt || new Date().toISOString(),
          questions:    sub.questions || [],
        });
      }

      offset += 100;
      if (submissions.length < 100) hasMore = false;
      console.log(`Fetched page at offset ${offset}, ${allNew.length} new found so far...`);
    }

    console.log(`Fetched all pages. Writing ${allNew.length} new submissions to sheet...`);
    await batchLogSubmissions(sheets, allNew);
    console.log(`Sync complete. ${allNew.length} new in-progress submissions added.`);
  } catch (err) {
    console.error("Sync error:", err);
  }
}

// ── HUBSPOT PROJECTS SYNC ────────────────────────────────────────────────────

// Ensure the HubSpot Projects sheet has headers
async function ensureProjectHeaders(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${PROJECTS_SHEET}!1:1`,
  });
  if (!res.data.values?.length) {
    const headers = [
      "Project ID", "Project Name", "Pipeline", "Stage", "FP Owner",
      "WA Owner", "Card Due Date", "Target Due Date", "Associated Contact",
      "Contact Email", "Last Synced"
    ];
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${PROJECTS_SHEET}!1:1`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [headers] },
    });
    console.log("Created HubSpot Projects sheet headers.");
  }
}

// Fetch all projects from HubSpot custom object 0-970 with pagination
async function fetchAllProjects() {
  const properties = [
    "hs_object_id", "hs_name", "hs_pipeline", "hs_pipeline_stage",
    "fp_owner", "wa_owner", "card_due_date", "target_due_date"
  ];

  let allProjects = [];
  let after = null;

  while (true) {
    const url = new URL("https://api.hubapi.com/crm/v3/objects/0-970");
    url.searchParams.set("limit", "100");
    url.searchParams.set("properties", properties.join(","));
    url.searchParams.set("associations", "contacts");
    if (after) url.searchParams.set("after", after);

    const res = await fetch(url.toString(), {
      headers: {
        "Authorization": `Bearer ${HUBSPOT_TOKEN}`,
        "Content-Type": "application/json"
      }
    });

    if (!res.ok) {
      const err = await res.text();
      console.error("HubSpot API error:", err);
      throw new Error(`HubSpot API error: ${res.status}`);
    }

    const data = await res.json();
    allProjects = allProjects.concat(data.results || []);

    if (data.paging?.next?.after) {
      after = data.paging.next.after;
    } else {
      break;
    }
  }

  console.log(`Fetched ${allProjects.length} projects from HubSpot.`);
  return allProjects;
}

// Fetch pipeline stage labels so we can show readable names not IDs
async function fetchPipelineStages() {
  const res = await fetch(
    "https://api.hubapi.com/crm/v3/pipelines/0-970",
    {
      headers: {
        "Authorization": `Bearer ${HUBSPOT_TOKEN}`,
        "Content-Type": "application/json"
      }
    }
  );

  if (!res.ok) {
    console.warn("Could not fetch pipeline stages, will use raw IDs.");
    return { pipelines: {}, stages: {} };
  }

  const data = await res.json();
  const pipelines = {};
  const stages = {};

  for (const pipeline of (data.results || [])) {
    pipelines[pipeline.id] = pipeline.label;
    for (const stage of (pipeline.stages || [])) {
      stages[stage.id] = stage.label;
    }
  }

  return { pipelines, stages };
}

// Fetch contact name + email for a given contact ID
async function fetchContact(contactId) {
  try {
    const res = await fetch(
      `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}?properties=firstname,lastname,email`,
      {
        headers: {
          "Authorization": `Bearer ${HUBSPOT_TOKEN}`,
          "Content-Type": "application/json"
        }
      }
    );
    if (!res.ok) return { name: "", email: "" };
    const data = await res.json();
    const p = data.properties || {};
    const name = [p.firstname, p.lastname].filter(Boolean).join(" ");
    return { name, email: p.email || "" };
  } catch {
    return { name: "", email: "" };
  }
}

// Format a HubSpot timestamp (ms) to readable date
function formatDate(val) {
  if (!val) return "";
  const d = new Date(Number(val));
  return isNaN(d) ? val : d.toLocaleDateString("en-US", { timeZone: "Asia/Beirut" });
}

// Main HubSpot Projects sync function
async function syncHubSpotProjects() {
  if (!HUBSPOT_TOKEN) {
    console.warn("HUBSPOT_TOKEN not set, skipping projects sync.");
    return;
  }

  try {
    console.log("Syncing HubSpot Projects...");

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureProjectHeaders(sheets);

    const [projects, { pipelines, stages }] = await Promise.all([
      fetchAllProjects(),
      fetchPipelineStages()
    ]);

    const syncedAt = new Date().toLocaleString("en-US", { timeZone: "Asia/Beirut" });
    const rows = [];

    for (const project of projects) {
      const p = project.properties || {};

      // Get first associated contact if any
      const contactIds = project.associations?.contacts?.results?.map(c => c.id) || [];
      let contactName = "";
      let contactEmail = "";
      if (contactIds.length > 0) {
        const contact = await fetchContact(contactIds[0]);
        contactName  = contact.name;
        contactEmail = contact.email;
        // Small delay to avoid hitting HubSpot rate limits
        await new Promise(r => setTimeout(r, 100));
      }

      rows.push([
        p.hs_object_id || project.id || "",
        p.hs_name || "",
        pipelines[p.hs_pipeline] || p.hs_pipeline || "",
        stages[p.hs_pipeline_stage] || p.hs_pipeline_stage || "",
        p.fp_owner || "",
        p.wa_owner || "",
        formatDate(p.card_due_date),
        formatDate(p.target_due_date),
        contactName,
        contactEmail,
        syncedAt
      ]);
    }

    // Clear existing data (except header) then rewrite — full refresh
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: `${PROJECTS_SHEET}!A2:Z`,
    });

    if (rows.length > 0) {
      const chunkSize = 500;
      for (let i = 0; i < rows.length; i += chunkSize) {
        const chunk = rows.slice(i, i + chunkSize);
        await sheets.spreadsheets.values.append({
          spreadsheetId: SPREADSHEET_ID,
          range: `${PROJECTS_SHEET}!A2`,
          valueInputOption: "USER_ENTERED",
          requestBody: { values: chunk },
        });
        if (i + chunkSize < rows.length) {
          await new Promise(r => setTimeout(r, 2000));
        }
      }
    }

    console.log(`HubSpot Projects sync complete. ${rows.length} projects written.`);
  } catch (err) {
    console.error("HubSpot Projects sync error:", err);
  }
}

// ── WEBHOOK ENDPOINT ─────────────────────────────────────────────────────────
app.post("/webhook/fillout", async (req, res) => {
  try {
    const event = req.body;
    const eventType    = event.eventType || "submission.completed";
    const formId       = event.formId || event.form_id || FILLOUT_FORM_ID || "unknown-form";
    const formName     = event.formName || event.form_name || formId;
    const submissionId = event.submissionId || event.submission_id || "";
    const questions    = event.questions || event.data?.questions || [];

    let status;
    if (eventType === "submission.partial" || eventType === "submission.in_progress") {
      status = "In Progress";
    } else {
      status = "Completed";
    }

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureHeaders(sheets);
    await batchLogSubmissions(sheets, [{
      formId, formName, status, submissionId,
      timestamp: new Date().toISOString(),
      questions
    }]);

    console.log(`[${status}] Form: ${formName} | ${new Date().toISOString()}`);
    res.json({ success: true, status, formName });
  } catch (err) {
    console.error("Webhook error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── ENDPOINTS ────────────────────────────────────────────────────────────────

// Manual trigger for Fillout in-progress sync
app.get("/sync", async (req, res) => {
  syncInProgress();
  res.json({ success: true, message: "Fillout sync started in background" });
});

// Manual trigger for HubSpot Projects sync
app.get("/sync-projects", async (req, res) => {
  syncHubSpotProjects();
  res.json({ success: true, message: "HubSpot Projects sync started in background" });
});

app.get("/health", (_, res) => res.json({ status: "ok" }));

// ── SCHEDULES ────────────────────────────────────────────────────────────────
setInterval(syncInProgress, 60 * 60 * 1000);        // Fillout: every hour
setInterval(syncHubSpotProjects, 60 * 60 * 1000);   // HubSpot Projects: every hour

syncInProgress();
syncHubSpotProjects();

app.listen(PORT, () => console.log(`Webhook server running on port ${PORT}`));