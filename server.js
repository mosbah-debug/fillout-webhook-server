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
const PORT            = process.env.PORT || 3000;
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

// Ensure the header row exists
async function ensureHeaders(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A1:F1`,
  });
  if (!res.data.values?.length) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A1:F1`,
      valueInputOption: "USER_ENTERED",
      requestBody: {
        values: [[
          "Timestamp", "Form Name", "Form ID", "Status", "Submission ID", "Month"
        ]],
      },
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

// Append a new row for each submission
async function logSubmission(sheets, data) {
  const now = new Date(data.timestamp || Date.now());
  const timestamp = now.toLocaleString("en-US", { timeZone: "Asia/Beirut" });
  const month = now.toLocaleString("en-US", { month: "long", year: "numeric", timeZone: "Asia/Beirut" });

  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A:F`,
    valueInputOption: "USER_ENTERED",
    requestBody: {
      values: [[
        timestamp,
        data.formName,
        data.formId,
        data.status,
        data.submissionId || "",
        month
      ]],
    },
  });
}

// Fetch in-progress submissions from Fillout API with pagination
async function syncInProgress() {
  try {
    console.log("Syncing in-progress submissions from Fillout...");

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });
    await ensureHeaders(sheets);
    const loggedIds = await getLoggedIds(sheets);

    let offset = 0;
    let newCount = 0;
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
        const subId = sub.submissionId;
        if (loggedIds.has(subId)) continue;

        await logSubmission(sheets, {
          formId:       FILLOUT_FORM_ID,
          formName:     "Peak Fillout (vChris)",
          status:       "In Progress",
          submissionId: subId,
          timestamp:    sub.lastUpdatedAt || sub.startedAt || new Date().toISOString(),
        });
        loggedIds.add(subId);
        newCount++;
        await new Promise(r => setTimeout(r, 2000));
      }

      offset += 100;
      if (submissions.length < 100) hasMore = false;
      console.log(`Processed page at offset ${offset}, ${newCount} new so far...`);
    }

    console.log(`Sync complete. ${newCount} new in-progress submissions added.`);
  } catch (err) {
    console.error("Sync error:", err);
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

    let status;
    if (eventType === "submission.partial" || eventType === "submission.in_progress") {
      status = "In Progress";
    } else {
      status = "Completed";
    }

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });

    await ensureHeaders(sheets);
    await logSubmission(sheets, { formId, formName, status, submissionId, timestamp: new Date().toISOString() });

    console.log(`[${status}] Form: ${formName} | ${new Date().toISOString()}`);
    res.json({ success: true, status, formName });

  } catch (err) {
    console.error("Webhook error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Manual trigger endpoint
app.get("/sync", async (req, res) => {
  syncInProgress(); // run in background, don't await
  res.json({ success: true, message: "Sync started in background" });
});

app.get("/health", (_, res) => res.json({ status: "ok" }));

// Run sync every hour
setInterval(syncInProgress, 60 * 60 * 1000);

// Run once on startup
syncInProgress();

app.listen(PORT, () => console.log(`Webhook server running on port ${PORT}`));