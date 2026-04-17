const express = require("express");
const { google } = require("googleapis");
const fs = require("fs");

const app = express();
app.use(express.json());

// ── CONFIG ──────────────────────────────────────────────────────────────────
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;       // Your Google Sheet ID
const SHEET_NAME     = process.env.SHEET_NAME || "Form Tracker"; // Tab name
const FILLOUT_SECRET = process.env.FILLOUT_WEBHOOK_SECRET; // Optional: to verify requests
const PORT           = process.env.PORT || 3000;
// ────────────────────────────────────────────────────────────────────────────

// Authenticate with Google using Service Account
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

// Find a form's row by formId, or return null
async function findFormRow(sheets, formId) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A:A`,
  });
  const rows = res.data.values || [];
  for (let i = 1; i < rows.length; i++) {
    if (rows[i][0] === formId) return i + 1; // 1-indexed row number
  }
  return null;
}

// Get current counts for a row
async function getRowData(sheets, rowNum) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A${rowNum}:F${rowNum}`,
  });
  return res.data.values?.[0] || [];
}

// Add a new form row
async function addFormRow(sheets, formId, formName) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A:F`,
    valueInputOption: "USER_ENTERED",
    requestBody: {
      values: [[formId, formName, 0, 0, "0%", new Date().toISOString()]],
    },
  });
}

// Update counts for an existing row
async function updateRow(sheets, rowNum, completed, attempted) {
  const rate = attempted > 0
    ? Math.round((completed / attempted) * 100) + "%"
    : "0%";

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!C${rowNum}:F${rowNum}`,
    valueInputOption: "USER_ENTERED",
    requestBody: {
      values: [[completed, attempted, rate, new Date().toISOString()]],
    },
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
          "Form ID", "Form Name",
          "Submissions Completed", "Submissions Attempted",
          "Completion Rate", "Last Updated"
        ]],
      },
    });
  }
}

// ── WEBHOOK ENDPOINT ─────────────────────────────────────────────────────────
app.post("/webhook/fillout", async (req, res) => {
  try {
    // Optional: verify Fillout webhook secret
    if (FILLOUT_SECRET && req.headers["x-fillout-secret"] !== FILLOUT_SECRET) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const event = req.body;
    const eventType = event.eventType; // "submission.completed" or "submission.partial"
    const formId    = event.formId;
    const formName  = event.formName || formId;

    // Only handle relevant event types
    if (!["submission.completed", "submission.partial"].includes(eventType)) {
      return res.json({ message: "Event type ignored" });
    }

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });

    await ensureHeaders(sheets);

    let rowNum = await findFormRow(sheets, formId);

    if (!rowNum) {
      // New form — add a row then find it
      await addFormRow(sheets, formId, formName);
      rowNum = await findFormRow(sheets, formId);
    }

    // Get current values
    const rowData   = await getRowData(sheets, rowNum);
    let completed   = parseInt(rowData[2]) || 0;
    let attempted   = parseInt(rowData[3]) || 0;

    // Increment the right counter
    // Every event counts as an attempt; completed events also increment completed
    attempted += 1;
    if (eventType === "submission.completed") {
      completed += 1;
    }

    await updateRow(sheets, rowNum, completed, attempted);

    console.log(`[${eventType}] Form: ${formName} | Completed: ${completed} | Attempted: ${attempted}`);
    res.json({ success: true, completed, attempted });

  } catch (err) {
    console.error("Webhook error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/health", (_, res) => res.json({ status: "ok" }));

app.listen(PORT, () => console.log(`Webhook server running on port ${PORT}`));

