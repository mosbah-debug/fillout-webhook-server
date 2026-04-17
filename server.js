const express = require("express");
const { google } = require("googleapis");
const fs = require("fs");

const app = express();
app.use(express.json());

// ── CONFIG ──────────────────────────────────────────────────────────────────
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const SHEET_NAME     = process.env.SHEET_NAME || "Fillout Log";
const PORT           = process.env.PORT || 3000;
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

// Append a new row for each submission
async function logSubmission(sheets, data) {
  const now = new Date();
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

// ── WEBHOOK ENDPOINT ─────────────────────────────────────────────────────────
app.post("/webhook/fillout", async (req, res) => {
  try {
    const event = req.body;

    const eventType    = event.eventType || "submission.completed";
    const formId       = event.formId || event.form_id || "unknown-form";
    const formName     = event.formName || event.form_name || formId;
    const submissionId = event.submissionId || event.submission_id || "";

    // Map event type to a readable status
    let status;
    if (eventType === "submission.completed") {
      status = "Completed";
    } else if (eventType === "submission.partial" || eventType === "submission.in_progress") {
      status = "In Progress";
    } else {
      status = "Completed"; // default for Fillout REST webhook
    }

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: "v4", auth });

    await ensureHeaders(sheets);
    await logSubmission(sheets, { formId, formName, status, submissionId });

    console.log(`[${status}] Form: ${formName} | ${new Date().toISOString()}`);
    res.json({ success: true, status, formName });

  } catch (err) {
    console.error("Webhook error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/health", (_, res) => res.json({ status: "ok" }));

app.listen(PORT, () => console.log(`Webhook server running on port ${PORT}`));