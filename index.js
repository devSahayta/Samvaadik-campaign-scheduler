// scheduler/index.js

import express from "express";
import dotenv from "dotenv";
import { checkAndSendCampaigns } from "./campaignScheduler.js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

console.log("🚀 WhatsApp Campaign Scheduler API Starting...");

// Security token so random people cannot trigger it
const CRON_SECRET = process.env.CRON_SECRET || "mysecret";

// Endpoint called by cron-job.org
app.get("/run-scheduler", async (req, res) => {

  if (req.query.secret !== CRON_SECRET) {
    return res.status(401).send("Unauthorized");
  }

  console.log("🔔 Scheduler triggered:", new Date().toISOString());

  try {
    await checkAndSendCampaigns();
    res.send("Scheduler executed successfully");
  } catch (err) {
    console.error("Scheduler failed:", err);
    res.status(500).send("Scheduler error");
  }

});

// Health check
app.get("/", (req, res) => {
  res.send("Scheduler service running");
});

app.listen(PORT, () => {
  console.log(`🌐 Server running on port ${PORT}`);
});
