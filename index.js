// scheduler/index.js
// Entry point for WhatsApp Campaign Scheduler on Render.com

import dotenv from "dotenv";
import { startCampaignScheduler } from "./campaignScheduler.js";
import { startDailyResetCron } from "./dailyResetJob.js";
import { startAssumedFailedCron } from "./assumedFailedJob.js";
import { startCartRecoveryCron } from "./cartRecoveryJob.js";
import { startScheduledMessageCron } from "./scheduledMessageJob.js";
// Load environment variables
dotenv.config();

console.log("════════════════════════════════════════════════════════");
console.log("🚀 WhatsApp Campaign Scheduler - Starting...");
console.log("════════════════════════════════════════════════════════");
console.log("📅 Date:", new Date().toISOString());
console.log("🌍 Environment:", process.env.NODE_ENV || "development");
console.log("📍 Platform: Render.com Background Worker");
console.log("════════════════════════════════════════════════════════");

// Verify required environment variables
const requiredEnvVars = ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"];
const missingVars = requiredEnvVars.filter((varName) => !process.env[varName]);

if (missingVars.length > 0) {
  console.error(
    "❌ Missing required environment variables:",
    missingVars.join(", "),
  );
  console.error("💡 Please add them in Render dashboard → Environment");
  process.exit(1);
}

console.log("✅ Environment variables loaded");
console.log(
  "📊 Supabase URL:",
  process.env.SUPABASE_URL?.substring(0, 30) + "...",
);
console.log(
  "🔑 Supabase Service Role Key:",
  process.env.SUPABASE_SERVICE_ROLE_KEY?.substring(0, 20) + "...",
);

// Start the campaign scheduler
console.log("\n⏰ Starting cron job (runs every minute)...");
try {
  startCampaignScheduler();
  startDailyResetCron();
  startAssumedFailedCron();
  startCartRecoveryCron();
  startScheduledMessageCron();
  console.log("✅ Scheduler is running successfully!");
  console.log("🔄 Checking for campaigns every 60 seconds");
  console.log("💡 Press Ctrl+C to stop (or let Render manage it)");
  console.log("════════════════════════════════════════════════════════\n");
} catch (err) {
  console.error("❌ Failed to start scheduler:", err);
  process.exit(1);
}

/* =====================================
   GRACEFUL SHUTDOWN HANDLERS
====================================== */

process.on("SIGTERM", () => {
  console.log("\n════════════════════════════════════════════════════════");
  console.log("👋 Received SIGTERM signal");
  console.log("🛑 Shutting down gracefully...");
  console.log("════════════════════════════════════════════════════════");
  process.exit(0);
});

process.on("SIGINT", () => {
  console.log("\n════════════════════════════════════════════════════════");
  console.log("👋 Received SIGINT signal (Ctrl+C)");
  console.log("🛑 Shutting down gracefully...");
  console.log("════════════════════════════════════════════════════════");
  process.exit(0);
});

/* =====================================
   ERROR HANDLERS
====================================== */

process.on("uncaughtException", (error) => {
  console.error("\n════════════════════════════════════════════════════════");
  console.error("💥 Uncaught Exception:", error);
  console.error("Stack:", error.stack);
  console.error("════════════════════════════════════════════════════════");
  process.exit(1);
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("\n════════════════════════════════════════════════════════");
  console.error("💥 Unhandled Rejection at:", promise);
  console.error("Reason:", reason);
  console.error("════════════════════════════════════════════════════════");
  process.exit(1);
});

/* =====================================
   KEEP-ALIVE (Optional)
====================================== */

// Send heartbeat every 5 minutes to show service is alive
setInterval(
  () => {
    const now = new Date();
    console.log(
      `💓 Heartbeat: ${now.toISOString()} - Scheduler running smoothly`,
    );
  },
  5 * 60 * 1000,
); // Every 5 minutes
