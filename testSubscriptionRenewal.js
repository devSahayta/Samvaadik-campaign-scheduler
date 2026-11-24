// scheduler/testSubscriptionRenewal.js
// Run this manually to test the subscription auto-renewal check:
//   node scheduler/testSubscriptionRenewal.js
//
// Make sure your .env (in this scheduler repo) has:
//   BACKEND_URL (point this at your LOCAL backend while testing,
//                e.g. http://localhost:5000)
//   CRON_SECRET (must match the exact value set on the backend)

import dotenv from "dotenv";
dotenv.config();

const { runSubscriptionRenewalCheck } =
  await import("./subscriptionRenewalJob.js");

console.log("════════════════════════════════════════");
console.log("🧪 Manual Subscription Auto-Renewal Test");
console.log("════════════════════════════════════════");
console.log("📅 Time:", new Date().toISOString());
console.log("🌐 Backend URL:", process.env.BACKEND_URL || "❌ Missing");
console.log(
  "🔑 Cron Secret:",
  process.env.CRON_SECRET ? "✅ Found" : "❌ Missing",
);
console.log("════════════════════════════════════════\n");

if (!process.env.BACKEND_URL || !process.env.CRON_SECRET) {
  console.error(
    "❌ BACKEND_URL or CRON_SECRET is missing in .env — cannot run the check",
  );
  process.exit(1);
}

try {
  await runSubscriptionRenewalCheck();
  console.log("\n✅ Test complete — check the logs above for the result");
  process.exit(0);
} catch (err) {
  console.error("\n❌ Test failed:", err.message);
  process.exit(1);
}
