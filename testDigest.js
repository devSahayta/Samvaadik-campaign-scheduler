// scheduler/testDigest.js
// Run this manually to test the digest email:
//   node scheduler/testDigest.js
//
// Make sure your .env has:
//   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RESEND_API_KEY, FRONTEND_URL

import dotenv from "dotenv";
dotenv.config();

const { triggerDigestNow } = await import("./dailyNotificationDigest.js");

console.log("════════════════════════════════════════");
console.log("🧪 Manual Digest Test");
console.log("════════════════════════════════════════");
console.log("📅 Time:", new Date().toISOString());
console.log(
  "📧 Resend Key:",
  process.env.RESEND_API_KEY ? "✅ Found" : "❌ Missing",
);
console.log(
  "🗄️  Supabase:",
  process.env.SUPABASE_URL ? "✅ Found" : "❌ Missing",
);
console.log(
  "🌐 Frontend:",
  process.env.FRONTEND_URL || "http://localhost:5173",
);
console.log("════════════════════════════════════════\n");

if (!process.env.RESEND_API_KEY) {
  console.error("❌ RESEND_API_KEY is missing in .env — cannot send email");
  process.exit(1);
}

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error("❌ Supabase credentials missing in .env");
  process.exit(1);
}

try {
  await triggerDigestNow();
  console.log("\n✅ Test complete — check your inbox (and spam folder)");
  process.exit(0);
} catch (err) {
  console.error("\n❌ Test failed:", err.message);
  process.exit(1);
}
