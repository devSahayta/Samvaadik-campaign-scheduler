// test-retry-ready.js
// Run manually with: node test-retry-ready.js
// Fires the retry-ready check immediately, without waiting for the cron.
//
// Uses a dynamic import() for retryReadyNotificationJob.js deliberately:
// a static `import ... from` at the top of the file would get hoisted and
// resolved BEFORE dotenv.config() runs, so SUPABASE_URL etc. would still be
// undefined when that module's top-level createClient(...) call fires.

import dotenv from "dotenv";
dotenv.config();

const { triggerRetryReadyCheckNow } =
  await import("./retryReadyNotificationJob.js");

triggerRetryReadyCheckNow()
  .then(() => {
    console.log("Done.");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Test run failed:", err);
    process.exit(1);
  });
