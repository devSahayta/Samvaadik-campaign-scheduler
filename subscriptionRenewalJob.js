// scheduler/subscriptionRenewalJob.js
import axios from "axios";

const INTERVAL_MS = 15 * 60 * 1000; // run every 15 minutes

// ─── Small retry helper — mirrors orderReconciliationJob.js ─────────────────
async function withRetry(
  fn,
  { retries = 2, delayMs = 1000, label = "request" } = {},
) {
  let lastErr;
  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const retryable =
        err.code === "ECONNRESET" ||
        err.code === "ETIMEDOUT" ||
        err.message?.includes("socket hang up") ||
        err.message?.includes("timeout");
      console.warn(
        `   ⚠️  ${label} attempt ${attempt} failed: ${err.message}${retryable ? " (retrying)" : ""}`,
      );
      if (!retryable || attempt > retries) break;
      await new Promise((r) => setTimeout(r, delayMs * attempt));
    }
  }
  throw lastErr;
}

export async function runSubscriptionRenewalCheck() {
  const backendUrl = process.env.BACKEND_URL;
  const cronSecret = process.env.CRON_SECRET;

  if (!backendUrl || !cronSecret) {
    console.error(
      "   ❌ Subscription renewal check skipped — BACKEND_URL or CRON_SECRET not set in this scheduler's environment",
    );
    return;
  }

  console.log("\n💳 Subscription auto-renewal check starting...");
  try {
    // Each call renews at most ONE subscription by design (see
    // subscriptionRenewalService.js on the backend) — the backend is a
    // Vercel serverless function with a hard execution-time limit, so this
    // job is meant to tick every 15 min and drain due renewals one at a
    // time rather than ask the backend to process a whole batch at once.
    const res = await withRetry(
      () =>
        axios.post(
          `${backendUrl.replace(/\/$/, "")}/api/subscription/run-auto-renewals`,
          {},
          {
            headers: { "x-cron-secret": cronSecret },
            timeout: 20000,
          },
        ),
      { retries: 2, delayMs: 2000, label: "run-auto-renewals call" },
    );

    const { processed, results } = res.data || {};
    if (processed > 0) {
      console.log(`   ✅ Processed ${processed} renewal(s):`, results);
    } else {
      console.log("   ℹ️  No subscriptions due for renewal right now");
    }
  } catch (err) {
    const errMsg = err.response?.data?.message || err.message;
    console.error(`   ❌ Subscription renewal check failed: ${errMsg}`);
  }
  console.log("💳 Subscription auto-renewal check complete\n");
}

let isRunning = false; // prevents overlapping runs if one call takes longer than expected

export function startSubscriptionRenewalCron() {
  console.log(
    "💳 Subscription auto-renewal cron scheduled (every 15 min, 1 renewal per tick)",
  );
  setInterval(() => {
    if (isRunning) {
      console.log("⏭️  Skipping tick — previous renewal check still running");
      return;
    }
    isRunning = true;
    runSubscriptionRenewalCheck()
      .catch((err) =>
        console.error("❌ Subscription renewal cron error:", err.message),
      )
      .finally(() => {
        isRunning = false;
      });
  }, INTERVAL_MS);
}