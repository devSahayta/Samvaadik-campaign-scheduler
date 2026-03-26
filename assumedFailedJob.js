// scheduler/assumedFailedJob.js

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const BATCH_SIZE = 500;

async function markAssumedFailed() {
  console.log("⏳ Running assumed_failed job...");

  let totalUpdated = 0;

  try {
    while (true) {
      // Step 1: Fetch batch of eligible messages
      const { data: messages, error: fetchError } = await supabase
        .from("campaign_messages")
        .select("cm_id, sent_at")
        .eq("status", "sent")
        .is("delivered_at", null)
        .is("failed_at", null)
        .not("sent_at", "is", null)
        .lt("sent_at", new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString())
        .limit(BATCH_SIZE);

      if (fetchError) {
        console.error("❌ Fetch error:", fetchError);
        break;
      }

      if (!messages || messages.length === 0) {
        console.log("✅ No more messages to process");
        break;
      }

      const ids = messages.map((m) => m.cm_id);

      // Step 2: Update those IDs
      const { data: updated, error: updateError } = await supabase
        .from("campaign_messages")
        .update({
          status: "assumed_failed",
          failed_at: new Date().toISOString(), // ✅ mark failure time
          error_message: "Message not delivered within 24 hours (timeout)",
          error_code: "TIMEOUT",
          updated_at: new Date().toISOString(),
        })
        .in("cm_id", ids)
        .select("cm_id");

      if (updateError) {
        console.error("❌ Update error:", updateError);
        break;
      }

      const updatedCount = updated?.length || 0;
      totalUpdated += updatedCount;

      console.log(`🔁 Batch updated: ${updatedCount}`);

      // Safety break (optional)
      if (updatedCount < BATCH_SIZE) {
        break;
      }
    }

    console.log(`✅ Total marked as assumed_failed: ${totalUpdated}`);
  } catch (err) {
    console.error("❌ Cron crash:", err);
  }
}

export function startAssumedFailedCron() {
  cron.schedule(
    "0 * * * *", // every 1 hour
    markAssumedFailed,
    {
      timezone: "UTC",
    },
  );

  console.log("⏰ Assumed failed cron started (runs every hour)");
}
