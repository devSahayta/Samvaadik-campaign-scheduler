import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

async function runDailyReset() {
  console.log("🌙 Running midnight reset...");

  const now = new Date();

  const midnightUTC = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()),
  ).toISOString();

  const { data, error } = await supabase
    .from("whatsapp_accounts")
    .update({
      warmup_daily_sent: 0,
      tier_daily_sent: 0,
      warmup_daily_reset_at: midnightUTC,
      tier_daily_reset_at: midnightUTC,
    })
    .not("wa_id", "is", null)
    .eq("status", "active")
    .eq("warmup_enabled", true)
    .select();

  if (error) {
    console.error("❌ Reset failed:", error);
  } else {
    console.log(`✅ Reset success for ${data.length} accounts`);
  }
}

export function startDailyResetCron() {
  cron.schedule(
    "0 0 * * *", // midnight UTC
    // "*/1 * * * *", // every 1 minute Testing
    runDailyReset,
    {
      timezone: "UTC",
    },
  );

  console.log("⏰ Daily reset cron started (UTC midnight)");
}
