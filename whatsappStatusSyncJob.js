// scheduler/whatsappStatusSyncJob.js
// Runs every minute.
//
// What it does:
//   1. Finds pending rows in whatsapp_status_events (synced_at IS NULL), oldest first
//      — this is exactly what idx_wse_pending is built for.
//   2. For each event, finds the matching row in scheduled_messages, campaign_messages,
//      and woocommerce_automation_logs, and updates status + the relevant *_at
//      timestamp (and error_code/error_message on failure).
//   3. Marks the event as synced (synced_at = now()) once handled, so it never
//      gets reprocessed.
//
// whatsapp_messages is intentionally NOT touched here — it's the main table and
// is already kept up to date by the backend.
//
// Status only ever moves forward (sent -> delivered -> read, or -> failed) so an
// out-of-order webhook (e.g. "sent" arriving after "read") can't move a row backwards.
//
// scheduled_messages has no delivered_at/read_at columns (only sent_at/failed_at),
// and woocommerce_automation_logs has no timestamp columns for this at all — those
// tables just skip the timestamp write when the column doesn't exist.
//
// scheduled_messages/campaign_messages are matched by wa_message_id.
// woocommerce_automation_logs has no wa_message_id column, so it's matched by wm_id
// instead — events with no wm_id are skipped for that table.

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const BATCH_SIZE = 200;
const MAX_ATTEMPTS = 5;

// whatsapp_status_events is a queue/fan-out table only — whatsapp_messages is the
// system of record. Once a row is synced there's no reason to keep it around beyond
// a short debugging window, so synced rows older than this get deleted daily.
const RETENTION_DAYS = process.env.STATUS_EVENTS_RETENTION_DAYS_OVERRIDE
  ? parseInt(process.env.STATUS_EVENTS_RETENTION_DAYS_OVERRIDE)
  : 7;
const CLEANUP_BATCH_SIZE = 1000;

const STATUS_RANK = { sent: 1, delivered: 2, read: 3, failed: 1 };
const TIMESTAMP_FIELD = {
  sent: "sent_at",
  delivered: "delivered_at",
  read: "read_at",
  failed: "failed_at",
};

const TABLE_CONFIG = {
  scheduled_messages: {
    idColumn: "sm_id",
    matchColumn: "wa_message_id",
    timestampFields: ["sent_at", "failed_at"],
    hasUpdatedAt: true,
    hasErrorCode: true,
    backfillWmId: true,
  },
  campaign_messages: {
    idColumn: "cm_id",
    matchColumn: "wa_message_id",
    timestampFields: ["sent_at", "delivered_at", "read_at", "failed_at"],
    hasUpdatedAt: true,
    hasErrorCode: true,
    backfillWmId: true,
  },
  woocommerce_automation_logs: {
    idColumn: "id",
    matchColumn: "wm_id",
    timestampFields: [],
    hasUpdatedAt: false,
    hasErrorCode: false,
    backfillWmId: false,
  },
};

let isProcessing = false;

/* =====================================
   START CRON JOB
====================================== */

export function startWhatsappStatusSyncCron() {
  console.log("🚀 WhatsApp Status Sync Job Started!");
  console.log("⏰ Checking for pending status events every minute...");

  cron.schedule("* * * * *", async () => {
    if (isProcessing) {
      console.log("⏭️  Skipping: WhatsApp status sync job still running");
      return;
    }

    try {
      isProcessing = true;
      await runWhatsappStatusSyncJob();
    } catch (err) {
      console.error("❌ WhatsApp status sync job error:", err);
    } finally {
      isProcessing = false;
    }
  });
}

/* =====================================
   START CLEANUP CRON JOB
====================================== */

export function startWhatsappStatusEventsCleanupCron() {
  console.log("🚀 WhatsApp Status Events Cleanup Job Started!");
  console.log(
    `⏰ Deleting synced status events older than ${RETENTION_DAYS} days, daily at 3:00 AM UTC...`,
  );

  cron.schedule("0 3 * * *", async () => {
    try {
      await runWhatsappStatusEventsCleanupJob();
    } catch (err) {
      console.error("❌ WhatsApp status events cleanup job error:", err);
    }
  });
}

// Exported so it can be called manually for testing.
export async function runWhatsappStatusEventsCleanupJob() {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - RETENTION_DAYS);
  const cutoffISO = cutoffDate.toISOString();

  console.log(
    `🔍 Deleting synced whatsapp_status_events older than: ${cutoffISO}`,
  );

  let totalDeleted = 0;

  while (true) {
    // Only ever deletes synced rows — matches idx_wse_synced_at exactly.
    // Unsynced rows (synced_at IS NULL) are never touched here, even if old.
    const { data: rows, error: fetchError } = await supabase
      .from("whatsapp_status_events")
      .select("event_id")
      .not("synced_at", "is", null)
      .lt("synced_at", cutoffISO)
      .limit(CLEANUP_BATCH_SIZE);

    if (fetchError) {
      console.error(
        "❌ Failed to fetch expired status events:",
        fetchError.message,
      );
      break;
    }

    if (!rows || rows.length === 0) {
      break;
    }

    const ids = rows.map((r) => r.event_id);

    const { error: deleteError } = await supabase
      .from("whatsapp_status_events")
      .delete()
      .in("event_id", ids);

    if (deleteError) {
      console.error(
        "❌ Failed to delete expired status events:",
        deleteError.message,
      );
      break;
    }

    totalDeleted += ids.length;

    if (ids.length < CLEANUP_BATCH_SIZE) {
      break;
    }
  }

  if (totalDeleted > 0) {
    console.log(
      `✅ Deleted ${totalDeleted} synced status event(s) older than ${RETENTION_DAYS} days.`,
    );
  } else {
    console.log("✅ No expired status events found. Nothing to clean up.");
  }
}

/* =====================================
   FETCH AND PROCESS PENDING EVENTS
====================================== */

// Exported so it can be called manually for testing.
export async function runWhatsappStatusSyncJob() {
  let totalSynced = 0;

  while (true) {
    const { data: events, error } = await supabase
      .from("whatsapp_status_events")
      .select("*")
      .is("synced_at", null)
      .order("created_at", { ascending: true })
      .limit(BATCH_SIZE);

    if (error) {
      console.error(
        "❌ Failed to fetch pending whatsapp_status_events:",
        error.message,
      );
      break;
    }

    if (!events || events.length === 0) {
      break;
    }

    console.log(`📨 Syncing ${events.length} pending status event(s)...`);

    for (const event of events) {
      await syncEvent(event);
      totalSynced++;
    }

    if (events.length < BATCH_SIZE) {
      break;
    }
  }

  if (totalSynced > 0) {
    console.log(`✅ Synced ${totalSynced} status event(s).`);
  }
}

async function syncEvent(event) {
  try {
    for (const [table, config] of Object.entries(TABLE_CONFIG)) {
      await applyToTable(table, config, event);
    }

    const { error } = await supabase
      .from("whatsapp_status_events")
      .update({
        synced_at: new Date().toISOString(),
        attempts: event.attempts + 1,
      })
      .eq("event_id", event.event_id);

    if (error) {
      console.error(
        `❌ Failed to mark event ${event.event_id} as synced:`,
        error.message,
      );
    }
  } catch (err) {
    console.error(
      `❌ Failed to sync event ${event.event_id} (${event.wa_message_id}):`,
      err.message,
    );
    await markAttemptFailed(event, err.message);
  }
}

async function markAttemptFailed(event, message) {
  const attempts = event.attempts + 1;
  const giveUp = attempts >= MAX_ATTEMPTS;

  const { error } = await supabase
    .from("whatsapp_status_events")
    .update({
      attempts,
      last_error: message,
      // Stop retrying after MAX_ATTEMPTS so a permanently broken event
      // doesn't get re-fetched on every run forever.
      synced_at: giveUp ? new Date().toISOString() : null,
    })
    .eq("event_id", event.event_id);

  if (error) {
    console.error(
      `❌ Failed to record retry attempt for event ${event.event_id}:`,
      error.message,
    );
  } else if (giveUp) {
    console.error(
      `⚠️  Event ${event.event_id} (${event.wa_message_id}) exceeded ${MAX_ATTEMPTS} attempts — giving up.`,
    );
  }
}

async function applyToTable(table, config, event) {
  const matchValue = event[config.matchColumn];
  if (!matchValue) {
    return; // e.g. woocommerce_automation_logs needs wm_id, which can be null
  }

  const columns = [
    ...new Set([
      config.idColumn,
      "status",
      ...(config.backfillWmId ? ["wm_id"] : []),
      ...config.timestampFields,
    ]),
  ].join(", ");

  const { data: rows, error: fetchError } = await supabase
    .from(table)
    .select(columns)
    .eq(config.matchColumn, matchValue)
    .limit(1);

  if (fetchError) {
    throw new Error(`${table} lookup failed: ${fetchError.message}`);
  }
  if (!rows || rows.length === 0) {
    return; // this message isn't tracked in this table — nothing to sync
  }

  const row = rows[0];
  const payload = {};

  const timestampField = TIMESTAMP_FIELD[event.status];
  if (
    timestampField &&
    config.timestampFields.includes(timestampField) &&
    !row[timestampField]
  ) {
    payload[timestampField] = event.event_at;
  }

  const currentRank = STATUS_RANK[row.status] ?? 0;
  const eventRank = STATUS_RANK[event.status] ?? 0;
  if (eventRank >= currentRank) {
    payload.status = event.status;
  }

  if (event.status === "failed") {
    payload.error_message = event.error_message;
    if (config.hasErrorCode) {
      payload.error_code = event.error_code;
    }
  }

  if (config.backfillWmId && event.wm_id && !row.wm_id) {
    payload.wm_id = event.wm_id;
  }

  if (Object.keys(payload).length === 0) {
    return;
  }

  if (config.hasUpdatedAt) {
    payload.updated_at = new Date().toISOString();
  }

  const { error: updateError } = await supabase
    .from(table)
    .update(payload)
    .eq(config.idColumn, row[config.idColumn]);

  if (updateError) {
    throw new Error(`${table} update failed: ${updateError.message}`);
  }
}
