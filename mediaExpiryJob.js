// scheduler/mediaExpiryJob.js
// Runs daily at 2:00 AM UTC.
//
// What it does:
//   1. Finds all rows in whatsapp_media_uploads where uploaded_at is older than 25 days.
//   2. For each expired media_id, sets media_id = NULL on the matching whatsapp_templates row
//      (single-header templates), and clears any matching entries inside carousel_media[]
//      (media card carousel templates — each card holds its own media_id).
//   3. Deletes the expired rows from whatsapp_media_uploads.
//
// Why 25 days: Meta expires uploaded media handles after 30 days.
// We clear at 25 to give users a 5-day buffer to re-upload before
// their next campaign send.

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const EXPIRY_DAYS = process.env.EXPIRY_DAYS_OVERRIDE
  ? parseInt(process.env.EXPIRY_DAYS_OVERRIDE)
  : 25;

export function startMediaExpiryCron() {
  // Runs every day at 2:00 AM UTC
  cron.schedule("0 2 * * *", async () => {
    console.log("\n════════════════════════════════════════════════════════");
    console.log("🗑️  MEDIA EXPIRY JOB — Starting");
    console.log(`📅 Date: ${new Date().toISOString()}`);
    console.log("════════════════════════════════════════════════════════");

    try {
      await runMediaExpiryJob();
    } catch (err) {
      console.error("❌ Media expiry job crashed:", err);
    }

    console.log("════════════════════════════════════════════════════════\n");
  });

  console.log("✅ Media expiry cron scheduled (daily at 2:00 AM UTC)");
}

// Exported so you can call it manually for testing.
// Pass a testAccountId to scope the run to one account only (safe for production data).
export async function runMediaExpiryJob({ testAccountId = null } = {}) {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - EXPIRY_DAYS);
  const cutoffISO = cutoffDate.toISOString();

  if (testAccountId) {
    console.log(`🧪 TEST MODE — scoped to account_id: ${testAccountId}`);
  }
  console.log(`🔍 Looking for media uploaded before: ${cutoffISO}`);

  // ── Step 1: Fetch expired media records ────────────────────────────────────
  let query = supabase
    .from("whatsapp_media_uploads")
    .select("wmu_id, media_id, file_name, account_id, uploaded_at")
    .lt("uploaded_at", cutoffISO);

  // Scope to one account when testing — never touches other users' data
  if (testAccountId) {
    query = query.eq("account_id", testAccountId);
  }

  const { data: expiredMedia, error: fetchErr } = await query;

  if (fetchErr) {
    console.error("❌ Failed to fetch expired media:", fetchErr.message);
    return;
  }

  if (!expiredMedia || expiredMedia.length === 0) {
    console.log("✅ No expired media found. Nothing to clean up.");
    return;
  }

  console.log(`📦 Found ${expiredMedia.length} expired media record(s):`);
  expiredMedia.forEach((m) => {
    const age = Math.floor(
      (Date.now() - new Date(m.uploaded_at).getTime()) / (1000 * 60 * 60 * 24),
    );
    console.log(
      `   • ${m.file_name} (media_id: ${m.media_id}) — ${age} days old`,
    );
  });

  const expiredMediaIds = expiredMedia.map((m) => m.media_id).filter(Boolean);

  // ── Step 2: Null out media_id on affected templates ────────────────────────
  if (expiredMediaIds.length > 0) {
    const { data: updatedTemplates, error: updateErr } = await supabase
      .from("whatsapp_templates")
      .update({ media_id: null })
      .in("media_id", expiredMediaIds)
      .select("wt_id, name, media_id");

    if (updateErr) {
      console.error(
        "❌ Failed to null out media_id on templates:",
        updateErr.message,
      );
      // Don't return — still try to delete the upload records
    } else {
      const count = updatedTemplates?.length ?? 0;
      if (count > 0) {
        console.log(`✅ Cleared media_id from ${count} template(s):`);
        updatedTemplates.forEach((t) => {
          console.log(`   • ${t.name} (${t.wt_id})`);
        });
      } else {
        console.log("ℹ️  No templates were referencing the expired media.");
      }
    }
  }

  // ── Step 2b: Null out matching entries inside carousel_media[] ─────────────
  // Carousel templates store one media_id per card in the carousel_media jsonb
  // column, so a single expired upload only needs that one card's media_id
  // cleared — card_index/header_format stay intact so the template list can
  // still show "Needs reupload" against the right card.
  if (expiredMediaIds.length > 0) {
    const expiredSet = new Set(expiredMediaIds);

    const { data: carouselTemplates, error: carouselFetchErr } = await supabase
      .from("whatsapp_templates")
      .select("wt_id, name, carousel_media")
      .eq("is_carousel", true)
      .not("carousel_media", "is", null);

    if (carouselFetchErr) {
      console.error(
        "❌ Failed to fetch carousel templates:",
        carouselFetchErr.message,
      );
    } else if (carouselTemplates && carouselTemplates.length > 0) {
      let clearedCount = 0;

      for (const tpl of carouselTemplates) {
        const cards = Array.isArray(tpl.carousel_media)
          ? tpl.carousel_media
          : [];
        const hasExpiredCard = cards.some((c) => expiredSet.has(c.media_id));
        if (!hasExpiredCard) continue;

        const updatedCards = cards.map((c) =>
          expiredSet.has(c.media_id) ? { ...c, media_id: null } : c,
        );

        const { error: updateCarouselErr } = await supabase
          .from("whatsapp_templates")
          .update({ carousel_media: updatedCards })
          .eq("wt_id", tpl.wt_id);

        if (updateCarouselErr) {
          console.error(
            `❌ Failed to clear carousel media for ${tpl.name} (${tpl.wt_id}):`,
            updateCarouselErr.message,
          );
        } else {
          clearedCount++;
          console.log(
            `   • ${tpl.name} (${tpl.wt_id}) — cleared expired card media`,
          );
        }
      }

      if (clearedCount > 0) {
        console.log(
          `✅ Cleared expired card media on ${clearedCount} carousel template(s).`,
        );
      } else {
        console.log(
          "ℹ️  No carousel templates were referencing the expired media.",
        );
      }
    } else {
      console.log("ℹ️  No carousel templates with stored media found.");
    }
  }

  // ── Step 3: Delete expired rows from whatsapp_media_uploads ───────────────
  const expiredIds = expiredMedia.map((m) => m.wmu_id);

  const { error: deleteErr } = await supabase
    .from("whatsapp_media_uploads")
    .delete()
    .in("wmu_id", expiredIds);

  if (deleteErr) {
    console.error(
      "❌ Failed to delete expired media records:",
      deleteErr.message,
    );
    return;
  }

  console.log(
    `✅ Deleted ${expiredIds.length} expired record(s) from whatsapp_media_uploads.`,
  );
  console.log(
    "💡 Affected templates (and carousel cards) now show media_id = NULL. Users can re-upload from the Templates page.",
  );
}
