// scheduler/retryReadyNotificationJob.js
//
// Sends a one-time "your retry is ready" email to a campaign's owner once
// retryCampaign() would actually let them retry — i.e. the campaign is
// completed/failed, nothing is still stuck mid-send (status = "sent"), and
// there's at least one failed/assumed_failed message to retry.
//
// The "48h" wait the product has in mind isn't a timer we own here — it's
// a side effect of assumedFailedJob.js, which only flips stuck "sent"
// messages to "assumed_failed" 24h after sent_at. This job just watches for
// that moment and fires the email, mirroring retryCampaign's own gating so
// the email and the button agree on what "ready" means.
//
// Plugs into the existing scheduler/index.js.

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";
import { Resend } from "resend";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const resend = new Resend(process.env.RESEND_API_KEY);

const FRONTEND_URL = process.env.FRONTEND_URL || "https://samvaadik.com";

// Only look at campaigns that finished recently. A campaign that completed
// with zero failures will never grow any later, so there's no point
// rescanning it forever — this just bounds the query.
const LOOKBACK_DAYS = 14;

// ─── Build HTML email ─────────────────────────────────────────────────────────
function buildEmailHtml({ username, campaign, failedCount }) {
  const retryUrl = `${FRONTEND_URL}/campaigns/${campaign.campaign_id}`;

  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>Samvaadik — Retry Ready</title>
</head>
<body style="margin:0;padding:0;background:#f6f6fa;font-family:'Segoe UI',Arial,sans-serif;">

  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f6f6fa;padding:32px 16px;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

          <!-- Header -->
          <tr>
            <td style="background:#ffffff;border-radius:16px 16px 0 0;padding:28px 32px 20px;text-align:center;border-bottom:1px solid #f0f0f7;">
              <img
                src="https://samvaadik.com/images/logo-samvaadik.png"
                alt="Samvaadik"
                width="150"
                style="display:block;margin:0 auto 10px;height:auto;max-width:150px;"
              />
              <p style="margin:0;color:#9ca3af;font-size:13px;">
                Retry Ready
              </p>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="background:#ffffff;padding:28px 32px;border-left:1px solid #e4e4eb;border-right:1px solid #e4e4eb;">

              <p style="margin:0 0 20px;font-size:14.5px;color:#374151;">
                Hi <strong>${username}</strong>, 👋
              </p>

              <p style="margin:0 0 20px;font-size:14.5px;color:#374151;line-height:1.6;">
                Good news — your campaign <strong>${campaign.campaign_name}</strong> is ready for a retry.
                Some messages couldn't reach their recipients the first time, and the retry window has now opened.
              </p>

              <!-- Stats grid -->
              <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">
                <tr>
                  <td width="33%" style="padding:4px;">
                    <div style="background:#f5f3ff;border:1px solid #ddd6fe;border-radius:12px;padding:16px;text-align:center;">
                      <div style="font-size:22px;font-weight:800;color:#7c3aed;margin-bottom:4px;">${campaign.total_recipients ?? "—"}</div>
                      <div style="font-size:11px;color:#6b7280;font-weight:500;">Total<br/>Recipients</div>
                    </div>
                  </td>
                  <td width="33%" style="padding:4px;">
                    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:16px;text-align:center;">
                      <div style="font-size:22px;font-weight:800;color:#15803d;margin-bottom:4px;">${campaign.messages_sent ?? 0}</div>
                      <div style="font-size:11px;color:#6b7280;font-weight:500;">Delivered<br/>Successfully</div>
                    </div>
                  </td>
                  <td width="33%" style="padding:4px;">
                    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:12px;padding:16px;text-align:center;">
                      <div style="font-size:22px;font-weight:800;color:#c2410c;margin-bottom:4px;">${failedCount}</div>
                      <div style="font-size:11px;color:#6b7280;font-weight:500;">Ready to<br/>Retry</div>
                    </div>
                  </td>
                </tr>
              </table>

              <!-- CTA -->
              <div style="text-align:center;margin-bottom:12px;">
                <a href="${retryUrl}"
                  style="display:inline-block;background:linear-gradient(135deg,#7c3aed,#a855f7);color:#ffffff;font-size:14px;font-weight:700;padding:13px 32px;border-radius:9999px;text-decoration:none;letter-spacing:-0.01em;">
                  Retry Campaign →
                </a>
              </div>

              <p style="margin:0;font-size:12.5px;color:#9ca3af;text-align:center;line-height:1.5;">
                Each message can be retried up to 3 times, so it's worth acting while<br/>
                the window is open. Timing matters — the sooner you retry, the better<br/>
                your chances of reaching your audience.
              </p>

            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="background:#f5f4fb;border:1px solid #e4e4eb;border-top:none;border-radius:0 0 16px 16px;padding:20px 32px;text-align:center;">
              <p style="margin:0;font-size:12px;color:#c4b5fd;">
                You're receiving this because a campaign on your Samvaadik account has failed messages eligible for retry.
              </p>
              <img
                src="https://ygynmoezdffuencztefl.supabase.co/storage/v1/object/public/Samvaadik-logo/Favicon%201%20Samvaadik.png"
                alt="Samvaadik"
                width="100"
                style="display:block;margin:6px auto 0;height:auto;opacity:0.5;"
              />
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>

</body>
</html>
  `.trim();
}

// ─── Check one campaign and notify if truly ready ─────────────────────────────
async function maybeNotifyCampaign(campaign) {
  try {
    // Mirrors retryCampaign's own gate: anything still "sent" (in flight /
    // assumed-failed window not yet elapsed) means retry isn't allowed yet.
    const { count: inFlightCount, error: inFlightError } = await supabase
      .from("campaign_messages")
      .select("cm_id", { count: "exact", head: true })
      .eq("campaign_id", campaign.campaign_id)
      .eq("status", "sent");

    if (inFlightError) throw inFlightError;
    if ((inFlightCount || 0) > 0) {
      // Not ready yet — stuck messages haven't aged into assumed_failed.
      return;
    }

    const { count: failedCount, error: failedError } = await supabase
      .from("campaign_messages")
      .select("cm_id", { count: "exact", head: true })
      .eq("campaign_id", campaign.campaign_id)
      .in("status", ["failed", "assumed_failed"]);

    if (failedError) throw failedError;

    if (!failedCount) {
      // Nothing to retry — mark as handled so we stop rechecking it.
      await supabase
        .from("campaigns")
        .update({ retry_notified_at: new Date().toISOString() })
        .eq("campaign_id", campaign.campaign_id);
      return;
    }

    const { data: user, error: userError } = await supabase
      .from("users")
      .select("email")
      .eq("user_id", campaign.user_id)
      .single();

    if (userError || !user?.email) {
      console.warn(
        `⏭️  [RetryReady] No email on file for user ${campaign.user_id} — skipping campaign ${campaign.campaign_id}`,
      );
      return;
    }

    const username = user.email.split("@")[0] || "there";

    const html = buildEmailHtml({ username, campaign, failedCount });

    const { error: sendError } = await resend.emails.send({
      from: "Samvaadik <noreply@samvaadik.com>",
      to: [user.email],
      subject: `Your retry is ready — ${campaign.campaign_name}`,
      html,
    });

    if (sendError) {
      console.error(
        `❌ [RetryReady] Failed to send for campaign ${campaign.campaign_id}:`,
        sendError,
      );
      return;
    }

    await supabase
      .from("campaigns")
      .update({ retry_notified_at: new Date().toISOString() })
      .eq("campaign_id", campaign.campaign_id);

    console.log(
      `✅ [RetryReady] Notified ${user.email} for campaign "${campaign.campaign_name}" (${failedCount} retryable)`,
    );
  } catch (err) {
    console.error(
      `❌ [RetryReady] Error checking campaign ${campaign.campaign_id}:`,
      err.message,
    );
  }
}

// ─── Main runner ───────────────────────────────────────────────────────────────
async function runRetryReadyCheck() {
  console.log("📧 [RetryReady] Checking for campaigns ready to retry...");

  try {
    const since = new Date(
      Date.now() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
    ).toISOString();

    const { data: campaigns, error } = await supabase
      .from("campaigns")
      .select(
        "campaign_id, campaign_name, user_id, total_recipients, messages_sent, messages_failed, completed_at",
      )
      .in("status", ["completed", "failed"])
      .is("retry_notified_at", null)
      .gte("completed_at", since);

    if (error) throw error;

    if (!campaigns?.length) {
      console.log("✅ [RetryReady] Nothing pending");
      return;
    }

    console.log(`📧 [RetryReady] ${campaigns.length} candidate(s) to check`);

    for (const campaign of campaigns) {
      await maybeNotifyCampaign(campaign);
      await new Promise((r) => setTimeout(r, 300)); // gentle on Resend rate limits
    }

    console.log("✅ [RetryReady] Check complete");
  } catch (err) {
    console.error("❌ [RetryReady] runRetryReadyCheck error:", err.message);
  }
}

// ─── Export cron starter ──────────────────────────────────────────────────────
export function startRetryReadyCron() {
  // Every 30 minutes — fine-grained enough to catch the moment
  // assumedFailedJob (hourly) flips stuck messages, without hammering the DB.
  cron.schedule(
    "*/30 * * * *",
    async () => {
      console.log("⏰ [RetryReady] Cron triggered");
      await runRetryReadyCheck();
    },
    {
      timezone: "UTC",
    },
  );

  console.log("✅ [RetryReady] Cron scheduled — runs every 30 minutes");
}

// ─── Manual trigger (for testing) ────────────────────────────────────────────
export async function triggerRetryReadyCheckNow() {
  console.log("🔧 [RetryReady] Manual trigger — running now...");
  await runRetryReadyCheck();
}
