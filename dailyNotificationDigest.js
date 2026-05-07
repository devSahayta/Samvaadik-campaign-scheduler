// scheduler/dailyNotificationDigest.js
// Sends a daily summary email to each admin at 8 AM IST
// Plugs into the existing scheduler/index.js

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";
import { Resend } from "resend";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const resend = new Resend(process.env.RESEND_API_KEY);

// ─── Build HTML email ─────────────────────────────────────────────────────────
function buildEmailHtml({ username, email, stats, pendingChats, date }) {
  const pendingRows =
    pendingChats.length > 0
      ? pendingChats
          .map(
            (c) => `
          <tr>
            <td style="padding:10px 14px;border-bottom:1px solid #f0f0f5;font-size:13.5px;color:#1a1523;font-weight:500;">
              ${c.person_name || c.phone_number || "Unknown"}
            </td>
            <td style="padding:10px 14px;border-bottom:1px solid #f0f0f5;font-size:13px;color:#6b7280;max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
              ${c.last_message || "—"}
            </td>
            <td style="padding:10px 14px;border-bottom:1px solid #f0f0f5;font-size:12px;color:#94a3b8;white-space:nowrap;">
              ${c.last_message_at ? new Date(c.last_message_at).toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" }) : "—"}
            </td>
          </tr>`,
          )
          .join("")
      : `<tr><td colspan="3" style="padding:20px;text-align:center;color:#94a3b8;font-size:13px;">No pending chats — all caught up! 🎉</td></tr>`;

  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>Samvaadik Daily Summary</title>
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
    Daily Activity Summary — ${date}
  </p>
</td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="background:#ffffff;padding:28px 32px;border-left:1px solid #e4e4eb;border-right:1px solid #e4e4eb;">

              <p style="margin:0 0 20px;font-size:14.5px;color:#374151;">
                Good morning, <strong>${username}</strong>! 👋<br/>
                Here's your WhatsApp activity for the last 24 hours.
              </p>

              <!-- Stats grid -->
              <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
                <tr>
                  <td width="25%" style="padding:4px;">
                    <div style="background:#f5f3ff;border:1px solid #ddd6fe;border-radius:12px;padding:16px;text-align:center;">
                      <div style="font-size:24px;font-weight:800;color:#7c3aed;margin-bottom:4px;">${stats.totalReceived}</div>
                      <div style="font-size:11.5px;color:#6b7280;font-weight:500;">Messages<br/>Received</div>
                    </div>
                  </td>
                  <td width="25%" style="padding:4px;">
                    <div style="background:#faf5ff;border:1px solid #e9d5ff;;border-radius:12px;padding:16px;text-align:center;">
                      <div style="font-size:24px;font-weight:800;color:#c2410c;margin-bottom:4px;">${stats.pendingCount}</div>
                      <div style="font-size:11.5px;color:#6b7280;font-weight:500;">Awaiting<br/>Reply</div>
                    </div>
                  </td>
                  <td width="25%" style="padding:4px;">
                    <div style="background:#f5f3ff;border:1px solid #ddd6fe;border-radius:12px;padding:16px;text-align:center;">
                      <div style="font-size:24px;font-weight:800;color:#1d4ed8;margin-bottom:4px;">${stats.botHandled}</div>
                      <div style="font-size:11.5px;color:#6b7280;font-weight:500;">Bot<br/>Handled</div>
                    </div>
                  </td>
                  <td width="25%" style="padding:4px;">
                    <div style="background:#faf5ff;border:1px solid #e9d5ff;border-radius:12px;padding:16px;text-align:center;">
                      <div style="font-size:24px;font-weight:800;color:#15803d;margin-bottom:4px;">${stats.newContacts}</div>
                      <div style="font-size:11.5px;color:#6b7280;font-weight:500;">New<br/>Contacts</div>
                    </div>
                  </td>
                </tr>
              </table>

              <!-- Pending chats table -->
              <h2 style="margin:0 0 12px;font-size:15px;font-weight:700;color:#0f1117;letter-spacing:-0.02em;">
               Chats Waiting for Your Reply
              </h2>

              <table width="100%" cellpadding="0" cellspacing="0"
                style="border:1px solid #e4e4eb;border-radius:10px;overflow:hidden;margin-bottom:28px;border-collapse:collapse;">
                <thead>
                  <tr style="background:#f5f3ff;">
                    <th style="padding:10px 14px;text-align:left;font-size:11.5px;font-weight:700;color:#6b7280;letter-spacing:0.04em;text-transform:uppercase;">Contact</th>
                    <th style="padding:10px 14px;text-align:left;font-size:11.5px;font-weight:700;color:#6b7280;letter-spacing:0.04em;text-transform:uppercase;">Last Message</th>
                    <th style="padding:10px 14px;text-align:left;font-size:11.5px;font-weight:700;color:#6b7280;letter-spacing:0.04em;text-transform:uppercase;">Time</th>
                  </tr>
                </thead>
                <tbody>
                  ${pendingRows}
                </tbody>
              </table>

              <!-- CTA -->
              <div style="text-align:center;margin-bottom:8px;">
                <a href="${process.env.FRONTEND_URL || "https://samvaadik.com"}/chat"
                  style="display:inline-block;background:linear-gradient(135deg,#7c3aed,#a855f7);color:#ffffff;font-size:14px;font-weight:700;padding:13px 32px;border-radius:9999px;text-decoration:none;letter-spacing:-0.01em;">
                  Open Chat Dashboard →
                </a>
              </div>

            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="background:#f5f4fb;border:1px solid #e4e4eb;border-top:none;border-radius:0 0 16px 16px;padding:20px 32px;text-align:center;">
              <p style="margin:8px 0 0;font-size:12px;color:#c4b5fd;">
                You're receiving this because you have an active Samvaadik account.<br/>
                This summary is sent every day at 8:00 AM IST.
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

// ─── Fetch stats for one user ─────────────────────────────────────────────────
async function fetchUserStats(user_id) {
  const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

  // 1. Total messages received in last 24h
  const { count: totalReceived } = await supabase
    .from("messages")
    .select("message_id", { count: "exact", head: true })
    .eq("sender_type", "user")
    .gte("created_at", since)
    .in(
      "chat_id",
      (
        await supabase.from("chats").select("chat_id").eq("user_id", user_id)
      ).data?.map((c) => c.chat_id) || [],
    );

  // 2. Chats where customer sent last message (pending reply)
  const { data: pendingChats } = await supabase
    .from("chats")
    .select("chat_id, person_name, phone_number, last_message, last_message_at")
    .eq("user_id", user_id)
    .eq("last_sender_type", "user")
    .order("last_message_at", { ascending: false })
    .limit(10);

  // 3. Bot handled conversations in last 24h
  const { count: botHandled } = await supabase
    .from("chatbot_sessions")
    .select("session_id", { count: "exact", head: true })
    .eq("status", "completed")
    .gte("updated_at", since)
    .in(
      "chat_id",
      (
        await supabase.from("chats").select("chat_id").eq("user_id", user_id)
      ).data?.map((c) => c.chat_id) || [],
    );

  // 4. New contacts created in last 24h
  const { count: newContacts } = await supabase
    .from("chats")
    .select("chat_id", { count: "exact", head: true })
    .eq("user_id", user_id)
    .gte("created_at", since);

  return {
    stats: {
      totalReceived: totalReceived || 0,
      pendingCount: pendingChats?.length || 0,
      botHandled: botHandled || 0,
      newContacts: newContacts || 0,
    },
    pendingChats: pendingChats || [],
  };
}

// ─── Send digest to one user ──────────────────────────────────────────────────
async function sendDigestToUser(user) {
  try {
    const { stats, pendingChats } = await fetchUserStats(user.user_id);

    // Skip if zero activity — no point sending empty email
    if (
      stats.totalReceived === 0 &&
      stats.pendingCount === 0 &&
      stats.newContacts === 0
    ) {
      console.log(`⏭️  [Digest] No activity for ${user.email} — skipping`);
      return;
    }

    const today = new Date().toLocaleDateString("en-IN", {
      weekday: "long",
      day: "numeric",
      month: "long",
      year: "numeric",
      timeZone: "Asia/Kolkata",
    });

    const username = user.email?.split("@")[0] || "there";

    const html = buildEmailHtml({
      username,
      email: user.email,
      stats,
      pendingChats,
      date: today,
    });

    const { error } = await resend.emails.send({
      from: "Samvaadik <noreply@samvaadik.com>", // use your verified domain
      to: [user.email],
      subject: `Your Samvaadik Summary — ${stats.pendingCount} chats waiting`,
      html,
    });

    if (error) {
      console.error(`❌ [Digest] Failed to send to ${user.email}:`, error);
    } else {
      console.log(`✅ [Digest] Email sent to ${user.email}`);
    }
  } catch (err) {
    console.error(`❌ [Digest] Error for ${user.email}:`, err.message);
  }
}

// ─── Main digest runner ───────────────────────────────────────────────────────
async function runDailyDigest() {
  console.log("📧 [Digest] Starting daily notification digest...");

  try {
    // Get all users who have at least one WhatsApp account connected
    const { data: users, error } = await supabase
      .from("users")
      .select("user_id, email")
      .not("email", "is", null);

    if (error) throw error;

    if (!users?.length) {
      console.log("⚠️  [Digest] No users found — skipping");
      return;
    }

    console.log(`📧 [Digest] Sending to ${users.length} users...`);

    // Send emails sequentially to avoid rate limits
    for (const user of users) {
      await sendDigestToUser(user);
      // Small delay between sends
      await new Promise((r) => setTimeout(r, 500));
    }

    console.log("✅ [Digest] Daily digest complete");
  } catch (err) {
    console.error("❌ [Digest] runDailyDigest error:", err.message);
  }
}

// ─── Export cron starter ──────────────────────────────────────────────────────
export function startDailyDigestCron() {
  // 8:00 AM IST = 2:30 AM UTC (IST is UTC+5:30)
  // Cron format: minute hour day month weekday
  cron.schedule(
    "30 2 * * *",
    async () => {
      console.log("⏰ [Digest] Cron triggered at 8:00 AM IST");
      await runDailyDigest();
    },
    {
      timezone: "UTC",
    },
  );

  console.log("✅ [Digest] Daily digest cron scheduled — runs at 8:00 AM IST");
}

// ─── Manual trigger (for testing) ────────────────────────────────────────────
export async function triggerDigestNow() {
  console.log("🔧 [Digest] Manual trigger — running now...");
  await runDailyDigest();
}
