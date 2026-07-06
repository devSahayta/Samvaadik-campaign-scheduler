// scheduler/campaignScheduler.js
//
// WHAT CHANGED vs the old version:
//   - checkAndSendCampaigns: groups campaigns by account_id, runs each group
//     in parallel via Promise.all instead of a sequential for...of loop
//   - processAccountCampaigns: new function that round-robins batches across
//     all campaigns on the same WhatsApp account so no campaign starves
//   - sendBatch: extracted helper so the batch loop is clean and reusable
//
// WHAT IS 100% IDENTICAL:
//   - BATCH_SIZE, BATCH_DELAY_MS, MESSAGE_DELAY_MIN/MAX constants
//   - sendWhatsAppMessage — every line unchanged
//   - findOrCreateChat — every line unchanged
//   - buildTemplateMessage, extractTemplateButtons — every line unchanged
//   - markCampaignCompleted — every line unchanged
//   - updateWarmupProgress — every line unchanged
//   - updateTierDailySent — every line unchanged
//   - All Supabase writes (campaign_messages, whatsapp_messages, messages, chats)
//   - Timeout protection (30 min guard on started_at)
//   - Daily counter reset logic
//   - isProcessing lock

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";
import axios from "axios";

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error("Missing Supabase credentials in environment variables");
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

let isProcessing = false;

// Sending rate constants (unchanged from original)
const BATCH_SIZE = 5;
const BATCH_DELAY_MS = 10000;
const MESSAGE_DELAY_MIN = 800;
const MESSAGE_DELAY_MAX = 2000;

/* ============================================================
   ENTRY POINT
   ============================================================ */

export function startCampaignScheduler() {
  console.log("Campaign Scheduler (parallel-by-account) started!");
  console.log(
    `Rate: ${BATCH_SIZE} msgs / ${BATCH_DELAY_MS / 1000}s per account`,
  );
  console.log(`Per-message delay: ${MESSAGE_DELAY_MIN}-${MESSAGE_DELAY_MAX}ms`);

  cron.schedule("* * * * *", async () => {
    if (isProcessing) {
      console.log("Skipping tick - previous run still active");
      return;
    }
    isProcessing = true;
    try {
      await checkAndSendCampaigns();
    } catch (err) {
      console.error("Scheduler top-level error:", err);
    } finally {
      isProcessing = false;
    }
  });
}

/* ============================================================
   STEP 1 - FIND DUE CAMPAIGNS, GROUP BY ACCOUNT, RUN PARALLEL
   ============================================================ */

export async function checkAndSendCampaigns() {
  const now = new Date();
  console.log(`\n[${now.toISOString()}] Checking for due campaigns...`);

  const { data: campaigns, error } = await supabase
    .from("campaigns")
    .select("*")
    .eq("status", "scheduled")
    .lte("scheduled_at", now.toISOString())
    .order("scheduled_at", { ascending: true });

  if (error) {
    console.error("Error fetching campaigns:", error);
    return;
  }

  if (!campaigns || campaigns.length === 0) {
    console.log("No campaigns ready to send");
    return;
  }

  console.log(`${campaigns.length} campaign(s) ready to send`);

  // Group by account_id
  const byAccount = new Map();
  for (const c of campaigns) {
    if (!byAccount.has(c.account_id)) byAccount.set(c.account_id, []);
    byAccount.get(c.account_id).push(c);
  }

  console.log(`Grouped into ${byAccount.size} WhatsApp account(s)`);

  // Run each account group in parallel
  await Promise.all(
    [...byAccount.entries()].map(([accountId, accountCampaigns]) =>
      processAccountCampaigns(accountId, accountCampaigns).catch((err) =>
        console.error(`Account ${accountId} processing failed:`, err.message),
      ),
    ),
  );
}

/* ============================================================
   STEP 2 - PROCESS ALL CAMPAIGNS FOR ONE ACCOUNT
   ============================================================ */

async function processAccountCampaigns(accountId, campaigns) {
  console.log(`\n${"=".repeat(56)}`);
  console.log(`Account ${accountId} - ${campaigns.length} campaign(s)`);
  console.log(`${"=".repeat(56)}`);

  // Load the WhatsApp account
  const { data: account, error: accErr } = await supabase
    .from("whatsapp_accounts")
    .select("*")
    .eq("wa_id", accountId)
    .single();

  if (accErr || !account) {
    console.error(`WhatsApp account ${accountId} not found - skipping`);
    return;
  }

  console.log(`Phone: ${account.business_phone_number}`);
  console.log(`Tier: ${account.messaging_limit_tier}`);

  // Timeout protection: same 30-min logic as original
  const safeCampaigns = [];
  for (const c of campaigns) {
    if (c.started_at) {
      const minutesRunning = (Date.now() - new Date(c.started_at)) / 60000;
      if (minutesRunning > 30) {
        console.warn(
          `Campaign "${c.campaign_name}" timed out (${minutesRunning.toFixed(0)} min) - marking failed`,
        );
        await supabase
          .from("campaigns")
          .update({ status: "failed", completed_at: new Date().toISOString() })
          .eq("campaign_id", c.campaign_id);
        continue;
      }
    }
    safeCampaigns.push(c);
  }

  if (safeCampaigns.length === 0) return;

  // Maybe reset daily counters if it is a new day
  await maybeResetDailyCounters(account);

  // Reload fresh account state after potential reset
  const { data: freshAccount } = await supabase
    .from("whatsapp_accounts")
    .select("*")
    .eq("wa_id", accountId)
    .single();
  const acct = freshAccount || account;

  // Mark all campaigns processing
  await Promise.all(
    safeCampaigns.map((c) =>
      supabase
        .from("campaigns")
        .update({
          status: "processing",
          started_at: c.started_at || new Date().toISOString(),
        })
        .eq("campaign_id", c.campaign_id)
        .eq("status", "scheduled"),
    ),
  );

  // Build a queue entry per campaign
  const queues = [];

  for (const campaign of safeCampaigns) {
    const { data: template, error: tErr } = await supabase
      .from("whatsapp_templates")
      .select("*")
      .eq("wt_id", campaign.wt_id)
      .single();

    if (tErr || !template) {
      console.error(
        `Template not found for campaign "${campaign.campaign_name}" - marking failed`,
      );
      await supabase
        .from("campaigns")
        .update({ status: "failed", completed_at: new Date().toISOString() })
        .eq("campaign_id", campaign.campaign_id);
      continue;
    }

    const warmupLimit = getWarmupMessageLimit(acct);
    const pending = await fetchPendingMessages(
      campaign.campaign_id,
      warmupLimit,
    );

    if (pending.length === 0) {
      console.log(`"${campaign.campaign_name}": no pending messages`);
      await finalizeCampaign(campaign, acct, 0, 0, false);
      continue;
    }

    console.log(
      `"${campaign.campaign_name}": ${pending.length} messages queued` +
        (warmupLimit !== null ? ` (warm-up cap: ${warmupLimit})` : ""),
    );

    queues.push({ campaign, template, pending, sent: 0, failed: 0 });
  }

  if (queues.length === 0) return;

  // ROUND-ROBIN across all campaigns on this account
  let roundIndex = 0;
  let consecutiveEmptyRounds = 0;

  while (true) {
    const anyRemaining = queues.some((q) => q.pending.length > 0);
    if (!anyRemaining) break;

    const queue = queues[roundIndex % queues.length];
    roundIndex++;

    if (queue.pending.length === 0) {
      consecutiveEmptyRounds++;
      if (consecutiveEmptyRounds >= queues.length) break;
      continue;
    }
    consecutiveEmptyRounds = 0;

    const batch = queue.pending.splice(0, BATCH_SIZE);

    console.log(
      `\n[${queue.campaign.campaign_name}] batch of ${batch.length} - ${queue.pending.length} remain`,
    );

    const { sent, failed } = await sendBatch(
      acct,
      queue.template,
      batch,
      queue.campaign,
    );
    queue.sent += sent;
    queue.failed += failed;

    // Batch delay only when there is more work to do
    const stillHasWork = queues.some((q) => q.pending.length > 0);
    if (stillHasWork) {
      console.log(`Batch delay ${BATCH_DELAY_MS / 1000}s...`);
      await sleep(BATCH_DELAY_MS);
    }
  }

  // Update warm-up and tier counters
  const totalSentThisTick = queues.reduce((sum, q) => sum + q.sent, 0);

  if (totalSentThisTick > 0) {
    if (acct.warmup_enabled && !acct.warmup_completed) {
      await updateWarmupProgress(acct.wa_id, totalSentThisTick, acct);
    } else if (acct.warmup_completed) {
      await updateTierDailySent(acct.wa_id, totalSentThisTick, acct);
    }
  }

  // Finalize each campaign
  for (const queue of queues) {
    const wasLimited = queue.pending.length > 0;
    await finalizeCampaign(
      queue.campaign,
      acct,
      queue.sent,
      queue.failed,
      wasLimited,
    );
  }

  console.log(
    `\nAccount ${accountId} tick complete - sent ${totalSentThisTick} across ${queues.length} campaign(s)`,
  );
}

/* ============================================================
   SEND A BATCH (identical behaviour to original batch loop)
   ============================================================ */

async function sendBatch(account, template, messages, campaign) {
  let sent = 0;
  let failed = 0;

  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];

    try {
      const result = await sendWhatsAppMessage(
        account,
        template,
        message.phone_number,
        message.contact_name,
        campaign.group_id,
        campaign.user_id,
        campaign.template_variables,
        campaign.media_id,
      );

      await supabase
        .from("campaign_messages")
        .update({
          status: "sent",
          sent_at: new Date().toISOString(),
          wm_id: result.wm_id,
          wa_message_id: result.wa_message_id,
        })
        .eq("cm_id", message.cm_id);

      sent++;
      console.log(`  OK ${message.phone_number}`);
    } catch (err) {
      await supabase
        .from("campaign_messages")
        .update({
          status: "failed",
          failed_at: new Date().toISOString(),
          error_message: err.message || "Unknown error",
          error_code: err.code || "SEND_ERROR",
        })
        .eq("cm_id", message.cm_id);

      failed++;
      console.log(`  FAIL ${message.phone_number}: ${err.message}`);
    }

    // Per-message random delay (skip after last message in batch)
    if (i < messages.length - 1) {
      await sleep(randomDelay(MESSAGE_DELAY_MIN, MESSAGE_DELAY_MAX));
    }
  }

  return { sent, failed };
}

/* ============================================================
   FINALIZE OR PAUSE A CAMPAIGN
   ============================================================ */

async function finalizeCampaign(
  campaign,
  account,
  sentThisTick,
  failedThisTick,
  wasLimited,
) {
  const totalSent = (campaign.messages_sent || 0) + sentThisTick;
  const totalFailed = (campaign.messages_failed || 0) + failedThisTick;

  if (wasLimited) {
    await supabase
      .from("campaigns")
      .update({
        status: "scheduled",
        messages_sent: totalSent,
        messages_failed: totalFailed,
        updated_at: new Date().toISOString(),
      })
      .eq("campaign_id", campaign.campaign_id);
    console.log(`"${campaign.campaign_name}" paused - will resume next tick`);
    return;
  }

  const { count: pendingLeft } = await supabase
    .from("campaign_messages")
    .select("cm_id", { count: "exact", head: true })
    .eq("campaign_id", campaign.campaign_id)
    .eq("status", "pending");

  if ((pendingLeft || 0) > 0) {
    await supabase
      .from("campaigns")
      .update({
        status: "scheduled",
        messages_sent: totalSent,
        messages_failed: totalFailed,
        updated_at: new Date().toISOString(),
      })
      .eq("campaign_id", campaign.campaign_id);
    console.log(`"${campaign.campaign_name}" paused - ${pendingLeft} remain`);
  } else {
    await markCampaignCompleted(campaign.campaign_id, totalSent, totalFailed);
    console.log(
      `"${campaign.campaign_name}" completed - sent ${totalSent} / failed ${totalFailed}`,
    );
  }
}

/* ============================================================
   FETCH PENDING MESSAGES (paginated)
   ============================================================ */

async function fetchPendingMessages(campaignId, limit = null) {
  let allMessages = [];
  let page = 0;
  const pageSize = 1000;

  while (true) {
    const canFetch =
      limit !== null
        ? Math.min(pageSize, limit - allMessages.length)
        : pageSize;

    if (canFetch <= 0) break;

    const { data: messages, error } = await supabase
      .from("campaign_messages")
      .select("*")
      .eq("campaign_id", campaignId)
      .eq("status", "pending")
      .range(page * pageSize, page * pageSize + canFetch - 1);

    if (error) throw error;
    if (!messages || messages.length === 0) break;

    allMessages = allMessages.concat(messages);
    if (messages.length < canFetch) break;
    page++;
  }

  return allMessages;
}

/* ============================================================
   WARM-UP HELPERS
   ============================================================ */

function getWarmupMessageLimit(account) {
  if (!account.warmup_enabled || account.warmup_completed) return null;
  const limits = account.warmup_limits || [200, 500, 1000];
  const stage = account.warmup_stage || 1;
  const stageLimit = limits[stage - 1];
  const stageProgress = account.warmup_stage_progress || 0;
  const remaining = stageLimit - stageProgress;
  return remaining > 0 ? remaining : 0;
}

async function maybeResetDailyCounters(account) {
  const today = new Date().toISOString().split("T")[0];
  const warmupDay = account.warmup_daily_reset_at
    ? new Date(account.warmup_daily_reset_at).toISOString().split("T")[0]
    : null;
  const tierDay = account.tier_daily_reset_at
    ? new Date(account.tier_daily_reset_at).toISOString().split("T")[0]
    : null;

  const updates = {};
  if (warmupDay !== today) {
    updates.warmup_daily_sent = 0;
    updates.warmup_daily_reset_at = new Date().toISOString();
  }
  if (tierDay !== today) {
    updates.tier_daily_sent = 0;
    updates.tier_daily_reset_at = new Date().toISOString();
  }
  if (Object.keys(updates).length > 0) {
    await supabase
      .from("whatsapp_accounts")
      .update(updates)
      .eq("wa_id", account.wa_id);
  }
}

async function updateWarmupProgress(accountId, messagesSent, account) {
  try {
    function shouldResetDailyCounter(last_reset) {
      if (!last_reset) return true;
      const now = new Date();
      const lastReset = new Date(last_reset);
      return (
        now.toISOString().split("T")[0] !==
        lastReset.toISOString().split("T")[0]
      );
    }

    let current_daily_sent = account.warmup_daily_sent || 0;
    if (shouldResetDailyCounter(account.warmup_daily_reset_at)) {
      current_daily_sent = 0;
    }

    const new_daily_sent = current_daily_sent + messagesSent;
    const warmup_limits = account.warmup_limits || [200, 500, 1000];
    const current_stage = account.warmup_stage || 1;
    const current_progress =
      (account.warmup_stage_progress || 0) + messagesSent;
    const current_limit = warmup_limits[current_stage - 1];

    console.log(
      `Warm-up Stage ${current_stage}: ${current_progress}/${current_limit}`,
    );
    console.log(`Daily: ${new_daily_sent}/${current_limit}`);

    if (current_progress >= current_limit) {
      if (current_stage < warmup_limits.length) {
        await supabase
          .from("whatsapp_accounts")
          .update({
            warmup_stage: current_stage + 1,
            warmup_stage_progress: 0,
            warmup_daily_sent: new_daily_sent,
            warmup_daily_reset_at: new Date().toISOString(),
            warmup_last_updated_at: new Date().toISOString(),
          })
          .eq("wa_id", accountId);
        console.log(
          `Stage ${current_stage} complete - advanced to Stage ${current_stage + 1}`,
        );
      } else {
        await supabase
          .from("whatsapp_accounts")
          .update({
            warmup_completed: true,
            warmup_daily_sent: new_daily_sent,
            warmup_daily_reset_at: new Date().toISOString(),
            warmup_last_updated_at: new Date().toISOString(),
          })
          .eq("wa_id", accountId);
        console.log(
          "WARM-UP COMPLETED - account can now send at full capacity!",
        );
      }
    } else {
      await supabase
        .from("whatsapp_accounts")
        .update({
          warmup_stage_progress: current_progress,
          warmup_daily_sent: new_daily_sent,
          warmup_daily_reset_at: new Date().toISOString(),
          warmup_last_updated_at: new Date().toISOString(),
        })
        .eq("wa_id", accountId);

      const stage_remaining = current_limit - current_progress;
      const daily_remaining = current_limit - new_daily_sent;
      console.log(
        `${stage_remaining} to next stage | ${daily_remaining} remaining today`,
      );
    }
  } catch (err) {
    console.error("Error updating warm-up progress:", err.message);
  }
}

async function updateTierDailySent(accountId, messagesSent, account) {
  try {
    const now = new Date();
    const tierDailySent = account.tier_daily_sent || 0;
    const lastResetDate = account.tier_daily_reset_at
      ? new Date(account.tier_daily_reset_at).toISOString().split("T")[0]
      : null;
    const todayDate = now.toISOString().split("T")[0];

    const newTierDailySent =
      !lastResetDate || lastResetDate !== todayDate
        ? messagesSent
        : tierDailySent + messagesSent;

    await supabase
      .from("whatsapp_accounts")
      .update({
        tier_daily_sent: newTierDailySent,
        tier_daily_reset_at: now.toISOString(),
      })
      .eq("wa_id", accountId);

    console.log(
      `Tier daily sent: ${newTierDailySent}/${account.messaging_limit_per_day}`,
    );
  } catch (err) {
    console.error("Error in updateTierDailySent:", err.message);
  }
}

/* ============================================================
   MARK CAMPAIGN COMPLETED (unchanged)
   ============================================================ */

async function markCampaignCompleted(campaignId, sent, failed) {
  try {
    await supabase
      .from("campaigns")
      .update({
        status: "completed",
        completed_at: new Date().toISOString(),
        messages_sent: sent,
        messages_failed: failed,
      })
      .eq("campaign_id", campaignId);
  } catch (err) {
    console.error("Error marking campaign completed:", err.message);
  }
}

/* ============================================================
   SEND WHATSAPP MESSAGE (100% identical to original)
   ============================================================ */

async function sendWhatsAppMessage(
  account,
  template,
  phoneNumber,
  contactName,
  groupId,
  userId,
  variables,
  campaignMediaId,
) {
  try {
    let templateComponents = template.components;
    if (typeof templateComponents === "string") {
      try {
        templateComponents = JSON.parse(templateComponents);
      } catch (e) {
        templateComponents = [];
      }
    }

    const messageBody = {
      messaging_product: "whatsapp",
      to: phoneNumber,
      type: "template",
      template: {
        name: template.name,
        language: { code: template.language },
        components: [],
      },
    };

    const headerComponent = templateComponents.find(
      (comp) => comp.type === "HEADER",
    );
    if (headerComponent && headerComponent.format) {
      const format = headerComponent.format.toUpperCase();
      if (["IMAGE", "VIDEO", "DOCUMENT"].includes(format)) {
        if (!campaignMediaId) {
          throw new Error(
            `Media ${format} template requires media_id but campaign has none selected`,
          );
        }
        messageBody.template.components.push({
          type: "header",
          parameters: [
            {
              type: format.toLowerCase(),
              [format.toLowerCase()]: { id: campaignMediaId },
            },
          ],
        });
      } else if (format === "TEXT" && headerComponent.example) {
        const headerText = headerComponent.example.header_text || [];
        if (headerText.length > 0) {
          messageBody.template.components.push({
            type: "header",
            parameters: headerText.map((text) => ({ type: "text", text })),
          });
        }
      }
    }

    let parsedVariables = variables;
    if (typeof variables === "string") {
      try {
        parsedVariables = JSON.parse(variables);
      } catch (e) {
        parsedVariables = {};
      }
    }
    if (parsedVariables && Object.keys(parsedVariables).length > 0) {
      messageBody.template.components.push({
        type: "body",
        parameters: Object.values(parsedVariables).map((value) => ({
          type: "text",
          text: String(value),
        })),
      });
    }

    const response = await axios.post(
      `https://graph.facebook.com/v21.0/${account.phone_number_id}/messages`,
      messageBody,
      {
        headers: {
          Authorization: `Bearer ${account.system_user_access_token}`,
          "Content-Type": "application/json",
        },
        timeout: 30000,
      },
    );

    const wa_message_id = response.data.messages?.[0]?.id;
    const templateText = buildTemplateMessage(template);
    const templateButtons = extractTemplateButtons(template);

    let components = template.components;
    if (typeof components === "string") {
      try {
        components = JSON.parse(components);
      } catch {
        components = [];
      }
    }
    const headerComp = Array.isArray(components)
      ? components.find((c) => c.type === "HEADER")
      : null;
    const headerFormat = headerComp?.format?.toUpperCase();
    const messageType =
      headerFormat === "VIDEO"
        ? "template_video"
        : headerFormat === "DOCUMENT"
          ? "template_document"
          : "template";

    const { data: wmRecord, error: wmError } = await supabase
      .from("whatsapp_messages")
      .insert({
        account_id: account.wa_id,
        to_number: phoneNumber,
        template_name: template.name,
        message_body: JSON.stringify(messageBody),
        wa_message_id,
        status: "sent",
        sent_at: new Date().toISOString(),
      })
      .select()
      .single();

    if (wmError) {
      console.error("Failed to store in whatsapp_messages:", wmError);
    }

    const chatId = await findOrCreateChat(
      phoneNumber,
      contactName,
      groupId,
      userId,
      templateText,
      campaignMediaId,
    );

    await supabase.from("messages").insert({
      chat_id: chatId,
      sender_type: "admin",
      message: templateText,
      message_type: messageType,
      media_path: campaignMediaId || null,
      buttons: templateButtons,
      wm_id: wmRecord?.wm_id,
      created_at: new Date().toISOString(),
    });

    return { wm_id: wmRecord?.wm_id, wa_message_id, chat_id: chatId };
  } catch (err) {
    const errorMessage =
      err.response?.data?.error?.message ||
      err.message ||
      "Failed to send message";
    throw new Error(errorMessage);
  }
}

/* ============================================================
   FIND OR CREATE CHAT (100% identical to original)
   ============================================================ */

async function findOrCreateChat(
  phoneNumber,
  contactName,
  groupId,
  userId,
  lastMessage = "Template message sent via campaign",
  mediaId = null,
) {
  try {
    const { data: existingChats } = await supabase
      .from("chats")
      .select("chat_id, group_id, person_name")
      .eq("phone_number", phoneNumber)
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(1);

    if (existingChats && existingChats.length > 0) {
      const existingChat = existingChats[0];
      await supabase
        .from("chats")
        .update({
          last_message: lastMessage,
          last_message_at: new Date().toISOString(),
          last_admin_message_at: new Date().toISOString(),
          last_sender_type: "admin",
          group_id: groupId,
          person_name: contactName || existingChat.person_name || "Unknown",
          updated_at: new Date().toISOString(),
        })
        .eq("chat_id", existingChat.chat_id);
      return existingChat.chat_id;
    }

    const { data: newChat, error } = await supabase
      .from("chats")
      .insert({
        phone_number: phoneNumber,
        person_name: contactName || "Unknown",
        last_message: lastMessage,
        last_message_at: new Date().toISOString(),
        last_sender_type: "admin",
        group_id: groupId,
        mode: "AUTO",
        last_admin_message_at: new Date().toISOString(),
        user_id: userId,
        status: "active",
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .select()
      .single();

    if (error) throw error;
    return newChat.chat_id;
  } catch (err) {
    console.error("Error in findOrCreateChat:", err.message);
    return null;
  }
}

/* ============================================================
   BUILD TEMPLATE MESSAGE (100% identical to original)
   ============================================================ */

function buildTemplateMessage(template) {
  try {
    let components = template.components;
    if (typeof components === "string") {
      try {
        components = JSON.parse(components);
      } catch {
        return `Template: ${template.name}`;
      }
    }
    if (!Array.isArray(components)) return `Template: ${template.name}`;

    const parts = [];
    const header = components.find((c) => c.type === "HEADER");
    if (header?.format === "TEXT" && header?.text)
      parts.push(`**${header.text}**`);
    const body = components.find((c) => c.type === "BODY");
    if (body?.text) parts.push(body.text);
    const footer = components.find((c) => c.type === "FOOTER");
    if (footer?.text) parts.push(`_${footer.text}_`);
    return parts.length > 0 ? parts.join("\n\n") : `Template: ${template.name}`;
  } catch {
    return `Template: ${template.name}`;
  }
}

/* ============================================================
   EXTRACT TEMPLATE BUTTONS (100% identical to original)
   ============================================================ */

function extractTemplateButtons(template) {
  try {
    let components = template.components;
    if (typeof components === "string") {
      try {
        components = JSON.parse(components);
      } catch {
        return null;
      }
    }
    if (!Array.isArray(components)) return null;
    const btnComp = components.find((c) => c.type === "BUTTONS");
    if (!btnComp?.buttons?.length) return null;
    return JSON.stringify(
      btnComp.buttons.map((b) => ({
        type: b.type,
        text: b.text,
        ...(b.url ? { url: b.url } : {}),
        ...(b.phone_number ? { phone_number: b.phone_number } : {}),
      })),
    );
  } catch {
    return null;
  }
}

/* ============================================================
   UTILITIES
   ============================================================ */

function randomDelay(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export default { startCampaignScheduler, checkAndSendCampaigns };
