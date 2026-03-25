// scheduler/campaignScheduler.js
// UPDATED WITH WARM-UP TRACKING + INDUSTRY-STANDARD RATES
// Keeps all existing functionality, adds warm-up progress tracking

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";
import axios from "axios";

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error("❌ Missing Supabase credentials in environment variables");
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

let isProcessing = false;

// ✅ INDUSTRY-STANDARD SENDING RATES
// WhatsApp best practices: 20-40 messages/minute to avoid rate limits
const BATCH_SIZE = 5; // Send 5 messages in parallel (reduced from 10)
const BATCH_DELAY = 10000; // 6 seconds between batches (increased from 2s)
// = ~50 messages/minute (industry standard)

// ✅ PER-MESSAGE RANDOM DELAY (human-like sending pattern)
// Adds organic spacing between each message within a batch
// Helps improve delivery rates by avoiding burst detection
const MESSAGE_DELAY_MIN = 800; // Minimum delay between messages (ms)
const MESSAGE_DELAY_MAX = 2000; // Maximum delay between messages (ms)

/* =====================================
   MAIN SCHEDULER FUNCTION
====================================== */

export function startCampaignScheduler() {
  console.log("🚀 Campaign Scheduler Started!");
  console.log("⏰ Cron will run every minute...");
  console.log(
    `📊 Sending rate: ${BATCH_SIZE} messages every ${BATCH_DELAY / 1000}s (~${Math.round((BATCH_SIZE * 60) / (BATCH_DELAY / 1000))} msg/min)`,
  );
  console.log(
    `⏱️  Per-message delay: ${MESSAGE_DELAY_MIN}ms – ${MESSAGE_DELAY_MAX}ms (randomized)`,
  );

  // Run every minute
  cron.schedule("* * * * *", async () => {
    if (isProcessing) {
      console.log("⏭️  Skipping: Previous execution still running");
      return;
    }

    try {
      isProcessing = true;
      await checkAndSendCampaigns();
    } catch (err) {
      console.error("❌ Scheduler error:", err);
    } finally {
      isProcessing = false;
    }
  });
}

/* =====================================
   CHECK FOR SCHEDULED CAMPAIGNS
====================================== */

export async function checkAndSendCampaigns() {
  const now = new Date();
  console.log(`\n🔍 [${now.toISOString()}] Checking for campaigns...`);

  try {
    const { data: campaigns, error } = await supabase
      .from("campaigns")
      .select("*")
      .eq("status", "scheduled")
      .lte("scheduled_at", now.toISOString())
      .order("scheduled_at", { ascending: true })
      .limit(5);

    if (error) {
      console.error("❌ Error fetching campaigns:", error);
      return;
    }

    if (!campaigns || campaigns.length === 0) {
      console.log("✅ No campaigns ready to send");
      return;
    }

    console.log(`\n📢 Found ${campaigns.length} campaign(s) ready to send!`);

    // Process each campaign
    for (const campaign of campaigns) {
      await processCampaign(campaign);
    }
  } catch (err) {
    console.error("❌ Error in checkAndSendCampaigns:", err);
  }
}

/* =====================================
   PROCESS SINGLE CAMPAIGN
====================================== */

async function processCampaign(campaign) {
  const startTime = Date.now();

  console.log(`\n${"═".repeat(60)}`);
  console.log(`📤 PROCESSING CAMPAIGN`);
  console.log(`${"═".repeat(60)}`);
  console.log(`Name: ${campaign.campaign_name}`);
  console.log(`ID: ${campaign.campaign_id}`);
  console.log(`Scheduled: ${campaign.scheduled_at}`);
  console.log(`Recipients: ${campaign.total_recipients}`);
  if (campaign.warmup_stage) {
    console.log(`🔥 Warm-up Stage: ${campaign.warmup_stage}`);
  }
  if (campaign.media_id) {
    console.log(`Media ID: ${campaign.media_id}`);
  }
  console.log(`${"═".repeat(60)}\n`);

  try {
    // ✅ TIMEOUT PROTECTION
    if (campaign.started_at) {
      const startedAt = new Date(campaign.started_at);
      const now = new Date();
      const minutesRunning = (now - startedAt) / 1000 / 60;

      if (minutesRunning > 30) {
        console.log(
          `⚠️  Campaign timeout! Running for ${minutesRunning.toFixed(1)} minutes`,
        );

        await supabase
          .from("campaigns")
          .update({
            status: "failed",
            completed_at: new Date().toISOString(),
          })
          .eq("campaign_id", campaign.campaign_id);

        console.log("❌ Campaign marked as failed (timeout)");
        return;
      }
    }

    // 1️⃣ Update status to processing
    console.log("📝 Updating campaign status to PROCESSING...");
    const { error: updateError } = await supabase
      .from("campaigns")
      .update({
        status: "processing",
        started_at: campaign.started_at || new Date().toISOString(),
      })
      .eq("campaign_id", campaign.campaign_id);

    if (updateError) {
      console.error("❌ Failed to update status:", updateError);
      throw updateError;
    }

    console.log("✅ Status updated to PROCESSING");

    // 2️⃣ Get WhatsApp account (needed for warm-up check)
    console.log("🔍 Fetching WhatsApp account...");
    const { data: account, error: accountError } = await supabase
      .from("whatsapp_accounts")
      .select("*")
      .eq("wa_id", campaign.account_id)
      .single();

    if (accountError || !account) {
      console.error("❌ WhatsApp account not found:", accountError);
      throw new Error("WhatsApp account not found");
    }

    console.log(`✅ Account: ${account.business_phone_number}`);
    console.log(`   Tier: ${account.messaging_limit_tier}`);

    // ✅ CHECK WARM-UP STATUS
    if (account.warmup_enabled && !account.warmup_completed) {
      const warmup_limits = account.warmup_limits || [200, 500, 1000];
      const current_stage = account.warmup_stage || 1;
      const current_limit = warmup_limits[current_stage - 1];
      const current_progress = account.warmup_stage_progress || 0;
      const remaining_in_stage = current_limit - current_progress;

      console.log(`🔥 WARM-UP ACTIVE:`);
      console.log(`   Stage: ${current_stage}/${warmup_limits.length}`);
      console.log(`   Limit: ${current_limit} messages/stage`);
      console.log(`   Progress: ${current_progress}/${current_limit}`);
      console.log(`   Remaining: ${remaining_in_stage}`);
    } else if (account.warmup_completed) {
      console.log(`✅ Warm-up completed - sending at full capacity`);
    } else {
      console.log(`⏭️  No warm-up required for this tier`);
    }

    // 3️⃣ Get total pending count
    console.log("\n📊 Counting pending messages...");
    const { count: totalPending, error: countError } = await supabase
      .from("campaign_messages")
      .select("cm_id", { count: "exact", head: true })
      .eq("campaign_id", campaign.campaign_id)
      .eq("status", "pending");

    if (countError) {
      console.error("❌ Failed to count messages:", countError);
      throw countError;
    }

    console.log(`✅ Found ${totalPending} pending messages`);

    if (totalPending === 0) {
      console.log("⚠️  No pending messages - marking campaign as completed");

      const { count: sentCount } = await supabase
        .from("campaign_messages")
        .select("cm_id", { count: "exact", head: true })
        .eq("campaign_id", campaign.campaign_id)
        .eq("status", "sent");

      const { count: failedCount } = await supabase
        .from("campaign_messages")
        .select("cm_id", { count: "exact", head: true })
        .eq("campaign_id", campaign.campaign_id)
        .eq("status", "failed");

      await markCampaignCompleted(
        campaign.campaign_id,
        sentCount || 0,
        failedCount || 0,
      );
      return;
    }

    // ✅ WARM-UP LIMIT CHECK
    // Determine how many messages to send in this run
    let messagesToSendCount = totalPending;
    let warmupLimited = false;

    if (account.warmup_enabled && !account.warmup_completed) {
      const warmup_limits = account.warmup_limits || [200, 500, 1000];
      const current_stage = account.warmup_stage || 1;
      const current_limit = warmup_limits[current_stage - 1];
      const current_progress = account.warmup_stage_progress || 0;
      const remaining_in_stage = current_limit - current_progress;

      if (totalPending > remaining_in_stage) {
        console.log(
          `\n🔥 WARM-UP LIMIT: Can only send ${remaining_in_stage} of ${totalPending} messages`,
        );
        messagesToSendCount = remaining_in_stage;
        warmupLimited = true;
      }
    }

    // 4️⃣ Fetch messages to send (with warm-up limit)
    console.log(`\n📥 Fetching ${messagesToSendCount} messages to send...`);
    let allMessages = [];
    let page = 0;
    const pageSize = 1000;

    while (allMessages.length < messagesToSendCount) {
      const { data: messages, error: msgError } = await supabase
        .from("campaign_messages")
        .select("*")
        .eq("campaign_id", campaign.campaign_id)
        .eq("status", "pending")
        .range(page * pageSize, (page + 1) * pageSize - 1);

      if (msgError) {
        console.error("❌ Error fetching messages:", msgError);
        throw msgError;
      }

      if (!messages || messages.length === 0) break;

      allMessages = allMessages.concat(messages);
      console.log(
        `   📄 Page ${page + 1}: ${messages.length} messages (Total: ${allMessages.length})`,
      );

      if (allMessages.length >= messagesToSendCount) {
        allMessages = allMessages.slice(0, messagesToSendCount);
        break;
      }

      if (messages.length < pageSize) break;
      page++;
    }

    console.log(`✅ Will send ${allMessages.length} messages this run`);
    if (warmupLimited) {
      console.log(
        `⏸️  Remaining ${totalPending - allMessages.length} messages will be sent after warm-up stage completes`,
      );
    }

    // 5️⃣ Get template
    console.log("\n🔍 Fetching template...");
    const { data: template, error: templateError } = await supabase
      .from("whatsapp_templates")
      .select("*")
      .eq("wt_id", campaign.wt_id)
      .single();

    if (templateError || !template) {
      console.error("❌ Template not found:", templateError);
      throw new Error("Template not found");
    }

    console.log(`✅ Template: ${template.name} (${template.language})`);

    // 6️⃣ Send messages in BATCHES with PER-MESSAGE RANDOM DELAY
    console.log(`\n📤 Starting batch sending...`);
    let sent = 0;
    let failed = 0;

    const totalBatches = Math.ceil(allMessages.length / BATCH_SIZE);
    const avgMsgDelay = (MESSAGE_DELAY_MIN + MESSAGE_DELAY_MAX) / 2;
    const estimatedTimePerBatch =
      ((BATCH_SIZE - 1) * avgMsgDelay + BATCH_DELAY) / 1000;

    console.log(`   Total batches: ${totalBatches}`);
    console.log(`   Batch size: ${BATCH_SIZE} messages`);
    console.log(
      `   Per-message delay: ${MESSAGE_DELAY_MIN}ms – ${MESSAGE_DELAY_MAX}ms (random)`,
    );
    console.log(`   Batch delay: ${BATCH_DELAY}ms after each batch`);
    console.log(
      `   Est. time per batch: ~${estimatedTimePerBatch.toFixed(1)}s\n`,
    );

    for (let i = 0; i < allMessages.length; i += BATCH_SIZE) {
      const batch = allMessages.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;

      console.log(
        `📦 Batch ${batchNumber}/${totalBatches} (${batch.length} messages)...`,
      );

      // ✅ Send messages SEQUENTIALLY within the batch with per-message random delay
      // (Changed from Promise.allSettled to sequential for organic human-like pacing)
      const results = [];
      for (let k = 0; k < batch.length; k++) {
        const message = batch[k];

        // Send the message and capture result
        try {
          const value = await sendWhatsAppMessage(
            account,
            template,
            message.phone_number,
            message.contact_name,
            campaign.group_id,
            campaign.user_id,
            campaign.template_variables,
            campaign.media_id,
          );
          results.push({ status: "fulfilled", value });
        } catch (err) {
          results.push({ status: "rejected", reason: err });
        }

        // ✅ Per-message random delay (skip after the last message in the batch)
        if (k < batch.length - 1) {
          const msgDelay = randomDelay(MESSAGE_DELAY_MIN, MESSAGE_DELAY_MAX);
          console.log(`   ⏱️  Message delay: ${msgDelay}ms`);
          await sleep(msgDelay);
        }
      }

      // Process results and update database
      for (let j = 0; j < results.length; j++) {
        const result = results[j];
        const message = batch[j];

        if (result.status === "fulfilled") {
          // Success
          await supabase
            .from("campaign_messages")
            .update({
              status: "sent",
              sent_at: new Date().toISOString(),
              wm_id: result.value.wm_id,
              wa_message_id: result.value.wa_message_id,
            })
            .eq("cm_id", message.cm_id);

          sent++;
          console.log(`   ✅ ${message.phone_number}`);
        } else {
          // Failed
          await supabase
            .from("campaign_messages")
            .update({
              status: "failed",
              failed_at: new Date().toISOString(),
              error_message: result.reason?.message || "Unknown error",
              error_code: result.reason?.code || "SEND_ERROR",
            })
            .eq("cm_id", message.cm_id);

          failed++;
          console.log(
            `   ❌ ${message.phone_number}: ${result.reason?.message}`,
          );
        }
      }

      // Update campaign progress after each batch
      await supabase
        .from("campaigns")
        .update({
          messages_sent: (campaign.messages_sent || 0) + sent,
          messages_failed: (campaign.messages_failed || 0) + failed,
          updated_at: new Date().toISOString(),
        })
        .eq("campaign_id", campaign.campaign_id);

      console.log(
        `   📊 Progress: ${sent + failed}/${allMessages.length} (✅ ${sent} | ❌ ${failed})\n`,
      );

      // ✅ Batch delay between batches (industry-standard rate limiting)
      if (i + BATCH_SIZE < allMessages.length) {
        console.log(`   ⏳ Batch delay: ${BATCH_DELAY}ms before next batch...`);
        await sleep(BATCH_DELAY);
      }
    }

    // ✅ UPDATE WARM-UP PROGRESS (during warm-up)
    if (account.warmup_enabled && !account.warmup_completed && sent > 0) {
      console.log(`\n🔥 Updating warm-up progress...`);
      await updateWarmupProgress(account.wa_id, sent, account);
    }

    // ✅ NEW: UPDATE TIER DAILY SENT (after warm-up completes)
    if (account.warmup_completed && sent > 0) {
      console.log(`\n📊 Updating tier daily sent...`);
      await updateTierDailySent(account.wa_id, sent, account);
    }

    // 7️⃣ Complete or pause campaign
    const remainingPending = totalPending - allMessages.length;

    if (remainingPending > 0) {
      // Campaign has more messages, keep as scheduled
      console.log(
        `\n⏸️  Campaign paused: ${remainingPending} messages remaining`,
      );
      console.log(`   Will continue when warm-up stage progresses`);

      await supabase
        .from("campaigns")
        .update({
          status: "scheduled", // Keep as scheduled
          messages_sent: (campaign.messages_sent || 0) + sent,
          messages_failed: (campaign.messages_failed || 0) + failed,
          updated_at: new Date().toISOString(),
        })
        .eq("campaign_id", campaign.campaign_id);
    } else {
      // All messages sent, complete campaign
      await markCampaignCompleted(
        campaign.campaign_id,
        (campaign.messages_sent || 0) + sent,
        (campaign.messages_failed || 0) + failed,
      );
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log(`\n${"═".repeat(60)}`);
    console.log(`✅ CAMPAIGN ${remainingPending > 0 ? "PAUSED" : "COMPLETED"}`);
    console.log(`${"═".repeat(60)}`);
    console.log(`Name: ${campaign.campaign_name}`);
    console.log(`Sent this run: ${sent} ✅`);
    console.log(`Failed this run: ${failed} ❌`);
    console.log(`Remaining: ${remainingPending}`);
    console.log(
      `Success Rate: ${((sent / (sent + failed)) * 100).toFixed(1)}%`,
    );
    console.log(`Duration: ${duration}s`);
    console.log(`${"═".repeat(60)}\n`);
  } catch (err) {
    console.error(`\n❌ Campaign processing failed:`, err);
    console.error("Error details:", err.response?.data || err.message);

    await supabase
      .from("campaigns")
      .update({
        status: "failed",
        completed_at: new Date().toISOString(),
      })
      .eq("campaign_id", campaign.campaign_id);
  }
}

/* =====================================
   ✅ NEW: UPDATE WARM-UP PROGRESS
====================================== */

async function updateWarmupProgress(account_id, messages_sent, account) {
  try {
    // ✅ RESET DAILY COUNTER IF NEW DAY
    function shouldResetDailyCounter(last_reset) {
      if (!last_reset) return true;
      const now = new Date();
      const lastReset = new Date(last_reset);
      const nowDay = now.toISOString().split("T")[0];
      const lastResetDay = lastReset.toISOString().split("T")[0];
      return nowDay !== lastResetDay;
    }

    let current_daily_sent = account.warmup_daily_sent || 0;

    // Auto-reset if new day
    if (shouldResetDailyCounter(account.warmup_daily_reset_at)) {
      console.log("   🔄 Resetting daily counter (new day)");
      current_daily_sent = 0;
    }

    const new_daily_sent = current_daily_sent + messages_sent;

    const warmup_limits = account.warmup_limits || [200, 500, 1000];
    const current_stage = account.warmup_stage || 1;
    const current_progress =
      (account.warmup_stage_progress || 0) + messages_sent;
    const current_limit = warmup_limits[current_stage - 1];

    console.log(
      `   Progress: ${current_progress}/${current_limit} (Stage ${current_stage})`,
    );
    console.log(`   Daily: ${new_daily_sent}/${current_limit}`);

    // Check if stage completed
    if (current_progress >= current_limit) {
      if (current_stage < warmup_limits.length) {
        // ✅ Move to next stage + UPDATE DAILY
        await supabase
          .from("whatsapp_accounts")
          .update({
            warmup_stage: current_stage + 1,
            warmup_stage_progress: 0,
            warmup_daily_sent: new_daily_sent, // ✅ ADDED!
            warmup_daily_reset_at: new Date().toISOString(),
            warmup_last_updated_at: new Date().toISOString(),
          })
          .eq("wa_id", account_id);

        console.log(`   🎉 Stage ${current_stage} COMPLETED!`);
        console.log(
          `   ✅ Advanced to Stage ${current_stage + 1} (limit: ${warmup_limits[current_stage]})`,
        );
        console.log(`   📊 Daily sent updated: ${new_daily_sent}`);
      } else {
        // ✅ Warm-up completed + UPDATE DAILY
        await supabase
          .from("whatsapp_accounts")
          .update({
            warmup_completed: true,
            warmup_daily_sent: new_daily_sent, // ✅ ADDED!
            warmup_daily_reset_at: new Date().toISOString(),
            warmup_last_updated_at: new Date().toISOString(),
          })
          .eq("wa_id", account_id);

        console.log(
          `   🏆 WARM-UP COMPLETED! Account can now send at full capacity!`,
        );
        console.log(`   📊 Daily sent updated: ${new_daily_sent}`);
        console.log(
          `   ⚠️  Tier limit (${account.messaging_limit_per_day}/day) will still apply`,
        );
      }
    } else {
      // ✅ Update progress + UPDATE DAILY
      await supabase
        .from("whatsapp_accounts")
        .update({
          warmup_stage_progress: current_progress,
          warmup_daily_sent: new_daily_sent, // ✅ ADDED!
          warmup_daily_reset_at: new Date().toISOString(),
          warmup_last_updated_at: new Date().toISOString(),
        })
        .eq("wa_id", account_id);

      const stage_remaining = current_limit - current_progress;
      const daily_remaining = current_limit - new_daily_sent;

      console.log(`   📈 Stage: ${stage_remaining} messages to next stage`);
      console.log(`   📅 Daily: ${daily_remaining} messages remaining today`);
    }
  } catch (error) {
    console.error("   ⚠️  Error updating warm-up progress:", error);
  }
}

/* =====================================
   UPDATE TIER DAILY SENT
====================================== */
async function updateTierDailySent(account_id, messages_sent, account) {
  try {
    console.log(`📊 Updating tier daily sent for account ${account_id}`);

    const now = new Date();
    const tierDailySent = account.tier_daily_sent || 0;

    // Check if we need to reset (new day)
    const lastResetDate = account.tier_daily_reset_at
      ? new Date(account.tier_daily_reset_at).toISOString().split("T")[0]
      : null;
    const todayDate = now.toISOString().split("T")[0];

    let newTierDailySent;

    if (!lastResetDate || lastResetDate !== todayDate) {
      // New day - reset counter
      console.log("🔄 New day detected - resetting tier daily counter");
      newTierDailySent = messages_sent;
    } else {
      // Same day - increment
      newTierDailySent = tierDailySent + messages_sent;
    }

    // Update database
    const { data: updateData, error: updateError } = await supabase
      .from("whatsapp_accounts")
      .update({
        tier_daily_sent: newTierDailySent,
        tier_daily_reset_at: now.toISOString(),
      })
      .eq("wa_id", account_id)
      .select()
      .single();

    if (updateError) {
      console.error("❌ Error updating tier daily sent:", updateError);
      return;
    }

    console.log(
      `✅ Tier daily sent updated: ${newTierDailySent}/${account.messaging_limit_per_day}`,
    );
  } catch (err) {
    console.error("❌ Error in updateTierDailySent:", err);
  }
}

/* =====================================
   SEND WHATSAPP MESSAGE (UNCHANGED)
====================================== */

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
    // Parse template components
    let templateComponents = template.components;
    if (typeof templateComponents === "string") {
      try {
        templateComponents = JSON.parse(templateComponents);
      } catch (e) {
        console.log("   ⚠️  Failed to parse template components");
        templateComponents = [];
      }
    }

    // Build WhatsApp API message payload
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

    // Handle MEDIA HEADER (IMAGE/VIDEO/DOCUMENT)
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
              [format.toLowerCase()]: {
                id: campaignMediaId,
              },
            },
          ],
        });
      } else if (format === "TEXT" && headerComponent.example) {
        const headerText = headerComponent.example.header_text || [];
        if (headerText.length > 0) {
          messageBody.template.components.push({
            type: "header",
            parameters: headerText.map((text) => ({
              type: "text",
              text: text,
            })),
          });
        }
      }
    }

    // Handle BODY VARIABLES
    let parsedVariables = variables;
    if (typeof variables === "string") {
      try {
        parsedVariables = JSON.parse(variables);
      } catch (e) {
        parsedVariables = {};
      }
    }

    if (parsedVariables && Object.keys(parsedVariables).length > 0) {
      const parameters = Object.values(parsedVariables).map((value) => ({
        type: "text",
        text: String(value),
      }));

      messageBody.template.components.push({
        type: "body",
        parameters: parameters,
      });
    }

    const phoneNumberId = account.phone_number_id;
    const accessToken = account.system_user_access_token;

    // Send via WhatsApp Business API
    const response = await axios.post(
      `https://graph.facebook.com/v21.0/${phoneNumberId}/messages`,
      messageBody,
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": "application/json",
        },
        timeout: 30000,
      },
    );

    const wa_message_id = response.data.messages?.[0]?.id;
    const templateText = extractTemplateText(template);

    // Store in whatsapp_messages table
    const { data: wmRecord, error: wmError } = await supabase
      .from("whatsapp_messages")
      .insert({
        account_id: account.wa_id,
        to_number: phoneNumber,
        template_name: template.name,
        message_body: JSON.stringify(messageBody),
        wa_message_id: wa_message_id,
        status: "sent",
        sent_at: new Date().toISOString(),
      })
      .select()
      .single();

    if (wmError) {
      console.error("   ⚠️  Failed to store in whatsapp_messages:", wmError);
    }

    // Find or create chat
    const chatId = await findOrCreateChat(
      phoneNumber,
      contactName,
      groupId,
      userId,
    );

    // Store in messages table
    await supabase.from("messages").insert({
      chat_id: chatId,
      sender_type: "admin",
      message: templateText,
      message_type: "template",
      wa_message_id: wa_message_id,
      status: "sent",
      created_at: new Date().toISOString(),
    });

    return {
      wm_id: wmRecord?.wm_id,
      wa_message_id: wa_message_id,
      chat_id: chatId,
    };
  } catch (err) {
    const errorMessage =
      err.response?.data?.error?.message ||
      err.message ||
      "Failed to send message";
    throw new Error(errorMessage);
  }
}

/* =====================================
   HELPER FUNCTIONS (UNCHANGED)
====================================== */

async function findOrCreateChat(phoneNumber, contactName, groupId, userId) {
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
          last_message: "Template message sent via campaign",
          last_message_at: new Date().toISOString(),
          last_admin_message_at: new Date().toISOString(),
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
        last_message: "Template message sent via campaign",
        last_message_at: new Date().toISOString(),
        group_id: groupId,
        mode: "AUTO",
        last_admin_message_at: new Date().toISOString(),
        user_id: userId,
        status: "active",
        unread_count: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .select()
      .single();

    if (error) throw error;

    return newChat.chat_id;
  } catch (err) {
    console.error("   ⚠️  Error in findOrCreateChat:", err.message);
    return null;
  }
}

function extractTemplateText(template) {
  try {
    if (!template.components || !Array.isArray(template.components)) {
      return `Template: ${template.name}`;
    }

    let components = template.components;
    if (typeof components === "string") {
      try {
        components = JSON.parse(components);
      } catch (e) {
        return `Template: ${template.name}`;
      }
    }

    const bodyComponent = components.find((comp) => comp.type === "BODY");

    if (bodyComponent && bodyComponent.text) {
      return bodyComponent.text;
    }

    return `Template: ${template.name}`;
  } catch (err) {
    return `Template: ${template.name}`;
  }
}

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
    console.error("   ⚠️  Error marking campaign as completed:", err);
  }
}

// ✅ Returns a random integer between min and max (inclusive)
function randomDelay(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/* =====================================
   EXPORT
====================================== */

export default { startCampaignScheduler, checkAndSendCampaigns };
