// scheduledMessageJob.js
// Processes individual scheduled template messages inserted by the backend.
// Backend inserts a row into `scheduled_messages` with a future `scheduled_at`,
// and this job picks it up and sends it via WhatsApp Business API.

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";
import axios from "axios";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

let isProcessing = false;

// Max messages to process per minute cycle
const BATCH_LIMIT = 50;

/* =====================================
   START CRON JOB
====================================== */

export function startScheduledMessageCron() {
  console.log("🚀 Scheduled Message Job Started!");
  console.log("⏰ Checking for due scheduled messages every minute...");

  cron.schedule("* * * * *", async () => {
    if (isProcessing) {
      console.log("⏭️  Skipping: Scheduled message job still running");
      return;
    }

    try {
      isProcessing = true;
      await processScheduledMessages();
    } catch (err) {
      console.error("❌ Scheduled message job error:", err);
    } finally {
      isProcessing = false;
    }
  });
}

/* =====================================
   FETCH AND PROCESS DUE MESSAGES
====================================== */

async function processScheduledMessages() {
  const now = new Date();
  console.log(`\n📨 [${now.toISOString()}] Checking scheduled messages...`);

  const { data: messages, error } = await supabase
    .from("scheduled_messages")
    .select("*")
    .eq("status", "scheduled")
    .lte("scheduled_at", now.toISOString())
    .order("scheduled_at", { ascending: true })
    .limit(BATCH_LIMIT);

  if (error) {
    console.error("❌ Error fetching scheduled messages:", error);
    return;
  }

  if (!messages || messages.length === 0) {
    console.log("✅ No scheduled messages due");
    return;
  }

  console.log(`📩 Found ${messages.length} message(s) to send`);

  for (const msg of messages) {
    await processSingleMessage(msg);
    if (messages.indexOf(msg) < messages.length - 1) {
      const delay = randomBetween(1000, 2000);
      await sleep(delay);
    }
  }
}

/* =====================================
   PROCESS ONE SCHEDULED MESSAGE
====================================== */

async function processSingleMessage(msg) {
  console.log(`\n  → ${msg.phone_number} | sm_id: ${msg.sm_id}`);

  try {
    // Mark as processing so concurrent runs skip it
    await supabase
      .from("scheduled_messages")
      .update({ status: "processing", updated_at: new Date().toISOString() })
      .eq("sm_id", msg.sm_id);

    // Fetch WhatsApp account
    const { data: account, error: accountError } = await supabase
      .from("whatsapp_accounts")
      .select("*")
      .eq("wa_id", msg.account_id)
      .single();

    if (accountError || !account) {
      throw new Error("WhatsApp account not found");
    }

    // Fetch template
    const { data: template, error: templateError } = await supabase
      .from("whatsapp_templates")
      .select("*")
      .eq("wt_id", msg.wt_id)
      .single();

    if (templateError || !template) {
      throw new Error("Template not found");
    }

    // Send via WhatsApp Business API
    const result = await sendWhatsAppMessage(account, template, msg);

    // Mark as sent
    await supabase
      .from("scheduled_messages")
      .update({
        status: "sent",
        sent_at: new Date().toISOString(),
        wa_message_id: result.wa_message_id,
        wm_id: result.wm_id,
        updated_at: new Date().toISOString(),
      })
      .eq("sm_id", msg.sm_id);

    console.log(`  ✅ Sent to ${msg.phone_number}`);
  } catch (err) {
    const errorMessage =
      err.response?.data?.error?.message || err.message || "Unknown error";
    const errorCode =
      err.response?.data?.error?.code?.toString() || "SEND_ERROR";

    await supabase
      .from("scheduled_messages")
      .update({
        status: "failed",
        failed_at: new Date().toISOString(),
        error_message: errorMessage,
        error_code: errorCode,
        updated_at: new Date().toISOString(),
      })
      .eq("sm_id", msg.sm_id);

    console.log(`  ❌ Failed for ${msg.phone_number}: ${errorMessage}`);
  }
}

/* =====================================
   SEND WHATSAPP MESSAGE
====================================== */

async function sendWhatsAppMessage(account, template, msg) {
  // Parse template components
  let templateComponents = template.components;
  if (typeof templateComponents === "string") {
    try {
      templateComponents = JSON.parse(templateComponents);
    } catch {
      templateComponents = [];
    }
  }

  const messageBody = {
    messaging_product: "whatsapp",
    to: msg.phone_number,
    type: "template",
    template: {
      name: template.name,
      language: { code: template.language },
      components: [],
    },
  };

  // Handle media/text header
  const headerComponent = templateComponents?.find(
    (c) => c.type === "HEADER",
  );

  if (headerComponent?.format) {
    const format = headerComponent.format.toUpperCase();

    if (["IMAGE", "VIDEO", "DOCUMENT"].includes(format)) {
      if (!msg.media_id) {
        throw new Error(
          `Template requires a ${format} media_id but none was provided`,
        );
      }
      messageBody.template.components.push({
        type: "header",
        parameters: [
          {
            type: format.toLowerCase(),
            [format.toLowerCase()]: { id: msg.media_id },
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

  // Handle body variables
  let parsedVariables = msg.template_variables;
  if (typeof parsedVariables === "string") {
    try {
      parsedVariables = JSON.parse(parsedVariables);
    } catch {
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

  // Call WhatsApp Business API
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
  const templateText = substituteVariables(
    extractTemplateText(template),
    msg.template_variables,
  );
  const templateButtons = extractTemplateButtons(template);

  // Store in whatsapp_messages
  const { data: wmRecord } = await supabase
    .from("whatsapp_messages")
    .insert({
      account_id: account.wa_id,
      to_number: msg.phone_number,
      template_name: template.name,
      message_body: JSON.stringify(messageBody),
      wa_message_id,
      status: "sent",
      sent_at: new Date().toISOString(),
    })
    .select()
    .single();

  // Create or update chat record
  const chatId = await findOrCreateChat(
    msg.phone_number,
    msg.contact_name,
    msg.user_id,
    templateText,
    msg.media_id,
  );

  // Store in messages table
  if (chatId) {
    await supabase.from("messages").insert({
      chat_id: chatId,
      sender_type: "admin",
      message: templateText,
      message_type: "template",
      media_path: msg.media_id || null,
      buttons: templateButtons,
      created_at: new Date().toISOString(),
    });
  }

  return {
    wm_id: wmRecord?.wm_id,
    wa_message_id,
  };
}

/* =====================================
   HELPER: FIND OR CREATE CHAT
====================================== */

async function findOrCreateChat(
  phoneNumber,
  contactName,
  userId,
  lastMessage,
  mediaId,
) {
  try {
    const { data: existing } = await supabase
      .from("chats")
      .select("chat_id, person_name")
      .eq("phone_number", phoneNumber)
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(1);

    if (existing && existing.length > 0) {
      await supabase
        .from("chats")
        .update({
          last_message: lastMessage,
          last_message_at: new Date().toISOString(),
          last_admin_message_at: new Date().toISOString(),
          person_name: contactName || existing[0].person_name || "Unknown",
          updated_at: new Date().toISOString(),
        })
        .eq("chat_id", existing[0].chat_id);

      return existing[0].chat_id;
    }

    const { data: newChat, error } = await supabase
      .from("chats")
      .insert({
        phone_number: phoneNumber,
        person_name: contactName || "Unknown",
        last_message: lastMessage,
        last_message_at: new Date().toISOString(),
        last_admin_message_at: new Date().toISOString(),
        mode: "AUTO",
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
    console.error("  ⚠️  findOrCreateChat error:", err.message);
    return null;
  }
}

/* =====================================
   HELPER: EXTRACT TEMPLATE TEXT
====================================== */

function extractTemplateText(template) {
  try {
    let components = template.components;
    if (typeof components === "string") {
      components = JSON.parse(components);
    }
    const body = components?.find((c) => c.type === "BODY");
    return body?.text || `Template: ${template.name}`;
  } catch {
    return `Template: ${template.name}`;
  }
}

function substituteVariables(text, variables) {
  if (!text) return text;
  let vars = variables;
  if (typeof vars === "string") {
    try { vars = JSON.parse(vars); } catch { vars = {}; }
  }
  if (!vars || typeof vars !== "object") return text;
  return text.replace(/\{\{(\w+)\}\}/g, (match, key) =>
    vars[key] !== undefined ? String(vars[key]) : match,
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function extractTemplateButtons(template) {
  try {
    // Prefer the dedicated `buttons` field on the template row
    let buttons = template.buttons;
    if (typeof buttons === "string") {
      buttons = JSON.parse(buttons);
    }

    // Fall back to extracting from components array
    if (!buttons || !Array.isArray(buttons) || buttons.length === 0) {
      let components = template.components;
      if (typeof components === "string") {
        components = JSON.parse(components);
      }
      const buttonsComponent = components?.find((c) => c.type === "BUTTONS");
      buttons = buttonsComponent?.buttons || [];
    }

    if (!Array.isArray(buttons) || buttons.length === 0) return null;

    // Normalise to {text, type} shape
    return buttons.map((b) => ({ text: b.text, type: b.type }));
  } catch {
    return null;
  }
}
