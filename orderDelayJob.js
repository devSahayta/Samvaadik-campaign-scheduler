// scheduler/orderDelayJob.js
import cron from "node-cron";
import axios from "axios";
import FormData from "form-data";
import { decode } from "html-entities";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

// ─── Helpers ──────────────────────────────────────────────────────────────────

function wcClient(storeUrl, consumerKey, consumerSecret) {
  return axios.create({
    baseURL: `${storeUrl.replace(/\/$/, "")}/wp-json/wc/v3`,
    auth: { username: consumerKey, password: consumerSecret },
    timeout: 20000,
  });
}

function normalizePhone(phone, defaultCountryCode = "91") {
  if (!phone) return null;
  let cleaned = phone.replace(/[\s\-().]/g, "");
  if (cleaned.startsWith("+")) {
    const digits = cleaned.replace(/\D/g, "");
    return digits.length >= 10 ? digits : null;
  }
  if (cleaned.startsWith("00")) {
    cleaned = cleaned.slice(2);
    return cleaned.replace(/\D/g, "");
  }
  if (cleaned.startsWith("0")) cleaned = cleaned.slice(1);
  return defaultCountryCode + cleaned;
}

const STUCK_STATUSES = ["processing", "on-hold"];

function buildVariables(order, variableMap) {
  const variables = {};
  const orderFields = {
    order_number: String(order.number || order.id),
    order_id: String(order.id),
    total: `${order.currency_symbol || ""}${order.total}`,
    billing_full_name: decode(
      `${order.billing?.first_name || ""} ${order.billing?.last_name || ""}`.trim(),
    ),
    item_names: decode((order.line_items || []).map((i) => i.name).join(", ")),
    order_date: new Date(order.date_created).toLocaleDateString("en-IN"),
  };
  for (const [position, fieldName] of Object.entries(variableMap || {})) {
    variables[position] = orderFields[fieldName] || "";
  }
  return variables;
}

async function uploadImageToMeta(imageUrl, account) {
  try {
    const imageResponse = await axios.get(imageUrl, {
      responseType: "arraybuffer",
      timeout: 20000,
    });
    const imageBuffer = Buffer.from(imageResponse.data);
    const contentType = imageResponse.headers["content-type"] || "image/jpeg";
    if (contentType.includes("webp")) return null;

    const form = new FormData();
    form.append("messaging_product", "whatsapp");
    form.append("type", contentType);
    form.append("file", imageBuffer, {
      filename: "product-image.jpg",
      contentType,
    });

    const uploadResponse = await axios.post(
      `https://graph.facebook.com/v21.0/${account.phone_number_id}/media`,
      form,
      {
        headers: {
          Authorization: `Bearer ${account.system_user_access_token}`,
          ...form.getHeaders(),
        },
        timeout: 30000,
      },
    );
    return uploadResponse.data?.id || null;
  } catch (err) {
    console.warn(
      "   ⚠️  uploadImageToMeta failed:",
      err.response?.data || err.message,
    );
    return null;
  }
}

function buildWhatsAppPayload(
  template,
  phoneNumber,
  variables,
  mediaId = null,
) {
  let components = template.components;
  if (typeof components === "string") {
    try {
      components = JSON.parse(components);
    } catch {
      components = [];
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

  if (mediaId) {
    messageBody.template.components.push({
      type: "header",
      parameters: [{ type: "image", image: { id: mediaId } }],
    });
  }

  if (variables && Object.keys(variables).length > 0) {
    messageBody.template.components.push({
      type: "body",
      parameters: Object.values(variables).map((v) => ({
        type: "text",
        text: String(v),
      })),
    });
  }

  return messageBody;
}

// ─── Core job ─────────────────────────────────────────────────────────────────

export async function runOrderDelayCron() {
  console.log("\n════════════════════════════════════════════════════════");
  console.log("⏳ Order Delay Cron — Starting");
  console.log(`📅 ${new Date().toISOString()}`);
  console.log("════════════════════════════════════════════════════════");

  try {
    // 1. Find all active order.delayed automations
    const { data: automations, error: autoErr } = await supabase
      .from("woocommerce_automations")
      .select(
        `*, user_woocommerce_connections(*), whatsapp_templates(*), whatsapp_accounts(*)`,
      )
      .eq("trigger_event", "order.delayed")
      .eq("is_active", true);

    if (autoErr) throw autoErr;

    if (!automations || automations.length === 0) {
      console.log("✅ No active order-delay automations");
      return;
    }

    console.log(`✅ Found ${automations.length} order-delay automation(s)`);

    let totalChecked = 0;
    let totalSent = 0;
    let totalSkipped = 0;

    for (const automation of automations) {
      const connection = automation.user_woocommerce_connections;
      const template = automation.whatsapp_templates;
      const account = automation.whatsapp_accounts;

      if (!connection || !template || !account) {
        console.warn(
          "   ⚠️  Missing connection/template/account, skipping automation",
        );
        continue;
      }

      // Sequential stages — e.g. [2, 4, 6] means send at day 2, day 4, day 6
      const stages =
        Array.isArray(automation.delay_stages) &&
        automation.delay_stages.length > 0
          ? [...automation.delay_stages].sort((a, b) => a - b)
          : [automation.delay_days ?? 2];

      console.log(
        `\n   🏪 Checking store: ${connection.store_name} (stages: ${stages.join(", ")} day(s))`,
      );

      const client = wcClient(
        connection.store_url,
        connection.consumer_key,
        connection.consumer_secret,
      );

      for (const status of STUCK_STATUSES) {
        let orders = [];
        try {
          // Only fetch orders created after this automation was set up
          const automationCreatedDate = new Date(
            automation.created_at,
          ).toISOString();

          const res = await client.get("/orders", {
            params: {
              status,
              per_page: 100,
              orderby: "date",
              order: "desc", // newest first
              after: automationCreatedDate, // only orders after automation was created
            },
          });
          orders = res.data || [];
        } catch (e) {
          console.warn(
            `   ⚠️  Failed to fetch '${status}' orders:`,
            e.response?.data?.message || e.message,
          );
          continue;
        }

        console.log(
          `   📋 Found ${orders.length} '${status}' order(s) to evaluate`,
        );

        for (const order of orders) {
          totalChecked++;

          const orderDate = new Date(order.date_created);

          // Age in MINUTES for testing — revert to days for production:
          // Production:  / (1000 * 60 * 60 * 24)
          // Testing:     / (1000 * 60)
          // const ageInDays = (Date.now() - orderDate.getTime()) / (1000 * 60); // ← TESTING MODE
          // ✅ Production (days)
          const ageInDays =
            (Date.now() - orderDate.getTime()) / (1000 * 60 * 60 * 24);

          // ── Step 1: Check how many stages already sent for this order ──
          const { data: sentStages } = await supabase
            .from("woocommerce_order_delay_tracking")
            .select("stage")
            .eq("connection_id", connection.id)
            .eq("wc_order_id", String(order.id))
            .eq("status", "sent");

          const sentStageNumbers = (sentStages || []).map((r) => r.stage);
          const highestSentStage =
            sentStageNumbers.length > 0 ? Math.max(...sentStageNumbers) : 0;

          // ── Step 2: Find next stage to send ──
          const nextStageIndex = highestSentStage; // 0-indexed into stages array
          if (nextStageIndex >= stages.length) {
            totalSkipped++;
            continue; // all stages already sent for this order
          }

          const nextStageDay = stages[nextStageIndex]; // ✅ defined BEFORE use
          const nextStageNumber = nextStageIndex + 1;

          // ── Step 3: Check if order is old enough for this stage ──
          if (ageInDays < nextStageDay) {
            totalSkipped++;
            console.log(
              `   ⏭️  Order ${order.id} skipped — age ${ageInDays.toFixed(2)} mins < stage ${nextStageDay} mins needed`,
            );
            continue;
          }

          // ── Step 4: Validate phone ──
          const rawPhone = order.billing?.phone;
          const phone = normalizePhone(rawPhone);
          if (!phone) {
            totalSkipped++;
            continue;
          }

          console.log(
            `   📱 Sending stage ${nextStageNumber}/${stages.length} delay notice to ${phone} for order ${order.id} (age: ${ageInDays.toFixed(1)} mins, status: ${order.status})`,
          );

          // ── Step 5: Claim this stage (unique constraint prevents duplicates) ──
          const { data: trackingRow, error: trackErr } = await supabase
            .from("woocommerce_order_delay_tracking")
            .insert({
              automation_id: automation.id,
              connection_id: connection.id,
              user_id: connection.user_id,
              wc_order_id: String(order.id),
              order_status: order.status,
              phone_number: phone,
              stage: nextStageNumber,
              status: "pending",
            })
            .select()
            .single();

          if (trackErr) {
            console.warn(
              `   ⚠️  Could not claim order ${order.id} stage ${nextStageNumber}:`,
              trackErr.message,
            );
            totalSkipped++;
            continue;
          }

          // ── Step 6: Send WhatsApp message ──
          try {
            const variables = buildVariables(
              order,
              automation.template_variable_map || {},
            );

            let mediaId = null;
            if (automation.include_product_image) {
              const imageUrl = order.line_items?.[0]?.image?.src || null;
              if (imageUrl)
                mediaId = await uploadImageToMeta(imageUrl, account);
            }

            const payload = buildWhatsAppPayload(
              template,
              phone,
              variables,
              mediaId,
            );

            const waResponse = await axios.post(
              `https://graph.facebook.com/v21.0/${account.phone_number_id}/messages`,
              payload,
              {
                headers: {
                  Authorization: `Bearer ${account.system_user_access_token}`,
                  "Content-Type": "application/json",
                },
                timeout: 30000,
              },
            );

            const wa_message_id = waResponse.data.messages?.[0]?.id;

            await supabase
              .from("woocommerce_order_delay_tracking")
              .update({
                status: "sent",
                delay_message_sent_at: new Date().toISOString(),
              })
              .eq("id", trackingRow.id);

            await supabase.from("woocommerce_automation_logs").insert({
              automation_id: automation.id,
              user_id: connection.user_id,
              connection_id: connection.id,
              trigger_event: "order.delayed",
              wc_order_id: String(order.id),
              phone_number: phone,
              status: "sent",
              sent_at: new Date().toISOString(),
            });

            console.log(`   ✅ Delay notice sent! WA ID: ${wa_message_id}`);
            totalSent++;
          } catch (sendErr) {
            console.error(
              `   ❌ Send failed for order ${order.id}:`,
              sendErr.response?.data || sendErr.message,
            );

            await supabase
              .from("woocommerce_order_delay_tracking")
              .update({ status: "failed" })
              .eq("id", trackingRow.id);

            await supabase.from("woocommerce_automation_logs").insert({
              automation_id: automation.id,
              user_id: connection.user_id,
              connection_id: connection.id,
              trigger_event: "order.delayed",
              wc_order_id: String(order.id),
              phone_number: phone,
              status: "failed",
              error_message:
                sendErr.response?.data?.error?.message || sendErr.message,
            });
          }
        }
      }
    }

    console.log("\n📊 Order Delay Summary:");
    console.log(`   Orders checked: ${totalChecked}`);
    console.log(`   Delay messages sent: ${totalSent}`);
    console.log(`   Skipped: ${totalSkipped}`);
    console.log("════════════════════════════════════════════════════════\n");
  } catch (err) {
    console.error("❌ Order Delay Cron failed:", err.message);
  }
}

export function startOrderDelayCron() {
  cron.schedule("0 * * * *", runOrderDelayCron, { timezone: "UTC" });
  console.log("⏰ Order delay cron started (every 1 hour)");
}

// // ⚠️  TEMPORARY — remove before pushing to production
// runOrderDelayCron();
