// scheduler/cartRecoveryJob.js
// Abandoned Cart Recovery — runs every 30 minutes
// Fetches checkout-draft orders from WooCommerce and sends WhatsApp recovery messages

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";
import axios from "axios";
import FormData from "form-data";
// Add this import at the top
import { decode } from "html-entities";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const GRAPH = "https://graph.facebook.com/v21.0";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function normalizePhone(phone, countryCode = "91") {
  if (!phone) return null;
  const digits = phone.replace(/\D/g, "");
  if (digits.startsWith(countryCode) && digits.length > 10) return `+${digits}`;
  if (digits.length === 10) return `+${countryCode}${digits}`;
  if (digits.length > 10) return `+${digits}`;
  return null;
}

function buildVariables(order, varMap) {
  const billing = order.billing || {};
  const fullName =
    `${billing.first_name || ""} ${billing.last_name || ""}`.trim();
  // In buildVariables, update itemNames:
  const itemNames = (order.line_items || [])
    .map((i) => decode(i.name)) // ✅ decode HTML entities
    .join(", ");

  const SOURCE = {
    billing_full_name: fullName || "Customer",
    cart_total: `${order.currency_symbol || "₹"}${order.total || "0"}`,
    item_names: itemNames || "your items",
    checkout_url: order.payment_url || "",
    order_number: String(order.number || order.id),
  };

  const result = {};
  for (const [pos, field] of Object.entries(varMap)) {
    result[pos] = SOURCE[field] || "";
  }
  return result;
}

async function uploadImageToMeta(imageUrl, account) {
  try {
    const imageResponse = await axios.get(imageUrl, {
      responseType: "arraybuffer",
      timeout: 15000,
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
      `${GRAPH}/${account.phone_number_id}/media`,
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
    console.warn("   ⚠️  Image upload failed:", err.message);
    return null;
  }
}

async function sendRecoveryMessage(
  account,
  template,
  phone,
  variables,
  mediaId,
) {
  const components = [];

  if (mediaId) {
    components.push({
      type: "header",
      parameters: [{ type: "image", image: { id: mediaId } }],
    });
  }

  if (Object.keys(variables).length > 0) {
    components.push({
      type: "body",
      parameters: Object.values(variables).map((v) => ({
        type: "text",
        text: String(v),
      })),
    });
  }

  const payload = {
    messaging_product: "whatsapp",
    to: phone,
    type: "template",
    template: {
      name: template.name,
      language: { code: template.language || "en_US" },
      components,
    },
  };

  const response = await axios.post(
    `${GRAPH}/${account.phone_number_id}/messages`,
    payload,
    {
      headers: {
        Authorization: `Bearer ${account.system_user_access_token}`,
        "Content-Type": "application/json",
      },
      timeout: 30000,
    },
  );

  return response.data?.messages?.[0]?.id;
}

// ─── Main Recovery Logic ──────────────────────────────────────────────────────

async function processConnection(connection, automation, account, template) {
  const WC_BASE = `${connection.store_url}/wp-json/wc/v3`;
  const wcAuth = {
    username: connection.consumer_key,
    password: connection.consumer_secret,
  };

  console.log(
    `\n   🛒 Checking store: ${connection.store_name || connection.store_url}`,
  );

  // Fetch checkout-draft orders
  let draftOrders = [];
  try {
    const response = await axios.get(`${WC_BASE}/orders`, {
      params: {
        status: "checkout-draft",
        per_page: 50,
        orderby: "modified",
        order: "desc",
      },
      auth: wcAuth,
      timeout: 15000,
    });
    draftOrders = response.data || [];
  } catch (err) {
    console.warn(`   ⚠️  Could not fetch draft orders: ${err.message}`);
    return { checked: 0, sent: 0, skipped: 0 };
  }

  console.log(`   📋 Found ${draftOrders.length} draft order(s)`);

  const delayMinutes = automation.delay_minutes || 60;
  const cutoffTime = new Date(Date.now() - delayMinutes * 60 * 1000);

  let sent = 0,
    skipped = 0;

  for (const order of draftOrders) {
    try {
      // Check if cart is old enough to be considered abandoned
      // Check if cart is old enough to be considered abandoned
      const lastModified = new Date(order.date_modified);
      if (lastModified > cutoffTime) {
        skipped++;
        continue; // Too recent — not abandoned yet
      }

      // ✅ Skip carts older than 24 hours — too stale to recover
      const maxAge = new Date(Date.now() - 24 * 60 * 60 * 1000);
      if (lastModified < maxAge) {
        skipped++;
        continue; // Too old
      }

      // Must have phone number
      const rawPhone = order.billing?.phone;
      if (!rawPhone) {
        skipped++;
        continue;
      }

      const phone = normalizePhone(rawPhone);
      if (!phone) {
        skipped++;
        continue;
      }

      const wc_order_id = String(order.id);

      // Check if already processed
      const { data: existing } = await supabase
        .from("woocommerce_cart_recovery")
        .select("id, status")
        .eq("connection_id", connection.id)
        .eq("wc_order_id", wc_order_id)
        .maybeSingle();

      if (existing) {
        skipped++;
        continue; // Already sent or processed
      }

      // Get product image from line items (already in draft order response)
      const firstItem = order.line_items?.[0];
      const productImageUrl = firstItem?.image?.src || null;

      // Save to cart recovery table
      const cartEntry = {
        user_id: connection.user_id,
        connection_id: connection.id,
        wc_order_id,
        phone_number: phone,
        customer_name:
          `${order.billing?.first_name || ""} ${order.billing?.last_name || ""}`.trim(),
        cart_items:
          order.line_items?.map((i) => ({
            product_id: i.product_id,
            name: i.name,
            quantity: i.quantity,
            total: i.total,
            image: i.image?.src,
          })) || [],
        cart_total: order.total,
        cart_currency: order.currency || "INR",
        checkout_url: order.payment_url || "",
        product_image_url: productImageUrl,
        status: "sending",
      };

      const { data: cartRecord, error: insertErr } = await supabase
        .from("woocommerce_cart_recovery")
        .insert(cartEntry)
        .select()
        .single();

      if (insertErr) {
        console.warn(
          `   ⚠️  DB insert failed for order ${wc_order_id}:`,
          insertErr.message,
        );
        skipped++;
        continue;
      }

      // Upload product image if automation has include_product_image
      let mediaId = null;
      if (automation.include_product_image && productImageUrl) {
        console.log(`   🖼️  Uploading product image...`);
        mediaId = await uploadImageToMeta(productImageUrl, account);
      }

      // Build variables from template_variable_map
      const variables = buildVariables(
        order,
        automation.template_variable_map || {
          1: "billing_full_name",
          2: "item_names",
          3: "cart_total",
          4: "checkout_url",
        },
      );

      console.log(
        `   📱 Sending recovery to ${phone} for order ${wc_order_id}`,
      );
      console.log(`   📊 Variables:`, variables);

      // Send WhatsApp message
      let waMessageId = null;
      let sendStatus = "sent";
      let errorMessage = null;

      try {
        waMessageId = await sendRecoveryMessage(
          account,
          template,
          phone,
          variables,
          mediaId,
        );
        console.log(`   ✅ Recovery sent! WA ID: ${waMessageId}`);
      } catch (sendErr) {
        sendStatus = "failed";
        errorMessage =
          sendErr.response?.data?.error?.message || sendErr.message;
        console.error(`   ❌ Send failed:`, errorMessage);
      }

      // Update cart recovery record
      await supabase
        .from("woocommerce_cart_recovery")
        .update({
          status: sendStatus,
          recovery_sent_at:
            sendStatus === "sent" ? new Date().toISOString() : null,
          error_message: errorMessage,
          wm_id: waMessageId,
          updated_at: new Date().toISOString(),
        })
        .eq("id", cartRecord.id);

      if (sendStatus === "sent") sent++;
      else skipped++;
    } catch (err) {
      console.error(`   ❌ Error processing order ${order.id}:`, err.message);
      skipped++;
    }
  }

  return { checked: draftOrders.length, sent, skipped };
}

// ─── Main Cron Function ───────────────────────────────────────────────────────

async function runCartRecoveryCron() {
  console.log("\n════════════════════════════════════════════════════════");
  console.log("🛒 Cart Recovery Cron — Starting");
  console.log(`📅 ${new Date().toISOString()}`);
  console.log("════════════════════════════════════════════════════════");

  try {
    // Get all active cart recovery automations
    const { data: automations, error: autoErr } = await supabase
      .from("woocommerce_automations")
      .select(
        `
        *,
        user_woocommerce_connections (
          id, user_id, store_url, store_name,
          consumer_key, consumer_secret, is_active
        ),
        whatsapp_templates (
          wt_id, name, language, components, status
        ),
        whatsapp_accounts (
          wa_id, phone_number_id, system_user_access_token, waba_id
        )
      `,
      )
      .eq("trigger_event", "cart.abandoned")
      .eq("is_active", true);

    if (autoErr) throw autoErr;

    if (!automations || automations.length === 0) {
      console.log("ℹ️  No active cart recovery automations found");
      console.log("════════════════════════════════════════════════════════\n");
      return;
    }

    console.log(`✅ Found ${automations.length} cart recovery automation(s)`);

    let totalSent = 0,
      totalSkipped = 0,
      totalChecked = 0;

    for (const automation of automations) {
      const connection = automation.user_woocommerce_connections;
      const template = automation.whatsapp_templates;
      const account = automation.whatsapp_accounts;

      if (!connection?.is_active) {
        console.log(`⚠️  Skipping inactive connection`);
        continue;
      }

      if (!template || template.status !== "APPROVED") {
        console.log(`⚠️  Skipping — template not approved: ${template?.name}`);
        continue;
      }

      if (!account) {
        console.log(`⚠️  Skipping — no WhatsApp account found`);
        continue;
      }

      const result = await processConnection(
        connection,
        automation,
        account,
        template,
      );
      totalChecked += result.checked;
      totalSent += result.sent;
      totalSkipped += result.skipped;
    }

    console.log("\n📊 Cart Recovery Summary:");
    console.log(`   Draft orders checked: ${totalChecked}`);
    console.log(`   Recovery messages sent: ${totalSent}`);
    console.log(`   Skipped: ${totalSkipped}`);
  } catch (err) {
    console.error("❌ Cart recovery cron failed:", err.message);
  }

  console.log("════════════════════════════════════════════════════════\n");
}

// ─── Export ───────────────────────────────────────────────────────────────────

export function startCartRecoveryCron() {
  cron.schedule(
    "*/30 * * * *", // Every 3 minutes
    runCartRecoveryCron,
    { timezone: "UTC" },
  );

  console.log("⏰ Cart recovery cron started (every 3 minutes)");
}

// Export for manual testing
export { runCartRecoveryCron };
