// scheduler/cartRecoveryJob.js
// Abandoned Cart Recovery — runs every 10 minutes
// Fetches checkout-draft orders from WooCommerce and sends WhatsApp recovery messages

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";
import axios from "axios";
import FormData from "form-data";
import { decode } from "html-entities";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const GRAPH = "https://graph.facebook.com/v21.0";

// ─── Helpers ──────────────────────────────────────────────────────────────────

// Returns digits-only, no leading "+" — matches the format already used by
// the rest of the system (incoming-message chats, order automations) and
// Meta's documented WhatsApp Cloud API "to" field format. A mismatched
// format here (e.g. a leading "+") causes duplicate chat rows since the
// chats_user_phone_unique constraint does an exact string match.
function normalizePhone(phone, countryCode = "91") {
  if (!phone) return null;
  const digits = phone.replace(/\D/g, "");
  if (digits.startsWith(countryCode) && digits.length > 10) return digits;
  if (digits.length === 10) return `${countryCode}${digits}`;
  if (digits.length > 10) return digits;
  return null;
}

function buildVariables(order, varMap) {
  const billing = order.billing || {};
  const shipping = order.shipping || {};
  const fullName =
    `${billing.first_name || shipping.first_name || ""} ${billing.last_name || shipping.last_name || ""}`.trim();
  const itemNames = (order.line_items || [])
    .map((i) => decode(i.name))
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

// Renders a human-readable version of the sent template for storage in the
// chat thread (best-effort — falls back to a simple summary if the template
// body can't be parsed).
function buildTemplateText(template, variables) {
  try {
    let comps = template.components;
    if (typeof comps === "string") comps = JSON.parse(comps);
    const bodyComp = Array.isArray(comps)
      ? comps.find((c) => c.type === "BODY")
      : null;
    if (bodyComp?.text) {
      let text = bodyComp.text;
      for (const [pos, value] of Object.entries(variables)) {
        text = text.replace(
          new RegExp(`\\{\\{${pos}\\}\\}`, "g"),
          String(value),
        );
      }
      return text;
    }
  } catch {
    // fall through to fallback below
  }
  return Object.values(variables).filter(Boolean).join(" · ") || template.name;
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

// Mirrors the findOrCreateWooChat pattern used by the order-notification flow
// (Vercel backend). Uses upsert on the chats_user_phone_unique constraint
// (user_id, phone_number) so concurrent calls can't create duplicate/racing
// chat rows or return null.
async function findOrCreateWooChat(phone, personName, userId, lastMessageText) {
  try {
    const { data, error } = await supabase
      .from("chats")
      .upsert(
        {
          user_id: userId,
          phone_number: phone,
          person_name: personName || "Customer",
          last_message: lastMessageText,
          last_message_at: new Date().toISOString(),
          last_sender_type: "admin",
          last_admin_message_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        },
        { onConflict: "user_id,phone_number" },
      )
      .select("chat_id")
      .single();

    if (error) {
      console.error("   ⚠️  findOrCreateWooChat error:", error.message);
      return null;
    }

    return data?.chat_id || null;
  } catch (err) {
    console.error("   ⚠️  findOrCreateWooChat exception:", err.message);
    return null;
  }
}

async function storeCartRecoveryMessage({ chatId, templateText, mediaPath }) {
  const { data, error } = await supabase
    .from("messages")
    .insert({
      chat_id: chatId,
      sender_type: "admin",
      message: templateText,
      message_type: "template",
      media_path: mediaPath || null,
      buttons: null,
      created_at: new Date().toISOString(),
    })
    .select("message_id")
    .single();

  if (error) {
    console.error("   ❌ Failed to store message:", error.message);
    return null;
  }

  console.log(
    `   💬 Stored in chat dashboard — chat_id: ${chatId}, message_id: ${data.message_id}`,
  );
  return data.message_id;
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
  const BOT_UA_PATTERN =
    /bot|crawler|spider|storebot|slurp|facebookexternalhit/i;

  let sent = 0,
    skipped = 0;

  for (const order of draftOrders) {
    try {
      // Skip drafts created by crawlers (e.g. Google's Storebot visiting
      // checkout to collect shipping/tax data for Merchant Center). These
      // aren't real abandoned carts and always have empty billing/shipping.
      if (BOT_UA_PATTERN.test(order.customer_user_agent || "")) {
        console.log(
          `   🤖 Order ${order.id} skipped: bot-created draft (UA: ${order.customer_user_agent})`,
        );
        skipped++;
        continue;
      }

      // Skip carts older than 24 hours — too stale to recover.
      // Use date_created (immutable, set once) NOT date_modified — WooCommerce
      // Blocks checkout silently bumps date_modified on session heartbeats,
      // shipping recalcs, etc, even when the customer isn't actively doing
      // anything, which previously made carts look "fresh" forever.
      const createdAt = new Date(order.date_created);
      const maxAge = new Date(Date.now() - 24 * 60 * 60 * 1000);
      if (createdAt < maxAge) {
        console.log(
          `   ⏭️  Order ${order.id} skipped: too old (created ${order.date_created}, >24h)`,
        );
        skipped++;
        continue;
      }

      // Must have phone number. On this checkout, the Phone field lives
      // under the Shipping section — WC Blocks doesn't always mirror it
      // into billing.phone until the order is actually placed, so we
      // check shipping as a fallback for draft orders.
      const rawPhone = order.billing?.phone || order.shipping?.phone;
      if (!rawPhone) {
        console.log(
          `   ⏭️  Order ${order.id} skipped: no phone number on checkout draft (checked billing.phone and shipping.phone)`,
        );
        skipped++;
        continue;
      }

      const phone = normalizePhone(rawPhone);
      if (!phone) {
        console.log(
          `   ⏭️  Order ${order.id} skipped: phone "${rawPhone}" could not be normalized`,
        );
        skipped++;
        continue;
      }

      const wc_order_id = String(order.id);

      // Check if we're already tracking / have already processed this order.
      // created_at on THIS row is what we treat as "first seen" — it never
      // moves, unlike WooCommerce's date_modified.
      const { data: existing } = await supabase
        .from("woocommerce_cart_recovery")
        .select("id, status, created_at")
        .eq("connection_id", connection.id)
        .eq("wc_order_id", wc_order_id)
        .maybeSingle();

      if (existing && existing.status !== "pending") {
        console.log(
          `   ⏭️  Order ${wc_order_id} skipped: already processed (status: ${existing.status})`,
        );
        skipped++;
        continue;
      }

      // Get product image from line items (already in draft order response)
      const firstItem = order.line_items?.[0];
      const productImageUrl = firstItem?.image?.src || null;

      let cartRecord;

      if (!existing) {
        // First time we've seen this draft order — start tracking it, but
        // don't send yet. This stamps OUR OWN first-seen time via created_at.
        const { data: newRecord, error: insertErr } = await supabase
          .from("woocommerce_cart_recovery")
          .insert({
            user_id: connection.user_id,
            connection_id: connection.id,
            wc_order_id,
            phone_number: phone,
            customer_name:
              `${order.billing?.first_name || order.shipping?.first_name || ""} ${order.billing?.last_name || order.shipping?.last_name || ""}`.trim(),
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
            status: "pending",
          })
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

        console.log(
          `   👀 Order ${wc_order_id} first seen — tracking started, will recheck in ${delayMinutes}min`,
        );
        skipped++;
        continue;
      }

      // We've seen this order before (status === "pending") — check if
      // enough time has passed since OUR first-seen timestamp.
      const firstSeenAt = new Date(existing.created_at);
      const elapsedMinutes = (Date.now() - firstSeenAt.getTime()) / 60000;

      if (elapsedMinutes < delayMinutes) {
        console.log(
          `   ⏭️  Order ${wc_order_id} skipped: too recent (first seen ${existing.created_at}, ${Math.round(elapsedMinutes)}min elapsed, need ${delayMinutes}min)`,
        );
        skipped++;
        continue;
      }

      cartRecord = { id: existing.id };
      await supabase
        .from("woocommerce_cart_recovery")
        .update({ status: "sending", updated_at: new Date().toISOString() })
        .eq("id", existing.id);

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

      // Store in chats/messages so it shows in the chat dashboard —
      // same pattern as order.created / order.updated automations.
      if (sendStatus === "sent") {
        sent++;
        try {
          const templateText = buildTemplateText(template, variables);
          const contactName =
            `${order.billing?.first_name || order.shipping?.first_name || ""} ${order.billing?.last_name || order.shipping?.last_name || ""}`.trim() ||
            "Customer";

          const chatId = await findOrCreateWooChat(
            phone,
            contactName,
            connection.user_id,
            templateText,
          );

          if (chatId) {
            await storeCartRecoveryMessage({
              chatId,
              templateText,
              mediaPath: automation.include_product_image
                ? productImageUrl
                : null,
            });
          } else {
            console.error(
              `   ❌ chatId is null — skipping message insert for order ${wc_order_id}`,
            );
          }
        } catch (chatErr) {
          console.error(
            `   ⚠️  Chat/message storage failed for order ${wc_order_id}:`,
            chatErr.message,
          );
        }
      } else {
        skipped++;
      }
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
    "*/10 * * * *", // Every 10 minutes — tighter than delay_minutes to keep sends on time
    runCartRecoveryCron,
    { timezone: "UTC" },
  );

  console.log("⏰ Cart recovery cron started (every 10 minutes)");
}

// Export for manual testing
export { runCartRecoveryCron };
