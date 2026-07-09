// scheduler/orderReconciliationJob.js
import axios from "axios";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
); // ← use your existing scheduler supabase client
// If your scheduler's supabase client has a different path/name,
// adjust this import to match (same one orderDelayJob.js uses).

const INTERVAL_MS = 15 * 60 * 1000; // every 15 minutes
const LOOKBACK_MINUTES = 30; // only ever look at orders modified in this window
const MAX_ORDER_AGE_HOURS = 2; // extra safety net — never touch older orders

const STATUS_TO_TRIGGER = {
  processing: "order.created",
  completed: "order.completed",
  cancelled: "order.cancelled",
  refunded: "order.refunded",
  shipped: "order.shipped",
  "in-transit": "order.shipped",
  dispatched: "order.shipped",
};

// ─── Helpers ────────────────────────────────────────────────────────────────

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

function isOrderTooOld(order) {
  const created = new Date(order.date_created);
  if (isNaN(created.getTime())) return true;
  const ageHours = (Date.now() - created.getTime()) / (1000 * 60 * 60);
  return ageHours > MAX_ORDER_AGE_HOURS;
}

function getTrackingUrl(order, fallbackUrl = null) {
  const meta = order.meta_data || [];
  const trackUrl =
    meta.find((m) => m.key === "track_url")?.value ||
    meta.find((m) => m.key === "_shipment_track_url")?.value ||
    (meta.find((m) => m.key === "awb")?.value
      ? `https://shiprocket.co/tracking/${meta.find((m) => m.key === "awb")?.value}`
      : null) ||
    fallbackUrl ||
    null;
  return trackUrl;
}

function buildTemplateVariables(order, variableMap, connection) {
  const metaData = order.meta_data || [];
  const awbValue =
    metaData.find((m) => m.key === "awb")?.value ||
    (() => {
      const trackUrl = metaData.find((m) => m.key === "track_url")?.value;
      return trackUrl ? trackUrl.split("/").pop() : "";
    })() ||
    "";

  const productUrl =
    order.line_items?.[0]?.permalink ||
    (() => {
      const name = order.line_items?.[0]?.name;
      const storeUrl = connection?.store_url || "";
      if (name && storeUrl) {
        const slug = name
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .replace(/^-|-$/g, "");
        return `${storeUrl}/product/${slug}/`;
      }
      return "";
    })();

  const orderFields = {
    order_number: String(order.number || order.id),
    order_id: String(order.id),
    total: `${order.currency_symbol || "₹"}${order.total}`,
    subtotal: String(order.subtotal || ""),
    status: order.status,
    payment_method: order.payment_method_title || "",
    billing_first_name: order.billing?.first_name || "",
    billing_last_name: order.billing?.last_name || "",
    billing_full_name:
      `${order.billing?.first_name || ""} ${order.billing?.last_name || ""}`.trim(),
    billing_email: order.billing?.email || "",
    billing_phone: order.billing?.phone || "",
    shipping_address:
      [
        order.shipping?.address_1,
        order.shipping?.city,
        order.shipping?.state,
        order.shipping?.postcode,
      ]
        .filter(Boolean)
        .join(", ") || "",
    order_date: new Date(order.date_created).toLocaleDateString("en-IN"),
    item_names: (order.line_items || []).map((i) => i.name).join(", "),
    tracking_url: getTrackingUrl(order) || "",
    awb_number: awbValue,
    tracking_number: awbValue,
    product_url: productUrl,
    cart_total: `${order.currency_symbol || "₹"}${order.total}`,
  };

  const variables = {};
  for (const [position, fieldName] of Object.entries(variableMap)) {
    variables[position] = orderFields[fieldName] ?? "";
  }
  return variables;
}

function buildWhatsAppPayload(
  template,
  phoneNumber,
  variables,
  trackingUrl = null,
) {
  let components = template.components;
  if (typeof components === "string") {
    try {
      components = JSON.parse(components);
    } catch {
      components = [];
    }
  }

  const hasUrlButton = components.some(
    (c) => c.type === "BUTTONS" && c.buttons?.some((b) => b.type === "URL"),
  );

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

  if (variables && Object.keys(variables).length > 0) {
    messageBody.template.components.push({
      type: "body",
      parameters: Object.values(variables).map((value) => ({
        type: "text",
        text: String(value) || " ",
      })),
    });
  }

  if (hasUrlButton && trackingUrl) {
    const urlParts = trackingUrl.split("/");
    const awbOrSuffix = urlParts[urlParts.length - 1] || trackingUrl;
    messageBody.template.components.push({
      type: "button",
      sub_type: "url",
      index: "0",
      parameters: [{ type: "text", text: awbOrSuffix }],
    });
  }

  return messageBody;
}

async function sendMissedOrderMessage(automation, order, phone, connection) {
  const logEntry = {
    automation_id: automation.id,
    user_id: connection.user_id,
    connection_id: connection.id,
    trigger_event: automation.trigger_event,
    wc_order_id: String(order.id),
    wc_customer_id: String(order.customer_id || ""),
    phone_number: phone,
    status: "pending",
  };

  try {
    const template = automation.whatsapp_templates;
    const account = automation.whatsapp_accounts;
    if (!template || !account) throw new Error("Template or account missing");

    const templateVariables = buildTemplateVariables(
      order,
      automation.template_variable_map || {},
      connection,
    );

    const trackingUrl = getTrackingUrl(
      order,
      automation.shipping_fallback_url || null,
    );

    const messageBody = buildWhatsAppPayload(
      template,
      phone,
      templateVariables,
      trackingUrl,
    );

    const waResponse = await axios.post(
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

    const wa_message_id = waResponse.data.messages?.[0]?.id;

    const { data: wmRecord } = await supabase
      .from("whatsapp_messages")
      .insert({
        account_id: account.wa_id,
        to_number: phone,
        template_name: template.name,
        message_body: messageBody,
        wa_message_id,
        status: "sent",
        sent_at: new Date().toISOString(),
      })
      .select()
      .single();

    await supabase.from("woocommerce_automation_logs").insert({
      ...logEntry,
      wm_id: wmRecord?.wm_id,
      status: "sent",
      sent_at: new Date().toISOString(),
      error_message: "Recovered via reconciliation (original webhook missed)",
    });

    console.log(`      ✅ Recovered order #${order.id} → sent to ${phone}`);
  } catch (err) {
    const errMsg = err.response?.data?.error?.message || err.message;
    console.error(
      `      ❌ Reconciliation send failed for order #${order.id}: ${errMsg}`,
    );
    await supabase.from("woocommerce_automation_logs").insert({
      ...logEntry,
      status: "failed",
      error_message: `Reconciliation attempt failed: ${errMsg}`,
    });
  }
}

// ─── Main reconciliation logic ───────────────────────────────────────────────

async function reconcileOrder(order, connection) {
  if (isOrderTooOld(order)) return;

  const triggerEvent = STATUS_TO_TRIGGER[order.status];
  if (!triggerEvent) return;

  const { data: existingLog } = await supabase
    .from("woocommerce_automation_logs")
    .select("id")
    .eq("connection_id", connection.id)
    .eq("wc_order_id", String(order.id))
    .eq("trigger_event", triggerEvent)
    .maybeSingle();

  if (existingLog) return; // already handled — nothing to do

  const { data: automations } = await supabase
    .from("woocommerce_automations")
    .select(`*, whatsapp_templates (*), whatsapp_accounts (*)`)
    .eq("connection_id", connection.id)
    .eq("trigger_event", triggerEvent)
    .eq("is_active", true);

  if (!automations || automations.length === 0) return;

  const phone = normalizePhone(order.billing?.phone);
  if (!phone) return;

  console.log(
    `   🔁 Missed order detected: #${order.id} (${triggerEvent}) — recovering...`,
  );

  for (const automation of automations) {
    await sendMissedOrderMessage(automation, order, phone, connection);
  }
}

async function reconcileConnection(connection) {
  try {
    const client = axios.create({
      baseURL: `${connection.store_url.replace(/\/$/, "")}/wp-json/wc/v3`,
      auth: {
        username: connection.consumer_key,
        password: connection.consumer_secret,
      },
      timeout: 20000,
    });

    const sinceIso = new Date(
      Date.now() - LOOKBACK_MINUTES * 60 * 1000,
    ).toISOString();

    const res = await client.get("/orders", {
      params: {
        modified_after: sinceIso,
        per_page: 50,
        orderby: "modified",
        order: "desc",
      },
    });

    const recentOrders = res.data || [];
    if (recentOrders.length === 0) return;

    console.log(
      `   📦 ${connection.store_name || connection.store_url}: ${recentOrders.length} order(s) modified in last ${LOOKBACK_MINUTES} min`,
    );

    for (const order of recentOrders) {
      await reconcileOrder(order, connection);
    }
  } catch (err) {
    console.error(
      `   ❌ Reconciliation failed for ${connection.store_name || connection.store_url}: ${err.message}`,
    );
  }
}

export async function runReconciliation() {
  console.log("\n🔄 Reconciliation check starting...");
  try {
    const { data: connections, error } = await supabase
      .from("user_woocommerce_connections")
      .select("*")
      .eq("is_active", true);

    if (error) {
      console.error("   ❌ Failed to load connections:", error.message);
      return;
    }
    if (!connections || connections.length === 0) {
      console.log("   ℹ️  No active connections");
      return;
    }

    for (const connection of connections) {
      await reconcileConnection(connection);
    }
  } catch (err) {
    console.error("   ❌ Reconciliation run failed:", err.message);
  }
  console.log("🔄 Reconciliation check complete\n");
}

export function startReconciliationCron() {
  console.log(
    `🔄 Order reconciliation cron scheduled (every 15 min) — starts at ${new Date().toISOString()}`,
  );
  setInterval(() => {
    console.log(`⏰ Reconciliation cron tick at ${new Date().toISOString()}`);
    runReconciliation().catch((err) => {
      console.error("❌ Reconciliation cron error:", err.message);
    });
  }, INTERVAL_MS);
}
