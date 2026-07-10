// scheduler/orderReconciliationJob.js
import axios from "axios";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const INTERVAL_MS = 15 * 60 * 1000; // run every 15 minutes
const LOOKBACK_MINUTES = 30; // only ever look at orders modified in this window
const MAX_ORDER_AGE_HOURS = 2; // extra safety net — never touch older orders
const PLACEHOLDER_IMAGE =
  "https://ygynmoezdffuencztefl.supabase.co/storage/v1/object/public/default_templateImage/randomImg.jpg";

const STATUS_TO_TRIGGER = {
  processing: "order.created",
  completed: "order.completed",
  cancelled: "order.cancelled",
  refunded: "order.refunded",
  shipped: "order.shipped",
  "in-transit": "order.shipped",
  dispatched: "order.shipped",
};

// ─── Small retry helper — protects against flaky network calls ──────────────
async function withRetry(
  fn,
  { retries = 2, delayMs = 1000, label = "request" } = {},
) {
  let lastErr;
  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const retryable =
        err.code === "ECONNRESET" ||
        err.code === "ETIMEDOUT" ||
        err.message?.includes("socket hang up") ||
        err.message?.includes("timeout");
      console.warn(
        `      ⚠️  ${label} attempt ${attempt} failed: ${err.message}${retryable ? " (retrying)" : ""}`,
      );
      if (!retryable || attempt > retries) break;
      await new Promise((r) => setTimeout(r, delayMs * attempt));
    }
  }
  throw lastErr;
}

// ─── Phone normalization ─────────────────────────────────────────────────────
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

function safeParseComponents(template) {
  let components = template.components;
  if (typeof components === "string") {
    try {
      components = JSON.parse(components);
    } catch {
      return [];
    }
  }
  return Array.isArray(components) ? components : [];
}

// ─── Tracking helpers — mirrors main backend exactly ─────────────────────────
function buildCourierUrl(noteText, awb) {
  const text = (noteText || "").toLowerCase();
  if (text.includes("delhivery"))
    return `https://www.delhivery.com/track/package/${awb}`;
  if (text.includes("dtdc"))
    return `https://www.dtdc.in/trace.asp?strCnno=${awb}`;
  if (text.includes("xpressbees"))
    return `https://www.xpressbees.com/shipment/tracking?awb=${awb}`;
  if (text.includes("bluedart") || text.includes("blue dart"))
    return `https://www.bluedart.com/tracking?trackFor=0&field1=${awb}`;
  if (text.includes("ekart"))
    return `https://ekartlogistics.com/shipment-details/${awb}`;
  return `https://shiprocket.co/tracking/${awb}`;
}

function parseTrackingFromNotes(notes) {
  for (const note of notes || []) {
    const text = note.note || "";
    const urlMatch = text.match(/https?:\/\/[^\s<"]+tracking[^\s<"]+/i);
    if (urlMatch) {
      return { trackUrl: urlMatch[0], awb: urlMatch[0].split("/").pop() };
    }
    const trackingMatch = text.match(/[Tt]racking[:\s#]+([A-Z0-9]{8,20})/);
    if (trackingMatch) {
      const awb = trackingMatch[1];
      return { trackUrl: buildCourierUrl(text, awb), awb };
    }
  }
  return null;
}

async function getOrderNotes(order, client) {
  try {
    const res = await withRetry(() => client.get(`/orders/${order.id}/notes`), {
      retries: 2,
      delayMs: 1000,
      label: `order notes #${order.id}`,
    });
    return res.data || [];
  } catch {
    return [];
  }
}

function getTrackingUrl(order, orderNotes, fallbackUrl = null) {
  const meta = order.meta_data || [];

  const metaTrackUrl =
    meta.find((m) => m.key === "track_url")?.value ||
    meta.find((m) => m.key === "_shipment_track_url")?.value ||
    (meta.find((m) => m.key === "awb")?.value
      ? `https://shiprocket.co/tracking/${meta.find((m) => m.key === "awb")?.value}`
      : null);

  if (metaTrackUrl) return metaTrackUrl;

  if (orderNotes?.length > 0) {
    const parsed = parseTrackingFromNotes(orderNotes);
    if (parsed?.trackUrl) return parsed.trackUrl;
  }

  return fallbackUrl || null;
}

// ─── Product image resolver — API fallback like main backend ────────────────
async function resolveProductImage(order, client) {
  const fromPayload = order.line_items?.[0]?.image?.src;
  if (fromPayload) return fromPayload;

  const productId = order.line_items?.[0]?.product_id;
  if (!productId) return null;

  try {
    const res = await withRetry(() => client.get(`/products/${productId}`), {
      retries: 2,
      delayMs: 1000,
      label: `product image #${productId}`,
    });
    return res.data?.images?.[0]?.src || null;
  } catch {
    return null;
  }
}

// ─── Template variable builder — mirrors main backend fields ────────────────
function buildTemplateVariables(
  order,
  variableMap,
  connection,
  orderNotes,
  trackingUrlValue,
) {
  const metaData = order.meta_data || [];

  const awbFromMeta =
    metaData.find((m) => m.key === "awb")?.value ||
    metaData.find((m) => m.key === "_shipment_awb_code")?.value ||
    (() => {
      const trackUrl =
        metaData.find((m) => m.key === "track_url")?.value ||
        metaData.find((m) => m.key === "_shipment_track_url")?.value;
      return trackUrl ? trackUrl.split("/").pop() : "";
    })() ||
    "";

  const awbFromNotes =
    !awbFromMeta && orderNotes?.length > 0
      ? parseTrackingFromNotes(orderNotes)?.awb || ""
      : "";

  const awbValue = awbFromMeta || awbFromNotes;

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
    item_count: String((order.line_items || []).length),
    tracking_url: trackingUrlValue || "",
    awb_number: awbValue,
    tracking_number: awbValue,
    product_url: productUrl,
    cart_total: `${order.currency_symbol || "₹"}${order.total}`,
  };

  const variables = {};
  for (const [position, fieldName] of Object.entries(variableMap || {})) {
    // never send empty string — Meta rejects it (error 131008)
    variables[position] = orderFields[fieldName] || " ";
  }
  return variables;
}

// ─── WhatsApp payload builder — handles IMAGE header + URL button always ────
function buildWhatsAppPayload(
  template,
  phoneNumber,
  variables,
  mediaUrl,
  trackingUrl,
) {
  const components = safeParseComponents(template);
  const headerComp = components.find((c) => c.type === "HEADER");
  const needsImage = headerComp?.format === "IMAGE";
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

  if (needsImage) {
    // template REQUIRES an image — always send one, fall back to placeholder
    const imageLink = mediaUrl || PLACEHOLDER_IMAGE;
    messageBody.template.components.push({
      type: "header",
      parameters: [{ type: "image", image: { link: imageLink } }],
    });
  }

  if (variables && Object.keys(variables).length > 0) {
    messageBody.template.components.push({
      type: "body",
      parameters: Object.values(variables).map((value) => ({
        type: "text",
        text: String(value) || " ",
      })),
    });
  }

  if (hasUrlButton) {
    // template REQUIRES a button param — always send one, fall back to space
    let buttonSuffix = " ";
    if (trackingUrl) {
      const parts = trackingUrl.split("/");
      buttonSuffix = parts[parts.length - 1] || " ";
    }
    messageBody.template.components.push({
      type: "button",
      sub_type: "url",
      index: "0",
      parameters: [{ type: "text", text: buttonSuffix }],
    });
  }

  return { messageBody, needsImage };
}

// ─── Chat dashboard integration — mirrors main backend ──────────────────────
function extractTemplateText(template, variables = {}) {
  try {
    const components = safeParseComponents(template);
    const parts = [];
    const header = components.find((c) => c.type === "HEADER");
    if (header?.format === "TEXT" && header?.text)
      parts.push(`*${header.text}*`);
    if (header?.format === "IMAGE") parts.push("🖼️ [Product Image]");

    const body = components.find((c) => c.type === "BODY");
    if (body?.text) {
      let bodyText = body.text;
      Object.entries(variables).forEach(([pos, value]) => {
        bodyText = bodyText.replaceAll(
          `{{${pos}}}`,
          value?.trim() || `{{${pos}}}`,
        );
      });
      parts.push(bodyText);
    }

    const footer = components.find((c) => c.type === "FOOTER");
    if (footer?.text) parts.push(`_${footer.text}_`);

    const buttons = components.find((c) => c.type === "BUTTONS");
    if (buttons?.buttons?.length > 0)
      buttons.buttons.forEach((b) => parts.push(`[${b.text}]`));

    return parts.join("\n\n") || `Template: ${template.name}`;
  } catch {
    return `Template: ${template.name}`;
  }
}

async function storeInChatDashboard(
  order,
  template,
  templateVariables,
  mediaUrl,
  userId,
  phone,
) {
  try {
    const contactName =
      `${order.billing?.first_name || ""} ${order.billing?.last_name || ""}`.trim() ||
      "Customer";
    const templateText = extractTemplateText(template, templateVariables);

    const { data: existingChats } = await supabase
      .from("chats")
      .select("chat_id, person_name")
      .eq("phone_number", phone)
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(1);

    let chatId;
    if (existingChats?.length > 0) {
      chatId = existingChats[0].chat_id;
      await supabase
        .from("chats")
        .update({
          last_message: templateText,
          last_message_at: new Date().toISOString(),
          last_admin_message_at: new Date().toISOString(),
          last_sender_type: "admin",
          person_name:
            contactName || existingChats[0].person_name || "Customer",
          updated_at: new Date().toISOString(),
        })
        .eq("chat_id", chatId);
    } else {
      const { data: newChat } = await supabase
        .from("chats")
        .insert({
          phone_number: phone,
          person_name: contactName,
          last_message: templateText,
          last_message_at: new Date().toISOString(),
          last_sender_type: "admin",
          last_admin_message_at: new Date().toISOString(),
          mode: "AUTO",
          user_id: userId,
          status: "active",
          unread_count: 0,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        })
        .select()
        .single();
      chatId = newChat?.chat_id;
    }

    if (chatId) {
      const components = safeParseComponents(template);
      const btnComp = components.find((c) => c.type === "BUTTONS");
      const buttonsValue =
        btnComp?.buttons?.length > 0 ? btnComp.buttons : null;

      await supabase.from("messages").insert({
        chat_id: chatId,
        sender_type: "admin",
        message: templateText,
        message_type: "template",
        media_path: mediaUrl || null,
        buttons: buttonsValue,
        created_at: new Date().toISOString(),
      });
    }
  } catch (err) {
    console.warn(
      `      ⚠️  Chat dashboard store failed (non-fatal): ${err.message}`,
    );
  }
}

// ─── Send one missed order's message ─────────────────────────────────────────
async function sendMissedOrderMessage(
  automation,
  order,
  phone,
  connection,
  client,
) {
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
    if (!template || !account)
      throw new Error("Template or WhatsApp account missing on automation");

    // Fetch order notes only for shipped events (tracking fallback)
    let orderNotes = [];
    if (automation.trigger_event === "order.shipped") {
      orderNotes = await getOrderNotes(order, client);
    }

    const trackingUrl = getTrackingUrl(
      order,
      orderNotes,
      automation.shipping_fallback_url || null,
    );

    const templateVariables = buildTemplateVariables(
      order,
      automation.template_variable_map || {},
      connection,
      orderNotes,
      trackingUrl,
    );

    // Resolve product image if needed
    let mediaUrl = null;
    if (automation.include_product_image) {
      mediaUrl = await resolveProductImage(order, client);
    }

    const { messageBody } = buildWhatsAppPayload(
      template,
      phone,
      templateVariables,
      mediaUrl,
      trackingUrl,
    );

    const waResponse = await withRetry(
      () =>
        axios.post(
          `https://graph.facebook.com/v21.0/${account.phone_number_id}/messages`,
          messageBody,
          {
            headers: {
              Authorization: `Bearer ${account.system_user_access_token}`,
              "Content-Type": "application/json",
            },
            timeout: 30000,
          },
        ),
      { retries: 2, delayMs: 1500, label: "Meta send message" },
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

    // Best-effort chat dashboard sync — never blocks the send result
    await storeInChatDashboard(
      order,
      template,
      templateVariables,
      mediaUrl,
      connection.user_id,
      phone,
    );

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

// ─── Per-order reconciliation check ──────────────────────────────────────────
async function reconcileOrder(order, connection, client) {
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

  if (existingLog) return; // already handled — skip

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
    await sendMissedOrderMessage(automation, order, phone, connection, client);
  }
}

// ─── Per-connection reconciliation ───────────────────────────────────────────
async function reconcileConnection(connection) {
  try {
    const client = axios.create({
      baseURL: `${connection.store_url.replace(/\/$/, "")}/wp-json/wc/v3`,
      auth: {
        username: connection.consumer_key,
        password: connection.consumer_secret,
      },
      timeout: 25000,
    });

    const sinceIso = new Date(
      Date.now() - LOOKBACK_MINUTES * 60 * 1000,
    ).toISOString();

    const res = await withRetry(
      () =>
        client.get("/orders", {
          params: {
            modified_after: sinceIso,
            per_page: 50,
            orderby: "modified",
            order: "desc",
          },
        }),
      {
        retries: 2,
        delayMs: 1500,
        label: `fetch orders (${connection.store_name || connection.store_url})`,
      },
    );

    const recentOrders = res.data || [];
    if (recentOrders.length === 0) return;

    console.log(
      `   📦 ${connection.store_name || connection.store_url}: ${recentOrders.length} order(s) modified in last ${LOOKBACK_MINUTES} min`,
    );

    for (const order of recentOrders) {
      try {
        await reconcileOrder(order, connection, client);
      } catch (err) {
        // one bad order should never stop the rest from being checked
        console.error(
          `   ❌ Failed reconciling order #${order?.id}: ${err.message}`,
        );
      }
    }
  } catch (err) {
    console.error(
      `   ❌ Reconciliation failed for ${connection.store_name || connection.store_url}: ${err.message}`,
    );
  }
}

// ─── Main entry point ─────────────────────────────────────────────────────────
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

let isRunning = false; // ✅ prevents overlapping runs if one takes longer than 15 min

export function startReconciliationCron() {
  console.log("🔄 Order reconciliation cron scheduled (every 15 min)");
  setInterval(() => {
    if (isRunning) {
      console.log("⏭️  Skipping tick — previous reconciliation still running");
      return;
    }
    isRunning = true;
    runReconciliation()
      .catch((err) =>
        console.error("❌ Reconciliation cron error:", err.message),
      )
      .finally(() => {
        isRunning = false;
      });
  }, INTERVAL_MS);
}
