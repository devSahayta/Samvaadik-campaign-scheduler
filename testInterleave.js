// testInterleave.js
// Tests that a small campaign finishes BEFORE a big campaign completes
// when both run on the same account at the same time.
//
// Run from scheduler/ folder:
//   node testInterleave.js
//
// No real Supabase. No real WhatsApp API. Fully self-contained.

// ─── Colours ─────────────────────────────────────────────────────────────────
const G = (s) => `\x1b[32m${s}\x1b[0m`; // green
const R = (s) => `\x1b[31m${s}\x1b[0m`; // red
const Y = (s) => `\x1b[33m${s}\x1b[0m`; // yellow
const C = (s) => `\x1b[36m${s}\x1b[0m`; // cyan
const B = (s) => `\x1b[1m${s}\x1b[0m`; // bold
const D = (s) => `\x1b[2m${s}\x1b[0m`; // dim

// ─── Timeline tracker ────────────────────────────────────────────────────────
// Records exactly when each message was "sent" so we can prove interleaving
const timeline = [];

function record(campaignName, phone, batchNum) {
  timeline.push({
    time: Date.now(),
    campaign: campaignName,
    phone,
    batch: batchNum,
  });
}

// ─── Mock DB ─────────────────────────────────────────────────────────────────
let db = {
  campaigns: [],
  campaign_messages: [],
  whatsapp_accounts: [],
  whatsapp_templates: [],
  whatsapp_messages: [],
  chats: [],
  messages: [],
};

function resetDB(data) {
  db = {
    campaigns: data.campaigns || [],
    campaign_messages: data.campaign_messages || [],
    whatsapp_accounts: data.whatsapp_accounts || [],
    whatsapp_templates: data.whatsapp_templates || [],
    whatsapp_messages: [],
    chats: [],
    messages: [],
  };
  timeline.length = 0;
}

// ─── Mock Supabase ────────────────────────────────────────────────────────────
function buildMockSupabase() {
  function query(table) {
    let _filters = [];
    let _updates = null;
    let _inserts = null;
    let _range = null;
    let _limit = null;
    let _order = null;
    let _count = false;
    let _head = false;

    const q = {
      select(cols, opts = {}) {
        if (opts.count === "exact") _count = true;
        if (opts.head) _head = true;
        return q;
      },
      eq(col, val) {
        _filters.push({ op: "eq", col, val });
        return q;
      },
      in(col, vals) {
        _filters.push({ op: "in", col, vals });
        return q;
      },
      lte(col, val) {
        _filters.push({ op: "lte", col, val });
        return q;
      },
      not(col, op, val) {
        _filters.push({ op: "not", col, nop: op, val });
        return q;
      },
      order(col, opts = {}) {
        _order = { col, asc: opts.ascending !== false };
        return q;
      },
      limit(n) {
        _limit = n;
        return q;
      },
      range(f, t) {
        _range = { f, t };
        return q;
      },
      insert(data) {
        _inserts = Array.isArray(data) ? data : [data];
        return q;
      },
      update(data) {
        _updates = data;
        return q;
      },
      delete() {
        _updates = "__delete__";
        return q;
      },
      single() {
        return Promise.resolve(run(true));
      },
      then(res) {
        return Promise.resolve(run(false)).then(res);
      },
    };

    function applyFilters(rows) {
      for (const f of _filters) {
        if (f.op === "eq") rows = rows.filter((r) => r[f.col] === f.val);
        if (f.op === "in") rows = rows.filter((r) => f.vals.includes(r[f.col]));
        if (f.op === "lte") rows = rows.filter((r) => r[f.col] <= f.val);
        if (f.op === "not" && f.nop === "is")
          rows = rows.filter(
            (r) => r[f.col] !== null && r[f.col] !== undefined,
          );
      }
      return rows;
    }

    function run(single) {
      const rows = db[table] ? [...db[table]] : [];

      // INSERT
      if (_inserts) {
        const added = _inserts.map((row) => {
          const r = { ...row };
          if (!r.id) r.id = `id-${Math.random().toString(36).slice(2)}`;
          db[table].push(r);
          return r;
        });
        return single
          ? { data: added[0], error: null }
          : { data: added, error: null };
      }

      // DELETE
      if (_updates === "__delete__") {
        const matched = applyFilters([...rows]);
        const ids = new Set(
          matched.map((r) => r.campaign_id || r.cm_id || r.wa_id || r.id),
        );
        db[table] = db[table].filter(
          (r) => !ids.has(r.campaign_id || r.cm_id || r.wa_id || r.id),
        );
        return { data: null, error: null };
      }

      // UPDATE
      if (_updates) {
        const matched = applyFilters([...rows]);
        for (const row of db[table]) {
          if (
            matched.some(
              (m) =>
                (m.campaign_id && m.campaign_id === row.campaign_id) ||
                (m.cm_id && m.cm_id === row.cm_id) ||
                (m.wa_id && m.wa_id === row.wa_id) ||
                (m.id && m.id === row.id),
            )
          ) {
            Object.assign(row, _updates);
          }
        }
        const fresh = applyFilters([...db[table]]);
        return single
          ? { data: fresh[0] || null, error: null }
          : { data: fresh, error: null };
      }

      // SELECT
      let result = applyFilters(rows);
      if (_order) {
        result.sort((a, b) => {
          const av = a[_order.col],
            bv = b[_order.col];
          return _order.asc
            ? av < bv
              ? -1
              : av > bv
                ? 1
                : 0
            : av > bv
              ? -1
              : av < bv
                ? 1
                : 0;
        });
      }
      if (_limit !== null) result = result.slice(0, _limit);
      if (_range) result = result.slice(_range.f, _range.t + 1);
      if (_count && _head) return { count: result.length, error: null };
      return single
        ? { data: result[0] || null, error: null }
        : { data: result, error: null };
    }

    return q;
  }

  return { from: (t) => query(t) };
}

// ─── Mock WhatsApp API ────────────────────────────────────────────────────────
// Adds a small realistic delay per message so the timeline is meaningful
let currentBatchTracker = {}; // campaignId -> batchNumber

async function mockWhatsAppSend(url, body) {
  const phone = body?.to || "unknown";

  // Simulate a real API call taking 100-300ms
  await new Promise((r) => setTimeout(r, 100 + Math.random() * 200));

  return {
    data: { messages: [{ id: `wamid.mock.${phone}.${Date.now()}` }] },
  };
}

// ─── Inline scheduler (same logic as campaignScheduler.js, mocks injected) ───
function buildScheduler(supabase, axiosPost) {
  const BATCH_SIZE = 5;
  const BATCH_DELAY = 500; // 500ms instead of 10s so test runs fast
  const MSG_DELAY_MIN = 50; // 50ms instead of 800ms
  const MSG_DELAY_MAX = 150; // 150ms instead of 2000ms

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const rand = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

  function getWarmupLimit(acct) {
    if (!acct.warmup_enabled || acct.warmup_completed) return null;
    const limits = acct.warmup_limits || [200, 500, 1000];
    const stage = acct.warmup_stage || 1;
    const progress = acct.warmup_stage_progress || 0;
    return Math.max(0, limits[stage - 1] - progress);
  }

  async function fetchPending(campaignId, limit) {
    let all = [],
      page = 0;
    const pageSize = 1000;
    while (true) {
      const canFetch =
        limit != null ? Math.min(pageSize, limit - all.length) : pageSize;
      if (canFetch <= 0) break;
      const { data: msgs } = await supabase
        .from("campaign_messages")
        .select("*")
        .eq("campaign_id", campaignId)
        .eq("status", "pending")
        .range(page * pageSize, page * pageSize + canFetch - 1);
      if (!msgs || msgs.length === 0) break;
      all = all.concat(msgs);
      if (msgs.length < canFetch) break;
      page++;
    }
    return all;
  }

  async function sendBatch(acct, template, messages, campaign, batchNum) {
    let sent = 0,
      failed = 0;
    for (let i = 0; i < messages.length; i++) {
      const msg = messages[i];
      try {
        const res = await axiosPost(
          `https://graph.facebook.com/v21.0/${acct.phone_number_id}/messages`,
          {
            to: msg.phone_number,
            type: "template",
            template: {
              name: template.name,
              language: { code: template.language },
            },
          },
        );
        const wa_id = res.data.messages?.[0]?.id;

        await supabase
          .from("campaign_messages")
          .update({
            status: "sent",
            sent_at: new Date().toISOString(),
            wa_message_id: wa_id,
          })
          .eq("cm_id", msg.cm_id);

        // Record in timeline for assertions
        record(campaign.campaign_name, msg.phone_number, batchNum);
        sent++;
      } catch (err) {
        await supabase
          .from("campaign_messages")
          .update({
            status: "failed",
            failed_at: new Date().toISOString(),
            error_message: err.message,
          })
          .eq("cm_id", msg.cm_id);
        failed++;
      }
      if (i < messages.length - 1)
        await sleep(rand(MSG_DELAY_MIN, MSG_DELAY_MAX));
    }
    return { sent, failed };
  }

  async function finalize(campaign, sentThisTick, failedThisTick, wasLimited) {
    const totalSent = sentThisTick;
    const totalFailed = failedThisTick;

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
      return;
    }

    const { count: left } = await supabase
      .from("campaign_messages")
      .select("cm_id", { count: "exact", head: true })
      .eq("campaign_id", campaign.campaign_id)
      .eq("status", "pending");

    if ((left || 0) > 0) {
      await supabase
        .from("campaigns")
        .update({
          status: "scheduled",
          messages_sent: totalSent,
          messages_failed: totalFailed,
          updated_at: new Date().toISOString(),
        })
        .eq("campaign_id", campaign.campaign_id);
    } else {
      await supabase
        .from("campaigns")
        .update({
          status: "completed",
          completed_at: new Date().toISOString(),
          messages_sent: totalSent,
          messages_failed: totalFailed,
        })
        .eq("campaign_id", campaign.campaign_id);
    }
  }

  async function processAccountCampaigns(accountId, campaigns) {
    const { data: acct } = await supabase
      .from("whatsapp_accounts")
      .select("*")
      .eq("wa_id", accountId)
      .single();
    if (!acct) return;

    // Mark all processing
    await Promise.all(
      campaigns.map((c) =>
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

    // Build queues
    const queues = [];
    for (const campaign of campaigns) {
      const { data: tmpl } = await supabase
        .from("whatsapp_templates")
        .select("*")
        .eq("wt_id", campaign.wt_id)
        .single();
      if (!tmpl) {
        await supabase
          .from("campaigns")
          .update({ status: "failed" })
          .eq("campaign_id", campaign.campaign_id);
        continue;
      }
      const limit = getWarmupLimit(acct);
      const pending = await fetchPending(campaign.campaign_id, limit);
      if (pending.length === 0) {
        await finalize(campaign, 0, 0, false);
        continue;
      }
      queues.push({ campaign, tmpl, pending, sent: 0, failed: 0, batchNum: 0 });
    }

    if (queues.length === 0) return;

    // Round-robin
    let roundIdx = 0,
      emptyRounds = 0;
    while (true) {
      if (!queues.some((q) => q.pending.length > 0)) break;

      const queue = queues[roundIdx % queues.length];
      roundIdx++;

      if (queue.pending.length === 0) {
        emptyRounds++;
        if (emptyRounds >= queues.length) break;
        continue;
      }
      emptyRounds = 0;

      queue.batchNum++;
      const batch = queue.pending.splice(0, BATCH_SIZE);

      console.log(
        Y(`\n  ► [${queue.campaign.campaign_name}]`) +
          ` batch ${queue.batchNum} of ${batch.length} msg` +
          D(` — ${queue.pending.length} remain`),
      );

      const { sent, failed } = await sendBatch(
        acct,
        queue.tmpl,
        batch,
        queue.campaign,
        queue.batchNum,
      );
      queue.sent += sent;
      queue.failed += failed;

      if (queues.some((q) => q.pending.length > 0)) {
        console.log(D(`     batch delay ${BATCH_DELAY}ms...`));
        await sleep(BATCH_DELAY);
      }
    }

    for (const q of queues) {
      await finalize(q.campaign, q.sent, q.failed, q.pending.length > 0);
    }
  }

  async function checkAndSendCampaigns() {
    const { data: campaigns } = await supabase
      .from("campaigns")
      .select("*")
      .in("status", ["scheduled", "processing"])
      .lte("scheduled_at", new Date().toISOString())
      .order("scheduled_at", { ascending: true });

    if (!campaigns || campaigns.length === 0) {
      console.log("No campaigns ready");
      return;
    }

    const byAccount = new Map();
    for (const c of campaigns) {
      if (!byAccount.has(c.account_id)) byAccount.set(c.account_id, []);
      byAccount.get(c.account_id).push(c);
    }

    await Promise.all(
      [...byAccount.entries()].map(([id, list]) =>
        processAccountCampaigns(id, list).catch((err) =>
          console.error(`Account ${id} error:`, err.message),
        ),
      ),
    );
  }

  return { checkAndSendCampaigns };
}

// ─── Data helpers ─────────────────────────────────────────────────────────────
function makeAccount(id) {
  return {
    wa_id: id,
    business_phone_number: `+91${id}`,
    phone_number_id: `phone-${id}`,
    system_user_access_token: `token-${id}`,
    messaging_limit_tier: "TIER_2K",
    messaging_limit_per_day: 2000,
    warmup_enabled: false,
    warmup_completed: true,
    warmup_stage: 1,
    warmup_stage_progress: 0,
    warmup_limits: [200, 500, 1000],
    warmup_daily_sent: 0,
    tier_daily_sent: 0,
    status: "active",
  };
}

function makeTemplate(id) {
  return {
    wt_id: id,
    name: `template_${id}`,
    language: "en_US",
    components: JSON.stringify([{ type: "BODY", text: "Hello!" }]),
    status: "APPROVED",
  };
}

function makeCampaign(id, accountId, templateId, name) {
  return {
    campaign_id: id,
    campaign_name: name,
    account_id: accountId,
    wt_id: templateId,
    group_id: `group-${id}`,
    user_id: "user-1",
    scheduled_at: new Date(Date.now() - 5000).toISOString(),
    status: "scheduled",
    messages_sent: 0,
    messages_failed: 0,
    total_recipients: 0,
    started_at: null,
    template_variables: {},
    media_id: null,
  };
}

function makeMessages(campaignId, count) {
  return Array.from({ length: count }, (_, i) => ({
    cm_id: `cm-${campaignId}-${i}`,
    campaign_id: campaignId,
    contact_id: `contact-${i}`,
    phone_number: `91980000${String(i).padStart(4, "0")}`,
    contact_name: `Contact ${i}`,
    status: "pending",
    sent_at: null,
    wa_message_id: null,
  }));
}

function getStatus(campaignId) {
  const c = db.campaigns.find((x) => x.campaign_id === campaignId);
  const msgs = db.campaign_messages.filter((x) => x.campaign_id === campaignId);
  return {
    status: c?.status,
    sent: msgs.filter((m) => m.status === "sent").length,
    failed: msgs.filter((m) => m.status === "failed").length,
    pending: msgs.filter((m) => m.status === "pending").length,
    messages_sent: c?.messages_sent,
  };
}

// ─── Assertions ───────────────────────────────────────────────────────────────
let pass = 0,
  fail = 0;
function assert(label, ok, detail = "") {
  if (ok) {
    pass++;
    console.log(`  ${G("PASS")} ${label}`);
  } else {
    fail++;
    console.log(
      `  ${R("FAIL")} ${label}` + (detail ? `\n       ${D(detail)}` : ""),
    );
  }
}

// ─── MAIN TEST ────────────────────────────────────────────────────────────────
async function main() {
  console.log(B("\n════════════════════════════════════════════════"));
  console.log(B("  Interleave Test — Big + Small Campaign"));
  console.log(B("════════════════════════════════════════════════"));
  console.log("  Same account, scheduled at same time.");
  console.log("  Small campaign should FINISH FIRST.\n");

  const ACCOUNT_ID = "acct-main";
  const BIG_CAMP_ID = "camp-big";
  const SML_CAMP_ID = "camp-small";
  const TMPL_A = "tmpl-a";
  const TMPL_B = "tmpl-b";

  const BIG_COUNT = 10; // needs 2 batches of 5
  const SML_COUNT = 3; // needs 1 batch of 3

  resetDB({
    whatsapp_accounts: [makeAccount(ACCOUNT_ID)],
    whatsapp_templates: [makeTemplate(TMPL_A), makeTemplate(TMPL_B)],
    campaigns: [
      makeCampaign(
        BIG_CAMP_ID,
        ACCOUNT_ID,
        TMPL_A,
        "Big Campaign (10 contacts)",
      ),
      makeCampaign(
        SML_CAMP_ID,
        ACCOUNT_ID,
        TMPL_B,
        "Small Campaign (3 contacts)",
      ),
    ],
    campaign_messages: [
      ...makeMessages(BIG_CAMP_ID, BIG_COUNT),
      ...makeMessages(SML_CAMP_ID, SML_COUNT),
    ],
  });

  const supabase = buildMockSupabase();
  const scheduler = buildScheduler(supabase, mockWhatsAppSend);

  // ── Run the scheduler tick ────────────────────────────────────────────────
  const startTime = Date.now();
  console.log(C("⚙️  Running scheduler tick...\n"));
  await scheduler.checkAndSendCampaigns();
  const duration = ((Date.now() - startTime) / 1000).toFixed(1);

  // ── Print timeline ────────────────────────────────────────────────────────
  console.log(B("\n\n════════════════════════════════════════════════"));
  console.log(B("  Send Timeline (chronological)"));
  console.log(B("════════════════════════════════════════════════"));

  const t0 = timeline[0]?.time || startTime;
  for (const e of timeline) {
    const relMs = e.time - t0;
    const isBig = e.campaign.includes("Big");
    const colour = isBig ? Y : G;
    console.log(
      `  +${String(relMs).padStart(5)}ms  ` +
        colour(`[${e.campaign}]`) +
        `  ${e.phone}  batch ${e.batch}`,
    );
  }

  // ── Find when each campaign finished ─────────────────────────────────────
  // Last entry for each campaign in the timeline
  const bigEvents = timeline.filter((e) => e.campaign.includes("Big"));
  const smlEvents = timeline.filter((e) => e.campaign.includes("Small"));

  const bigFinishedAt = bigEvents.at(-1)?.time || 0;
  const smlFinishedAt = smlEvents.at(-1)?.time || 0;

  // ── Results ───────────────────────────────────────────────────────────────
  console.log(B("\n\n════════════════════════════════════════════════"));
  console.log(B("  Results"));
  console.log(B("════════════════════════════════════════════════\n"));

  const bigStatus = getStatus(BIG_CAMP_ID);
  const smlStatus = getStatus(SML_CAMP_ID);

  console.log(
    `  Big Campaign:   status=${bigStatus.status}  sent=${bigStatus.sent}  pending=${bigStatus.pending}`,
  );
  console.log(
    `  Small Campaign: status=${smlStatus.status}  sent=${smlStatus.sent}  pending=${smlStatus.pending}`,
  );
  console.log(`  Total duration: ${duration}s\n`);

  // ── Assertions ────────────────────────────────────────────────────────────
  console.log(B("  Assertions\n"));

  assert(
    "Big campaign completed",
    bigStatus.status === "completed",
    `got: ${bigStatus.status}`,
  );

  assert(
    "Small campaign completed",
    smlStatus.status === "completed",
    `got: ${smlStatus.status}`,
  );

  assert(
    `Big campaign sent all ${BIG_COUNT} messages`,
    bigStatus.sent === BIG_COUNT,
    `sent: ${bigStatus.sent}`,
  );

  assert(
    `Small campaign sent all ${SML_COUNT} messages`,
    smlStatus.sent === SML_COUNT,
    `sent: ${smlStatus.sent}`,
  );

  assert(
    "Small campaign finished BEFORE big campaign",
    smlFinishedAt < bigFinishedAt,
    `small last msg at +${smlFinishedAt - t0}ms, big last msg at +${bigFinishedAt - t0}ms`,
  );

  assert(
    "Messages from BOTH campaigns appear in timeline (interleaved)",
    bigEvents.length > 0 && smlEvents.length > 0,
    `big=${bigEvents.length} entries, small=${smlEvents.length} entries`,
  );

  // Check interleaving: small campaign messages should appear BETWEEN big campaign batches
  const firstBigBatch2 = bigEvents.find((e) => e.batch === 2);
  const lastSmlMsg = smlEvents.at(-1);
  assert(
    "Small campaign runs BETWEEN big campaign batches (true interleave)",
    firstBigBatch2 && lastSmlMsg && lastSmlMsg.time < firstBigBatch2.time,
    firstBigBatch2
      ? `small finished at +${lastSmlMsg.time - t0}ms, big batch 2 started at +${firstBigBatch2.time - t0}ms`
      : "big campaign only had 1 batch — increase BIG_COUNT above 5",
  );

  assert(
    "messages_sent counter correct on big campaign",
    bigStatus.messages_sent === BIG_COUNT,
    `got: ${bigStatus.messages_sent}`,
  );

  assert(
    "messages_sent counter correct on small campaign",
    smlStatus.messages_sent === SML_COUNT,
    `got: ${smlStatus.messages_sent}`,
  );

  // ── Summary ───────────────────────────────────────────────────────────────
  const total = pass + fail;
  console.log(
    B(`\n  ${pass}/${total} passed`) +
      (fail > 0 ? R(` — ${fail} FAILED`) : G(" — all good")),
  );
  console.log(B("════════════════════════════════════════════════\n"));

  if (fail > 0) process.exit(1);
}

main().catch((err) => {
  console.error(R("Test crashed:"), err);
  process.exit(1);
});
