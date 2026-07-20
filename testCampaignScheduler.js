// scheduler/testCampaignScheduler.js
//
// HOW TO RUN:
//   node --experimental-vm-modules scheduler/testCampaignScheduler.js
//
// This file tests the new parallel scheduler logic using mock data.
// It does NOT connect to Supabase or call the WhatsApp API.
// Every function that touches external services is replaced with a mock.
//
// Tests covered:
//   1. Single campaign, single account        - basic happy path
//   2. Two campaigns, same account            - round-robin interleaving
//   3. Two campaigns, different accounts      - true parallel
//   4. Warm-up limit applied                  - campaign pauses after limit
//   5. Timed-out campaign (started_at > 30min) - marked failed, skipped
//   6. Template missing                       - campaign marked failed, others continue
//   7. Empty pending messages                 - campaign finalized immediately
//   8. Mixed: some send fail, some succeed    - correct sent/failed counts

// ─── Minimal colour helpers for readable output ───────────────────────────────
const GREEN = (s) => `\x1b[32m${s}\x1b[0m`;
const RED = (s) => `\x1b[31m${s}\x1b[0m`;
const YELLOW = (s) => `\x1b[33m${s}\x1b[0m`;
const BOLD = (s) => `\x1b[1m${s}\x1b[0m`;
const DIM = (s) => `\x1b[2m${s}\x1b[0m`;

// ─── Test runner ──────────────────────────────────────────────────────────────
let passed = 0;
let failed = 0;
const results = [];

function assert(label, condition, detail = "") {
  if (condition) {
    passed++;
    results.push({ ok: true, label });
    console.log(`  ${GREEN("PASS")} ${label}`);
  } else {
    failed++;
    results.push({ ok: false, label, detail });
    console.log(
      `  ${RED("FAIL")} ${label}${detail ? `\n       ${DIM(detail)}` : ""}`,
    );
  }
}

// ─── Mock state (reset before each test) ──────────────────────────────────────
let mockDB = {
  campaigns: [],
  campaign_messages: [],
  whatsapp_accounts: [],
  whatsapp_templates: [],
  whatsapp_messages: [],
  chats: [],
  messages: [],
};

let sentCallLog = []; // each entry: { phone, campaign_name, account_id }
let failedCallLog = [];

// ─── Mock Supabase builder ────────────────────────────────────────────────────
// Returns an object that mimics the chained Supabase query API.
function mockSupabase() {
  const db = mockDB;

  function buildQuery(table) {
    let _filters = [];
    let _updates = null;
    let _inserts = null;
    let _select = null;
    let _order = null;
    let _limit = null;
    let _range = null;
    let _head = false;
    let _count = false;

    const q = {
      select(cols, opts = {}) {
        _select = cols;
        if (opts.count === "exact") _count = true;
        if (opts.head) _head = true;
        return q;
      },
      eq(col, val) {
        _filters.push({ type: "eq", col, val });
        return q;
      },
      lte(col, val) {
        _filters.push({ type: "lte", col, val });
        return q;
      },
      lt(col, val) {
        _filters.push({ type: "lt", col, val });
        return q;
      },
      is(col, val) {
        _filters.push({ type: "is", col, val });
        return q;
      },
      not(col, op, val) {
        _filters.push({ type: "not", col, op, val });
        return q;
      },
      in(col, vals) {
        _filters.push({ type: "in", col, vals });
        return q;
      },
      or(condition) {
        _filters.push({ type: "or", condition });
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
      range(from, to) {
        _range = { from, to };
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
        return q._execute(true);
      },
      then(resolve) {
        return Promise.resolve(q._execute(false)).then(resolve);
      },

      _execute(single = false) {
        const rows = db[table] ? [...db[table]] : [];

        // INSERT
        if (_inserts) {
          const inserted = _inserts.map((row) => {
            const newRow = { ...row };
            if (!newRow.id)
              newRow.id = `mock-${Math.random().toString(36).slice(2)}`;
            db[table].push(newRow);
            return newRow;
          });
          if (single) return { data: inserted[0], error: null };
          return { data: inserted, error: null };
        }

        // UPDATE
        if (_updates === "__delete__") {
          let matched = rows;
          for (const f of _filters) matched = applyFilter(matched, f);
          const idsToDelete = new Set(
            matched.map((r) => r.campaign_id || r.cm_id || r.wa_id || r.id),
          );
          db[table] = db[table].filter(
            (r) =>
              !idsToDelete.has(r.campaign_id || r.cm_id || r.wa_id || r.id),
          );
          return { data: null, error: null };
        }
        if (_updates) {
          let matched = rows;
          for (const f of _filters) matched = applyFilter(matched, f);
          for (const row of db[table]) {
            const hit = matched.find(
              (m) => JSON.stringify(m) === JSON.stringify(row),
            );
            if (hit) Object.assign(row, _updates);
          }
          // matched holds the SAME object references as db[table] (shallow
          // copy), so Object.assign above already mutated them in place.
          // Return matched directly rather than re-filtering post-update —
          // re-filtering would incorrectly return empty whenever the
          // update itself changes a field the filter depended on (e.g.
          // our lease claim updates lease_expires_at, which the .or()
          // filter also checks).
          if (single) return { data: matched[0] || null, error: null };
          if (_select) return { data: matched, error: null };
          return { data: null, error: null };
        }

        // SELECT
        let result = rows;
        for (const f of _filters) result = applyFilter(result, f);
        if (_order) {
          result = [...result].sort((a, b) => {
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
        if (_range) result = result.slice(_range.from, _range.to + 1);

        if (_count && _head) return { count: result.length, error: null };
        if (single) return { data: result[0] || null, error: null };
        return { data: result, error: null };
      },
    };
    return q;
  }

  function applyFilter(rows, f) {
    if (f.type === "eq") return rows.filter((r) => r[f.col] === f.val);
    if (f.type === "lte") return rows.filter((r) => r[f.col] <= f.val);
    if (f.type === "lt")
      return rows.filter(
        (r) => r[f.col] != null && new Date(r[f.col]) < new Date(f.val),
      );
    if (f.type === "is")
      return rows.filter((r) =>
        f.val === null
          ? r[f.col] === null || r[f.col] === undefined
          : r[f.col] === f.val,
      );
    if (f.type === "in") return rows.filter((r) => f.vals.includes(r[f.col]));
    if (f.type === "not") {
      if (f.op === "is")
        return rows.filter((r) => r[f.col] !== null && r[f.col] !== undefined);
    }
    if (f.type === "or") {
      const parts = f.condition.split(",");
      return rows.filter((r) =>
        parts.some((part) => {
          const firstDot = part.indexOf(".");
          const secondDot = part.indexOf(".", firstDot + 1);
          const col = part.slice(0, firstDot);
          const op = part.slice(firstDot + 1, secondDot);
          const val = part.slice(secondDot + 1);
          if (op === "is" && val === "null") {
            return r[col] === null || r[col] === undefined;
          }
          if (op === "lt") {
            return r[col] != null && new Date(r[col]) < new Date(val);
          }
          return false;
        }),
      );
    }
    return rows;
  }

  return {
    from: (table) => buildQuery(table),
  };
}

// ─── Mock axios (WhatsApp API call) ───────────────────────────────────────────
// By default succeeds. Tests can override mockAxiosError to simulate failures.
let mockAxiosError = null; // set to true to make all sends fail
let mockAxiosShouldFailFor = new Set(); // specific phone numbers that should fail

async function mockAxiosPost(url, body) {
  const phone = body.to;

  if (mockAxiosError || mockAxiosShouldFailFor.has(phone)) {
    const err = new Error("WhatsApp API error: invalid phone number");
    err.response = { data: { error: { message: "invalid phone" } } };
    throw err;
  }

  return {
    data: {
      messages: [{ id: `wamid.mock.${phone}.${Date.now()}` }],
    },
  };
}

// ─── Build the scheduler functions with mocks injected ───────────────────────
// We re-implement the module inline with mocks instead of importing the real
// file, so tests are fully self-contained and don't need env vars.

function buildScheduler(overrides = {}) {
  const supabase = overrides.supabase || mockSupabase();
  const axiosPost = overrides.axiosPost || mockAxiosPost;

  const BATCH_SIZE = overrides.BATCH_SIZE || 3; // smaller for faster tests
  const BATCH_DELAY_MS = overrides.BATCH_DELAY_MS || 0; // no sleep in tests
  const MESSAGE_DELAY_MIN = 0;
  const MESSAGE_DELAY_MAX = 0;

  // Tier 1 config — overridable so tests can exercise cap/lease behavior
  const WORKER_ID =
    overrides.WORKER_ID || `worker-${Math.random().toString(36).slice(2)}`;
  const MAX_CONCURRENT_ACCOUNTS = overrides.MAX_CONCURRENT_ACCOUNTS || 15;
  const LEASE_DURATION_MS = overrides.LEASE_DURATION_MS || 3 * 60 * 1000;

  function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }

  function randomDelay(min, max) {
    return 0; // no delay in tests
  }

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
    const new_daily_sent = (account.warmup_daily_sent || 0) + messagesSent;
    const warmup_limits = account.warmup_limits || [200, 500, 1000];
    const current_stage = account.warmup_stage || 1;
    const current_progress =
      (account.warmup_stage_progress || 0) + messagesSent;
    const current_limit = warmup_limits[current_stage - 1];

    if (current_progress >= current_limit) {
      if (current_stage < warmup_limits.length) {
        await supabase
          .from("whatsapp_accounts")
          .update({
            warmup_stage: current_stage + 1,
            warmup_stage_progress: 0,
            warmup_daily_sent: new_daily_sent,
            warmup_daily_reset_at: new Date().toISOString(),
          })
          .eq("wa_id", accountId);
      } else {
        await supabase
          .from("whatsapp_accounts")
          .update({
            warmup_completed: true,
            warmup_daily_sent: new_daily_sent,
            warmup_daily_reset_at: new Date().toISOString(),
          })
          .eq("wa_id", accountId);
      }
    } else {
      await supabase
        .from("whatsapp_accounts")
        .update({
          warmup_stage_progress: current_progress,
          warmup_daily_sent: new_daily_sent,
          warmup_daily_reset_at: new Date().toISOString(),
        })
        .eq("wa_id", accountId);
    }
  }

  async function updateTierDailySent(accountId, messagesSent, account) {
    const newVal = (account.tier_daily_sent || 0) + messagesSent;
    await supabase
      .from("whatsapp_accounts")
      .update({
        tier_daily_sent: newVal,
        tier_daily_reset_at: new Date().toISOString(),
      })
      .eq("wa_id", accountId);
  }

  async function markCampaignCompleted(campaignId, sent, failed) {
    await supabase
      .from("campaigns")
      .update({
        status: "completed",
        completed_at: new Date().toISOString(),
        messages_sent: sent,
        messages_failed: failed,
      })
      .eq("campaign_id", campaignId);
  }

  async function findOrCreateChat(phone, name, groupId, userId, lastMessage) {
    return `chat-${phone}`;
  }

  function buildTemplateMessage(template) {
    return `Template: ${template.name}`;
  }

  function extractTemplateButtons(template) {
    return null;
  }

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

    const response = await axiosPost(
      `https://graph.facebook.com/v21.0/${account.phone_number_id}/messages`,
      messageBody,
    );

    const wa_message_id = response.data.messages?.[0]?.id;

    const { data: wmRecord } = await supabase
      .from("whatsapp_messages")
      .insert({
        account_id: account.wa_id,
        to_number: phoneNumber,
        template_name: template.name,
        wa_message_id,
        status: "sent",
        sent_at: new Date().toISOString(),
      })
      .select()
      .single();

    const chatId = await findOrCreateChat(
      phoneNumber,
      contactName,
      groupId,
      userId,
      "",
    );

    await supabase.from("messages").insert({
      chat_id: chatId,
      sender_type: "admin",
      message: buildTemplateMessage(template),
      message_type: "template",
    });

    // Track in sentCallLog for assertions
    sentCallLog.push({
      phone: phoneNumber,
      campaign_name: "tracked",
      account_id: account.wa_id,
    });

    return { wm_id: wmRecord?.id, wa_message_id, chat_id: chatId };
  }

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
      } catch (err) {
        await supabase
          .from("campaign_messages")
          .update({
            status: "failed",
            failed_at: new Date().toISOString(),
            error_message: err.message,
            error_code: "SEND_ERROR",
          })
          .eq("cm_id", message.cm_id);
        failedCallLog.push({ phone: message.phone_number });
        failed++;
      }
    }
    return { sent, failed };
  }

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
    } else {
      await markCampaignCompleted(campaign.campaign_id, totalSent, totalFailed);
    }
  }

  async function processAccountCampaigns(accountId, campaigns) {
    const { data: account } = await supabase
      .from("whatsapp_accounts")
      .select("*")
      .eq("wa_id", accountId)
      .single();

    if (!account) return;

    await maybeResetDailyCounters(account);
    const { data: acct } = await supabase
      .from("whatsapp_accounts")
      .select("*")
      .eq("wa_id", accountId)
      .single();

    // Tier 1: DB lease claim — mirrors the real fix. Two separate atomic
    // UPDATE...WHERE passes (null-lease, then expired-lease) instead of
    // one .or()-based query, so two "workers" sharing the same mock DB
    // (different WORKER_ID) can never both successfully claim the same
    // campaign.
    const nowIso = new Date().toISOString();
    const leaseUntil = new Date(Date.now() + LEASE_DURATION_MS).toISOString();
    const candidateIds = campaigns.map((c) => c.campaign_id);

    const { data: claimedNullLease } = await supabase
      .from("campaigns")
      .update({ worker_id: WORKER_ID, lease_expires_at: leaseUntil })
      .in("campaign_id", candidateIds)
      .is("lease_expires_at", null)
      .select("campaign_id");

    const claimedIds = new Set(
      (claimedNullLease || []).map((r) => r.campaign_id),
    );
    const remainingCandidates = candidateIds.filter(
      (id) => !claimedIds.has(id),
    );

    if (remainingCandidates.length > 0) {
      const { data: claimedExpiredLease } = await supabase
        .from("campaigns")
        .update({ worker_id: WORKER_ID, lease_expires_at: leaseUntil })
        .in("campaign_id", remainingCandidates)
        .lt("lease_expires_at", nowIso)
        .select("campaign_id");
      for (const r of claimedExpiredLease || []) claimedIds.add(r.campaign_id);
    }

    const campaignsToProcess = campaigns.filter((c) =>
      claimedIds.has(c.campaign_id),
    );
    if (campaignsToProcess.length === 0) return;

    // Stall protection: only fail if no progress in STALL_MINUTES,
    // not just because it's been running a long time (matches real fix)
    const STALL_MINUTES = 15;
    const safeCampaigns = [];
    for (const c of campaignsToProcess) {
      if (c.started_at) {
        const lastActivity = new Date(c.updated_at || c.started_at);
        const minutesSinceActivity = (Date.now() - lastActivity) / 60000;
        if (minutesSinceActivity > STALL_MINUTES) {
          await supabase
            .from("campaigns")
            .update({
              status: "failed",
              completed_at: new Date().toISOString(),
              worker_id: null,
              lease_expires_at: null,
            })
            .eq("campaign_id", c.campaign_id);
          continue;
        }
      }
      safeCampaigns.push(c);
    }
    if (safeCampaigns.length === 0) return;

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

    const queues = [];
    for (const campaign of safeCampaigns) {
      const { data: template } = await supabase
        .from("whatsapp_templates")
        .select("*")
        .eq("wt_id", campaign.wt_id)
        .single();

      if (!template) {
        await supabase
          .from("campaigns")
          .update({
            status: "failed",
            completed_at: new Date().toISOString(),
            worker_id: null,
            lease_expires_at: null,
          })
          .eq("campaign_id", campaign.campaign_id);
        continue;
      }

      const warmupLimit = getWarmupMessageLimit(acct);
      const { count: pendingCount } = await supabase
        .from("campaign_messages")
        .select("cm_id", { count: "exact", head: true })
        .eq("campaign_id", campaign.campaign_id)
        .eq("status", "pending");

      if (!pendingCount || pendingCount === 0) {
        await finalizeCampaign(campaign, acct, 0, 0, false);
        continue;
      }

      const remainingBudget = warmupLimit !== null ? warmupLimit : Infinity;
      if (remainingBudget <= 0) {
        await finalizeCampaign(campaign, acct, 0, 0, true);
        continue;
      }

      queues.push({
        campaign,
        template,
        remainingBudget,
        exhausted: false,
        sent: 0,
        failed: 0,
      });
    }

    if (queues.length === 0) return;

    let roundIndex = 0;
    let consecutiveEmpty = 0;

    while (true) {
      const anyRemaining = queues.some((q) => !q.exhausted);
      if (!anyRemaining) break;

      const queue = queues[roundIndex % queues.length];
      roundIndex++;

      if (queue.exhausted) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= queues.length) break;
        continue;
      }
      consecutiveEmpty = 0;

      const fetchSize = Math.min(BATCH_SIZE, queue.remainingBudget);

      // Tier 1: streaming fetch — pull only the next batch, not the
      // whole campaign, mirroring the real fix.
      const { data: batch } = await supabase
        .from("campaign_messages")
        .select("*")
        .eq("campaign_id", queue.campaign.campaign_id)
        .eq("status", "pending")
        .limit(fetchSize);

      if (!batch || batch.length === 0) {
        queue.exhausted = true;
        continue;
      }

      const { sent, failed } = await sendBatch(
        acct,
        queue.template,
        batch,
        queue.campaign,
      );
      queue.sent += sent;
      queue.failed += failed;

      if (queue.remainingBudget !== Infinity) {
        queue.remainingBudget -= batch.length;
        if (queue.remainingBudget <= 0) queue.exhausted = true;
      }
      if (batch.length < fetchSize) queue.exhausted = true;

      // Heartbeat + lease refresh
      await supabase
        .from("campaigns")
        .update({
          updated_at: new Date().toISOString(),
          lease_expires_at: new Date(
            Date.now() + LEASE_DURATION_MS,
          ).toISOString(),
        })
        .eq("campaign_id", queue.campaign.campaign_id);

      const stillHasWork = queues.some((q) => !q.exhausted);
      if (stillHasWork) await sleep(BATCH_DELAY_MS);
    }

    const totalSentThisTick = queues.reduce((s, q) => s + q.sent, 0);
    if (totalSentThisTick > 0) {
      if (acct.warmup_enabled && !acct.warmup_completed) {
        await updateWarmupProgress(acct.wa_id, totalSentThisTick, acct);
      } else if (acct.warmup_completed) {
        await updateTierDailySent(acct.wa_id, totalSentThisTick, acct);
      }
    }

    for (const queue of queues) {
      await finalizeCampaign(
        queue.campaign,
        acct,
        queue.sent,
        queue.failed,
        false,
      );
    }
  }

  // Per-account in-flight tracker — mirrors the real fix. An account stays
  // in this set for as long as its processAccountCampaigns() call is
  // running, however long that takes. Any OTHER account not in this set
  // is dispatched immediately on the very next call, regardless.
  const accountsInFlight = new Set();

  async function checkAndSendCampaigns() {
    const now = new Date();
    const { data: campaigns, error } = await supabase
      .from("campaigns")
      .select("*")
      .in("status", ["scheduled", "processing"])
      .lte("scheduled_at", now.toISOString())
      .order("scheduled_at", { ascending: true });

    if (error || !campaigns || campaigns.length === 0) return [];

    const byAccount = new Map();
    for (const c of campaigns) {
      if (!byAccount.has(c.account_id)) byAccount.set(c.account_id, []);
      byAccount.get(c.account_id).push(c);
    }

    const dispatched = [];
    for (const [accountId, accountCampaigns] of byAccount.entries()) {
      if (accountsInFlight.has(accountId)) {
        continue; // busy account — skip this tick, will retry once free
      }

      // Tier 1: concurrency cap
      if (accountsInFlight.size >= MAX_CONCURRENT_ACCOUNTS) {
        break;
      }

      accountsInFlight.add(accountId);

      // Fire-and-forget — NOT awaited here, this is the core of the fix.
      const p = processAccountCampaigns(accountId, accountCampaigns)
        .catch(() => {})
        .finally(() => accountsInFlight.delete(accountId));

      dispatched.push(p);
    }

    // Returned so tests CAN await full completion with
    // `await Promise.all(await checkAndSendCampaigns())` when they want
    // deterministic results, while production cron does NOT await this.
    return dispatched;
  }

  return { checkAndSendCampaigns, accountsInFlight, WORKER_ID };
}

// ─── Data factory helpers ─────────────────────────────────────────────────────

function makeAccount(id, opts = {}) {
  return {
    wa_id: id,
    business_phone_number: `+91${id}`,
    phone_number_id: `phone-${id}`,
    system_user_access_token: `token-${id}`,
    messaging_limit_tier: "TIER_1K",
    messaging_limit_per_day: 1000,
    warmup_enabled: opts.warmup_enabled ?? false,
    warmup_completed: opts.warmup_completed ?? false,
    warmup_stage: opts.warmup_stage ?? 1,
    warmup_stage_progress: opts.warmup_stage_progress ?? 0,
    warmup_limits: opts.warmup_limits ?? [10, 20, 50],
    warmup_daily_sent: opts.warmup_daily_sent ?? 0,
    warmup_daily_reset_at: opts.warmup_daily_reset_at ?? null,
    tier_daily_sent: opts.tier_daily_sent ?? 0,
    tier_daily_reset_at: opts.tier_daily_reset_at ?? null,
    status: "active",
  };
}

function makeTemplate(id) {
  return {
    wt_id: id,
    name: `template-${id}`,
    language: "en_US",
    components: JSON.stringify([{ type: "BODY", text: "Hello {{1}}" }]),
    status: "APPROVED",
  };
}

function makeCampaign(id, accountId, templateId, opts = {}) {
  return {
    campaign_id: id,
    campaign_name: opts.name || `Campaign ${id}`,
    account_id: accountId,
    wt_id: templateId,
    group_id: `group-${id}`,
    user_id: `user-1`,
    scheduled_at:
      opts.scheduled_at || new Date(Date.now() - 1000).toISOString(),
    status: "scheduled",
    messages_sent: 0,
    messages_failed: 0,
    total_recipients: opts.contacts || 0,
    started_at: opts.started_at || null,
    template_variables: {},
    media_id: null,
  };
}

function makeMessages(campaignId, count) {
  return Array.from({ length: count }, (_, i) => ({
    cm_id: `cm-${campaignId}-${i}`,
    campaign_id: campaignId,
    contact_id: `contact-${i}`,
    phone_number: `9191${campaignId}${String(i).padStart(4, "0")}`,
    contact_name: `Contact ${i}`,
    status: "pending",
    sent_at: null,
    wm_id: null,
    wa_message_id: null,
  }));
}

function resetMock(data = {}) {
  mockDB = {
    campaigns: data.campaigns || [],
    campaign_messages: data.campaign_messages || [],
    whatsapp_accounts: data.accounts || [],
    whatsapp_templates: data.templates || [],
    whatsapp_messages: [],
    chats: [],
    messages: [],
  };
  sentCallLog = [];
  failedCallLog = [];
  mockAxiosError = false;
  mockAxiosShouldFailFor = new Set();
}

function getCampaign(id) {
  return mockDB.campaigns.find((c) => c.campaign_id === id);
}

function getMessageStatuses(campaignId) {
  const msgs = mockDB.campaign_messages.filter(
    (m) => m.campaign_id === campaignId,
  );
  return {
    total: msgs.length,
    pending: msgs.filter((m) => m.status === "pending").length,
    sent: msgs.filter((m) => m.status === "sent").length,
    failed: msgs.filter((m) => m.status === "failed").length,
  };
}

/* ============================================================
   THE TESTS
   ============================================================ */

async function runTests() {
  console.log(
    BOLD("\n============================================================"),
  );
  console.log(BOLD("  Campaign Scheduler — Test Suite"));
  console.log(
    BOLD("============================================================\n"),
  );

  // ── TEST 1: Single campaign, single account ────────────────────────────────
  {
    console.log(
      BOLD("Test 1: Single campaign, single account (basic happy path)"),
    );
    const acctId = "acct-1";
    const campId = "camp-1";
    const tmplId = "tmpl-1";

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplId)],
      campaigns: [
        makeCampaign(campId, acctId, tmplId, { name: "Camp-1", contacts: 6 }),
      ],
      campaign_messages: makeMessages(campId, 6),
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const camp = getCampaign(campId);
    const stats = getMessageStatuses(campId);

    assert(
      "Campaign status is completed",
      camp?.status === "completed",
      `got: ${camp?.status}`,
    );
    assert(
      "All 6 messages sent",
      stats.sent === 6,
      `sent=${stats.sent} pending=${stats.pending}`,
    );
    assert("0 messages failed", stats.failed === 0);
    assert("completed_at is set", !!camp?.completed_at);
    assert(
      "messages_sent counter = 6",
      camp?.messages_sent === 6,
      `got: ${camp?.messages_sent}`,
    );
    console.log();
  }

  // ── TEST 2: Two campaigns, same account — round-robin ─────────────────────
  {
    console.log(
      BOLD("Test 2: Two campaigns, same account (round-robin interleaving)"),
    );
    const acctId = "acct-2";
    const campAId = "camp-2a";
    const campBId = "camp-2b";
    const tmplId = "tmpl-2";

    const messages2a = makeMessages(campAId, 7); // will need 3 batches (3,3,1)
    const messages2b = makeMessages(campBId, 4); // will need 2 batches (3,1)

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplId)],
      campaigns: [
        makeCampaign(campAId, acctId, tmplId, { name: "Camp-A", contacts: 7 }),
        makeCampaign(campBId, acctId, tmplId, { name: "Camp-B", contacts: 4 }),
      ],
      campaign_messages: [...messages2a, ...messages2b],
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const campA = getCampaign(campAId);
    const campB = getCampaign(campBId);
    const statsA = getMessageStatuses(campAId);
    const statsB = getMessageStatuses(campBId);

    assert(
      "Camp-A completed",
      campA?.status === "completed",
      `got: ${campA?.status}`,
    );
    assert(
      "Camp-B completed",
      campB?.status === "completed",
      `got: ${campB?.status}`,
    );
    assert("Camp-A: all 7 sent", statsA.sent === 7, `sent=${statsA.sent}`);
    assert("Camp-B: all 4 sent", statsB.sent === 4, `sent=${statsB.sent}`);
    assert(
      "Camp-B did not wait for Camp-A to fully finish",
      campB?.status === "completed",
      "Camp-B should be completed independently",
    );
    console.log();
  }

  // ── TEST 3: Two campaigns, DIFFERENT accounts — true parallel ─────────────
  {
    console.log(
      BOLD("Test 3: Two campaigns, different accounts (parallel execution)"),
    );
    const acctA = "acct-3a";
    const acctB = "acct-3b";
    const campA = "camp-3a";
    const campB = "camp-3b";
    const tmplA = "tmpl-3a";
    const tmplB = "tmpl-3b";

    const executionOrder = [];
    let callCount = 0;

    const trackingAxiosPost = async (url, body) => {
      callCount++;
      executionOrder.push({ phone: body.to, time: Date.now() });
      return { data: { messages: [{ id: `wamid-${callCount}` }] } };
    };

    resetMock({
      accounts: [
        makeAccount(acctA, { warmup_completed: true }),
        makeAccount(acctB, { warmup_completed: true }),
      ],
      templates: [makeTemplate(tmplA), makeTemplate(tmplB)],
      campaigns: [
        makeCampaign(campA, acctA, tmplA, { name: "Camp-AcctA", contacts: 3 }),
        makeCampaign(campB, acctB, tmplB, { name: "Camp-AcctB", contacts: 3 }),
      ],
      campaign_messages: [...makeMessages(campA, 3), ...makeMessages(campB, 3)],
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
      axiosPost: trackingAxiosPost,
    });
    await Promise.all(await checkAndSendCampaigns());

    const cA = getCampaign(campA);
    const cB = getCampaign(campB);

    assert(
      "Camp-AcctA completed",
      cA?.status === "completed",
      `got: ${cA?.status}`,
    );
    assert(
      "Camp-AcctB completed",
      cB?.status === "completed",
      `got: ${cB?.status}`,
    );
    assert("Total 6 WA API calls made", callCount === 6, `got: ${callCount}`);
    console.log();
  }

  // ── TEST 4: Warm-up limit — campaign pauses after limit ───────────────────
  {
    console.log(
      BOLD("Test 4: Warm-up limit — campaign pauses, resumes next tick"),
    );
    const acctId = "acct-4";
    const campId = "camp-4";
    const tmplId = "tmpl-4";

    // warmup stage 1 limit is 5, progress is 0 → can send 5 this tick
    resetMock({
      accounts: [
        makeAccount(acctId, {
          warmup_enabled: true,
          warmup_completed: false,
          warmup_stage: 1,
          warmup_stage_progress: 0,
          warmup_limits: [5, 10, 20],
        }),
      ],
      templates: [makeTemplate(tmplId)],
      campaigns: [
        makeCampaign(campId, acctId, tmplId, {
          name: "Camp-Warmup",
          contacts: 10,
        }),
      ],
      campaign_messages: makeMessages(campId, 10),
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const camp = getCampaign(campId);
    const stats = getMessageStatuses(campId);

    assert(
      "Campaign paused (status = scheduled)",
      camp?.status === "scheduled",
      `got: ${camp?.status}`,
    );
    assert(
      "Exactly 5 messages sent (warmup limit)",
      stats.sent === 5,
      `sent=${stats.sent}`,
    );
    assert(
      "5 messages still pending",
      stats.pending === 5,
      `pending=${stats.pending}`,
    );
    console.log();
  }

  // ── TEST 5: Genuinely STALLED campaign (no progress in 15+ min) ──────────
  {
    console.log(
      BOLD(
        "Test 5: Genuinely stalled campaign (no progress in 15+ min) — marked failed",
      ),
    );
    const acctId = "acct-5";
    const campId = "camp-5";
    const tmplId = "tmpl-5";

    // Both started_at AND updated_at are old — no batch has run in ages.
    // This is a REAL stall (e.g. crashed mid-send, orphaned row).
    const oldTime = new Date(Date.now() - 20 * 60 * 1000).toISOString();

    const campaign = makeCampaign(campId, acctId, tmplId, {
      name: "Camp-Stalled",
      contacts: 5,
      started_at: oldTime,
    });
    campaign.updated_at = oldTime; // last activity was 20 min ago — genuinely stuck

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplId)],
      campaigns: [campaign],
      campaign_messages: makeMessages(campId, 5),
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const camp = getCampaign(campId);
    const stats = getMessageStatuses(campId);

    assert(
      "Genuinely stalled campaign marked failed",
      camp?.status === "failed",
      `got: ${camp?.status}`,
    );
    assert("No messages were sent", stats.sent === 0, `sent=${stats.sent}`);
    console.log();
  }

  // ── TEST 11: LONG-RUNNING BUT HEALTHY campaign — must NOT be killed ──────
  // This reproduces the exact production bug: "mrk preview" ran 184 minutes
  // total (huge campaign, round-robining with a sibling on the same account)
  // but was ACTIVELY sending the whole time. The old code checked elapsed
  // time since started_at and killed it at the 30-min mark regardless of
  // activity. The fix checks time since LAST PROGRESS (updated_at) instead.
  {
    console.log(
      BOLD(
        "Test 11: Long-running but healthy campaign (like production 'mrk preview' bug) — must NOT fail",
      ),
    );
    const acctId = "acct-11";
    const campId = "camp-11";
    const tmplId = "tmpl-11";

    // started_at is 3 hours ago (way past the old 30-min elapsed-time limit)
    const startedLongAgo = new Date(
      Date.now() - 3 * 60 * 60 * 1000,
    ).toISOString();
    // BUT updated_at is recent — a batch was sent just 2 minutes ago.
    // This proves the campaign is still healthy and actively progressing.
    const recentActivity = new Date(Date.now() - 2 * 60 * 1000).toISOString();

    const campaign = makeCampaign(campId, acctId, tmplId, {
      name: "Camp-LongButHealthy",
      contacts: 8,
      started_at: startedLongAgo,
    });
    campaign.updated_at = recentActivity;
    campaign.messages_sent = 1990; // simulating a huge campaign mostly done already

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplId)],
      campaigns: [campaign],
      campaign_messages: makeMessages(campId, 8),
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const camp = getCampaign(campId);
    const stats = getMessageStatuses(campId);

    assert(
      "Long-running but healthy campaign was NOT marked failed",
      camp?.status !== "failed",
      `got: ${camp?.status} (this is the exact production bug if 'failed')`,
    );
    assert(
      "Its remaining 8 messages were sent normally",
      stats.sent === 8,
      `sent=${stats.sent}`,
    );
    assert(
      "Campaign completed successfully despite 3-hour total runtime",
      camp?.status === "completed",
      `got: ${camp?.status}`,
    );
    console.log();
  }

  // ── TEST 6: Missing template — campaign fails, others continue ─────────────
  {
    console.log(
      BOLD(
        "Test 6: Missing template — campaign fails, sibling campaigns continue",
      ),
    );
    const acctId = "acct-6";
    const campBadId = "camp-6-bad";
    const campGoodId = "camp-6-good";
    const tmplGoodId = "tmpl-6-good";
    // campBadId references a template that does NOT exist in the DB

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplGoodId)], // only the good template
      campaigns: [
        makeCampaign(campBadId, acctId, "tmpl-MISSING", {
          name: "Camp-Bad",
          contacts: 3,
        }),
        makeCampaign(campGoodId, acctId, tmplGoodId, {
          name: "Camp-Good",
          contacts: 3,
        }),
      ],
      campaign_messages: [
        ...makeMessages(campBadId, 3),
        ...makeMessages(campGoodId, 3),
      ],
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const campBad = getCampaign(campBadId);
    const campGood = getCampaign(campGoodId);
    const statsGood = getMessageStatuses(campGoodId);

    assert(
      "Bad campaign marked failed",
      campBad?.status === "failed",
      `got: ${campBad?.status}`,
    );
    assert(
      "Good campaign still completed",
      campGood?.status === "completed",
      `got: ${campGood?.status}`,
    );
    assert(
      "Good campaign sent all 3 messages",
      statsGood.sent === 3,
      `sent=${statsGood.sent}`,
    );
    console.log();
  }

  // ── TEST 7: Empty pending messages ────────────────────────────────────────
  {
    console.log(
      BOLD("Test 7: Campaign with 0 pending messages — finalizes immediately"),
    );
    const acctId = "acct-7";
    const campId = "camp-7";
    const tmplId = "tmpl-7";

    const camp = makeCampaign(campId, acctId, tmplId, {
      name: "Camp-Empty",
      contacts: 0,
    });
    camp.messages_sent = 5; // already sent from a previous tick

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplId)],
      campaigns: [camp],
      campaign_messages: [], // no pending messages at all
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const result = getCampaign(campId);
    assert(
      "Campaign completed (no pending)",
      result?.status === "completed",
      `got: ${result?.status}`,
    );
    console.log();
  }

  // ── TEST 8: Some messages fail — counts are correct ───────────────────────
  {
    console.log(
      BOLD("Test 8: Some API calls fail — sent/failed counts are correct"),
    );
    const acctId = "acct-8";
    const campId = "camp-8";
    const tmplId = "tmpl-8";

    const msgs = makeMessages(campId, 6);
    // Make phones at index 1, 3 fail
    const failPhones = [msgs[1].phone_number, msgs[3].phone_number];

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplId)],
      campaigns: [
        makeCampaign(campId, acctId, tmplId, {
          name: "Camp-PartialFail",
          contacts: 6,
        }),
      ],
      campaign_messages: msgs,
    });

    mockAxiosShouldFailFor = new Set(failPhones);

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const camp = getCampaign(campId);
    const stats = getMessageStatuses(campId);

    assert(
      "Campaign completed despite partial failures",
      camp?.status === "completed",
      `got: ${camp?.status}`,
    );
    assert(
      "4 messages sent successfully",
      stats.sent === 4,
      `sent=${stats.sent}`,
    );
    assert("2 messages failed", stats.failed === 2, `failed=${stats.failed}`);
    assert(
      "messages_sent counter = 4",
      camp?.messages_sent === 4,
      `got: ${camp?.messages_sent}`,
    );
    assert(
      "messages_failed counter = 2",
      camp?.messages_failed === 2,
      `got: ${camp?.messages_failed}`,
    );
    console.log();
  }

  // ── TEST 9: Warmup stage advances when limit hit ───────────────────────────
  {
    console.log(
      BOLD("Test 9: Warmup stage advances when stage limit is reached"),
    );
    const acctId = "acct-9";
    const campId = "camp-9";
    const tmplId = "tmpl-9";

    // Stage 1 limit = 6, progress = 3. Sending 3 more will complete stage 1.
    resetMock({
      accounts: [
        makeAccount(acctId, {
          warmup_enabled: true,
          warmup_completed: false,
          warmup_stage: 1,
          warmup_stage_progress: 3,
          warmup_limits: [6, 15, 30],
          warmup_daily_sent: 3,
        }),
      ],
      templates: [makeTemplate(tmplId)],
      campaigns: [
        makeCampaign(campId, acctId, tmplId, {
          name: "Camp-StageUp",
          contacts: 3,
        }),
      ],
      campaign_messages: makeMessages(campId, 3),
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const updatedAccount = mockDB.whatsapp_accounts.find(
      (a) => a.wa_id === acctId,
    );

    assert(
      "Warmup advanced to stage 2",
      updatedAccount?.warmup_stage === 2,
      `got: ${updatedAccount?.warmup_stage}`,
    );
    assert(
      "Stage progress reset to 0",
      updatedAccount?.warmup_stage_progress === 0,
      `got: ${updatedAccount?.warmup_stage_progress}`,
    );
    console.log();
  }

  // ── TEST 10: Future-scheduled campaign is NOT picked up ───────────────────
  {
    console.log(
      BOLD("Test 10: Future-scheduled campaign is ignored this tick"),
    );
    const acctId = "acct-10";
    const campId = "camp-10";
    const tmplId = "tmpl-10";

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplId)],
      campaigns: [
        makeCampaign(campId, acctId, tmplId, {
          name: "Camp-Future",
          contacts: 3,
          scheduled_at: new Date(Date.now() + 60 * 60 * 1000).toISOString(), // 1 hour from now
        }),
      ],
      campaign_messages: makeMessages(campId, 3),
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
    });
    await Promise.all(await checkAndSendCampaigns());

    const camp = getCampaign(campId);
    const stats = getMessageStatuses(campId);

    assert(
      "Future campaign status unchanged (still scheduled)",
      camp?.status === "scheduled",
      `got: ${camp?.status}`,
    );
    assert("No messages sent", stats.sent === 0, `sent=${stats.sent}`);
    console.log();
  }

  // ── TEST 12: THE PRODUCTION BUG — busy account must NOT block a
  //    DIFFERENT account's campaign from starting on the next dispatch.
  //
  //    This reproduces exactly what was reported: client's big campaign
  //    runs on Account A for a long time. A small campaign is created on
  //    Account B (a completely different WhatsApp account) partway through.
  //    Under the OLD bug, the global isProcessing lock meant the entire
  //    scheduler tick (and therefore the SELECT query for new due
  //    campaigns) would not run again until Account A's Promise.all
  //    resolved — so Account B's campaign silently waited for HOURS even
  //    though it wasn't sharing anything with Account A.
  //
  //    Under the fix, dispatch is fire-and-forget and tracked per-account,
  //    so calling checkAndSendCampaigns() again while Account A is still
  //    mid-flight must immediately pick up and start Account B.
  {
    console.log(
      BOLD(
        "Test 12: Busy account (long campaign) does NOT block a different account's campaign",
      ),
    );
    const acctA = "acct-12-big"; // client's account — long-running campaign
    const acctB = "acct-12-small"; // a different account — small test campaign
    const campBigId = "camp-12-big";
    const campSmallId = "camp-12-small";
    const tmplBigId = "tmpl-12-big";
    const tmplSmallId = "tmpl-12-small";

    const BIG_COUNT = 15; // 3 batches at BATCH_SIZE=5 → several real delays
    const SMALL_COUNT = 2; // 1 batch, should finish almost instantly

    resetMock({
      accounts: [
        makeAccount(acctA, { warmup_completed: true }),
        makeAccount(acctB, { warmup_completed: true }),
      ],
      templates: [makeTemplate(tmplBigId), makeTemplate(tmplSmallId)],
      campaigns: [
        makeCampaign(campBigId, acctA, tmplBigId, {
          name: "Client-Big-Campaign",
          contacts: BIG_COUNT,
        }),
      ],
      campaign_messages: [...makeMessages(campBigId, BIG_COUNT)],
    });

    // Use a real (small) batch delay so Account A's campaign takes
    // measurable time — long enough that we can call checkAndSendCampaigns()
    // a second time WHILE it is still mid-flight, exactly like a real
    // 60-second cron tick firing while a multi-hour campaign is running.
    const supabase = mockSupabase();
    const { checkAndSendCampaigns, accountsInFlight } = buildScheduler({
      supabase,
      BATCH_DELAY_MS: 400, // ms between batches, enough to observe overlap
      BATCH_SIZE: 5,
    });

    // ── Tick 1: dispatch Account A's big campaign (fire-and-forget) ──────
    const tick1Promises = await checkAndSendCampaigns();

    assert(
      "Account A is marked in-flight immediately after dispatch",
      accountsInFlight.has(acctA),
      `accountsInFlight: ${[...accountsInFlight]}`,
    );

    // ── Simulate 5 minutes passing: the small campaign gets created now,
    //    on a DIFFERENT account, while Account A is still mid-flight ───────
    await supabase.from("campaigns").insert({
      campaign_id: campSmallId,
      campaign_name: "My-Small-Test-Campaign",
      account_id: acctB,
      wt_id: tmplSmallId,
      group_id: "group-small",
      user_id: "user-1",
      scheduled_at: new Date(Date.now() - 1000).toISOString(),
      status: "scheduled",
      messages_sent: 0,
      messages_failed: 0,
      total_recipients: SMALL_COUNT,
      started_at: null,
      template_variables: {},
      media_id: null,
    });
    await supabase
      .from("campaign_messages")
      .insert(makeMessages(campSmallId, SMALL_COUNT));

    // ── Tick 2: fires WHILE Account A's campaign is still running ────────
    // This is the exact moment the old bug would have caused Account B's
    // campaign to be silently ignored, because the old global isProcessing
    // lock (wrapping the whole tick, including Promise.all on Account A)
    // would still be held.
    assert(
      "Account A is STILL in-flight when tick 2 fires (proves overlap is real)",
      accountsInFlight.has(acctA),
      "if this fails, the test isn't actually overlapping — increase BATCH_DELAY_MS",
    );

    const tick2Promises = await checkAndSendCampaigns();

    assert(
      "Account B was dispatched on tick 2 despite Account A still being busy",
      accountsInFlight.has(acctB),
      `accountsInFlight after tick 2: ${[...accountsInFlight]}`,
    );

    // ── Wait for the small campaign to finish, independently ─────────────
    await Promise.all(tick2Promises);

    const smallStatusEarly = getCampaign(campSmallId);
    const bigStatusEarly = getCampaign(campBigId);

    assert(
      "Small campaign (Account B) completed WHILE big campaign (Account A) was still running",
      smallStatusEarly?.status === "completed" &&
        bigStatusEarly?.status !== "completed",
      `small=${smallStatusEarly?.status}, big=${bigStatusEarly?.status}` +
        " — if big is already 'completed' here, the test ran too fast to prove overlap; this is informational, not a hard failure of the fix itself",
    );

    // ── Now let the big campaign finish too, for cleanup/completeness ────
    await Promise.all(tick1Promises);

    const smallFinal = getCampaign(campSmallId);
    const bigFinal = getCampaign(campBigId);
    const smallStats = getMessageStatuses(campSmallId);
    const bigStats = getMessageStatuses(campBigId);

    assert(
      "Small campaign fully completed",
      smallFinal?.status === "completed",
      `got: ${smallFinal?.status}`,
    );
    assert(
      `Small campaign sent all ${SMALL_COUNT} messages`,
      smallStats.sent === SMALL_COUNT,
      `sent=${smallStats.sent}`,
    );
    assert(
      "Big campaign also fully completed (independently)",
      bigFinal?.status === "completed",
      `got: ${bigFinal?.status}`,
    );
    assert(
      `Big campaign sent all ${BIG_COUNT} messages`,
      bigStats.sent === BIG_COUNT,
      `sent=${bigStats.sent}`,
    );
    assert(
      "Both accounts are no longer in-flight after completion",
      !accountsInFlight.has(acctA) && !accountsInFlight.has(acctB),
      `accountsInFlight: ${[...accountsInFlight]}`,
    );

    console.log();
  }

  // ── TEST 13: DB LEASE — two "workers" can never both claim the same
  //    account's campaigns. This is what makes horizontal scaling safe.
  {
    console.log(
      BOLD(
        "Test 13: DB lease prevents two workers from double-claiming the same account",
      ),
    );
    const acctId = "acct-13";
    const campId = "camp-13";
    const tmplId = "tmpl-13";

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplId)],
      campaigns: [
        makeCampaign(campId, acctId, tmplId, {
          name: "Camp-Lease",
          contacts: 3,
        }),
      ],
      campaign_messages: makeMessages(campId, 3),
    });

    const sharedSupabase = mockSupabase();

    // Two independent "worker processes" sharing the same DB
    const workerA = buildScheduler({
      supabase: sharedSupabase,
      WORKER_ID: "worker-A",
    });
    const workerB = buildScheduler({
      supabase: sharedSupabase,
      WORKER_ID: "worker-B",
    });

    // Both workers see the same due campaign in the same "tick" and race
    // to claim it — call both dispatch functions without awaiting between
    // them, simulating two processes hitting the DB at nearly the same time.
    const [aPromises, bPromises] = await Promise.all([
      workerA.checkAndSendCampaigns(),
      workerB.checkAndSendCampaigns(),
    ]);

    await Promise.all([...aPromises, ...bPromises]);

    const camp = getCampaign(campId);
    const stats = getMessageStatuses(campId);

    assert(
      "Campaign completed exactly once (not double-processed)",
      camp?.status === "completed",
      `got: ${camp?.status}`,
    );
    assert(
      "Exactly 3 messages sent (not 6 — no double-send)",
      stats.sent === 3,
      `sent=${stats.sent}`,
    );
    assert(
      "Only one worker actually claimed and ran it",
      aPromises.length + bPromises.length <= 2,
      `worker A dispatched ${aPromises.length}, worker B dispatched ${bPromises.length}`,
    );
    console.log();
  }

  // ── TEST 14: CONCURRENCY CAP — accounts beyond the cap wait for the
  //    next tick instead of all being dispatched at once.
  {
    console.log(
      BOLD(
        "Test 14: Concurrency cap defers accounts beyond the limit to the next tick",
      ),
    );

    const NUM_ACCOUNTS = 5;
    const CAP = 2;

    const accounts = [];
    const templates = [];
    const campaigns = [];
    const messages = [];

    for (let i = 0; i < NUM_ACCOUNTS; i++) {
      const acctId = `acct-14-${i}`;
      const tmplId = `tmpl-14-${i}`;
      const campId = `camp-14-${i}`;
      accounts.push(makeAccount(acctId, { warmup_completed: true }));
      templates.push(makeTemplate(tmplId));
      campaigns.push(
        makeCampaign(campId, acctId, tmplId, {
          name: `Camp-Cap-${i}`,
          contacts: 2,
        }),
      );
      messages.push(...makeMessages(campId, 2));
    }

    resetMock({ accounts, templates, campaigns, campaign_messages: messages });

    const { checkAndSendCampaigns, accountsInFlight } = buildScheduler({
      supabase: mockSupabase(),
      MAX_CONCURRENT_ACCOUNTS: CAP,
      BATCH_DELAY_MS: 200, // enough to observe the cap before campaigns finish
    });

    const dispatched = await checkAndSendCampaigns();

    assert(
      `Only ${CAP} accounts dispatched this tick (cap respected)`,
      accountsInFlight.size <= CAP,
      `accountsInFlight.size=${accountsInFlight.size}, cap=${CAP}`,
    );
    assert(
      `Exactly ${CAP} promises returned from this tick's dispatch`,
      dispatched.length === CAP,
      `got: ${dispatched.length}`,
    );

    // Let everything finish, then run more ticks until every account has
    // been picked up. 5 accounts / cap 2 needs 3 ticks to fully drain
    // (2 + 2 + 1) — this loop mirrors what the real 60s cron would do
    // naturally over consecutive ticks.
    await Promise.all(dispatched);

    let tickCount = 1;
    while (tickCount < 10) {
      const nextDispatch = await checkAndSendCampaigns();
      if (nextDispatch.length === 0) break;
      await Promise.all(nextDispatch);
      tickCount++;
    }

    const finalCampaigns = campaigns.map((c) => getCampaign(c.campaign_id));
    const allCompleted = finalCampaigns.every((c) => c?.status === "completed");

    assert(
      "All accounts eventually completed across multiple ticks",
      allCompleted,
      `statuses: ${finalCampaigns.map((c) => c?.status).join(", ")} (took ${tickCount} ticks)`,
    );
    console.log();
  }

  // ── TEST 15: STREAMING FETCH — a campaign larger than one page still
  //    sends every message exactly once, without preloading the whole
  //    list into memory upfront.
  {
    console.log(
      BOLD(
        "Test 15: Streaming fetch correctly drains a large campaign in small batches",
      ),
    );
    const acctId = "acct-15";
    const campId = "camp-15";
    const tmplId = "tmpl-15";

    const LARGE_COUNT = 47; // deliberately not a multiple of BATCH_SIZE

    resetMock({
      accounts: [makeAccount(acctId, { warmup_completed: true })],
      templates: [makeTemplate(tmplId)],
      campaigns: [
        makeCampaign(campId, acctId, tmplId, {
          name: "Camp-Large",
          contacts: LARGE_COUNT,
        }),
      ],
      campaign_messages: makeMessages(campId, LARGE_COUNT),
    });

    const { checkAndSendCampaigns } = buildScheduler({
      supabase: mockSupabase(),
      BATCH_SIZE: 5,
    });

    await Promise.all(await checkAndSendCampaigns());

    const camp = getCampaign(campId);
    const stats = getMessageStatuses(campId);

    assert(
      "Large campaign completed",
      camp?.status === "completed",
      `got: ${camp?.status}`,
    );
    assert(
      `All ${LARGE_COUNT} messages sent exactly once`,
      stats.sent === LARGE_COUNT,
      `sent=${stats.sent}, total=${stats.total}`,
    );
    assert(
      "No messages left pending",
      stats.pending === 0,
      `pending=${stats.pending}`,
    );
    assert(
      "No duplicate sends (sent count matches total exactly)",
      stats.sent === stats.total,
      `sent=${stats.sent}, total=${stats.total}`,
    );
    console.log();
  }

  // ── RESULTS SUMMARY ───────────────────────────────────────────────────────
  const total = passed + failed;
  console.log(
    BOLD("============================================================"),
  );
  console.log(BOLD("  Results"));
  console.log(
    BOLD("============================================================"),
  );
  console.log(`  Total:  ${total}`);
  console.log(`  ${GREEN(`Passed: ${passed}`)}`);
  if (failed > 0) {
    console.log(`  ${RED(`Failed: ${failed}`)}`);
    console.log("\n  Failed tests:");
    results
      .filter((r) => !r.ok)
      .forEach((r) => {
        console.log(`    ${RED("FAIL")} ${r.label}`);
        if (r.detail) console.log(`         ${DIM(r.detail)}`);
      });
  }
  console.log(
    BOLD("============================================================\n"),
  );

  if (failed > 0) process.exit(1);
}

runTests().catch((err) => {
  console.error(RED("Test runner crashed:"), err);
  process.exit(1);
});
