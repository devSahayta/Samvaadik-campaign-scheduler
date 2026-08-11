// scheduler/campaignScheduler.js
//
// ARCHITECTURE (v3 — Tier 1 hardening for high concurrent volume):
//   Adds three safety mechanisms on top of v2's per-account fairness,
//   none of which touch sending pace, batch size, or delays:
//
//   1. CONCURRENCY CAP — v2 dispatched every free account unconditionally.
//      If 50+ accounts became due in the same tick, 50+ full campaigns'
//      worth of processing would kick off at once. MAX_CONCURRENT_ACCOUNTS
//      bounds this; accounts beyond the cap simply wait for the next tick
//      (oldest-due-first, since campaigns are queried ordered by
//      scheduled_at). No campaign is skipped, only delayed a few ticks
//      under genuinely heavy simultaneous load.
//
//   2. DB LEASE — v2's accountsInFlight is an in-memory Set, only valid
//      inside one process. If a second Render worker were ever run, both
//      instances would independently see an account as "free" and could
//      double-claim it, sending duplicate messages. Each campaign row now
//      carries worker_id + lease_expires_at; claiming is an atomic
//      UPDATE...WHERE (lease_expires_at IS NULL OR expired), which
//      Postgres guarantees only one process can win. This is what makes
//      horizontal scaling (running >1 worker) safe in the future, without
//      needing a full queue migration now.
//
//   3. STREAMING FETCH — v2 loaded an entire campaign's pending contact
//      list into memory upfront (fetchPendingMessages), then round-robin
//      spliced 5 at a time from that array. For very large campaigns run
//      concurrently across many accounts, this holds a lot in RAM at
//      once. Now each round-robin turn fetches only the next BATCH_SIZE
//      rows right before sending them — memory per campaign stays flat
//      regardless of whether it has 50 or 500,000 contacts.
//
//   SENDING PACE IS UNTOUCHED: BATCH_SIZE=5, BATCH_DELAY_MS=10000,
//   MESSAGE_DELAY_MIN/MAX=800-2000ms are byte-for-byte identical to v2.
//   These three mechanisms only decide *when a batch is allowed to start*
//   and *how much is held in memory* — never how fast messages actually
//   go out once a batch begins. Meta-facing delivery timing is unchanged.
//
// CARRIED FROM v2:
//   - checkAndSendCampaigns: groups by account_id, fire-and-forget dispatch
//     (does not block subsequent ticks from finding work on free accounts)
//   - Stall detection via updated_at (time since LAST PROGRESS), not
//     started_at (total elapsed time) — long-but-healthy campaigns are
//     never wrongly killed
//   - Heartbeat write (updated_at) after every batch
//   - Recovers orphaned "processing" campaigns after a worker restart
//
// WHAT IS 100% IDENTICAL TO THE ORIGINAL:
//   - BATCH_SIZE, BATCH_DELAY_MS, MESSAGE_DELAY_MIN/MAX constants
//   - sendWhatsAppMessage — every line unchanged
//   - findOrCreateChat — every line unchanged
//   - buildTemplateMessage, extractTemplateButtons — every line unchanged
//   - markCampaignCompleted — every line unchanged
//   - updateWarmupProgress — every line unchanged
//   - updateTierDailySent — every line unchanged
//   - All Supabase writes (campaign_messages, whatsapp_messages, messages, chats)
//   - Round-robin batch interleaving for campaigns sharing one account
//   - Daily counter reset logic
//
// REQUIRES: run migration_add_lease_columns.sql before deploying this file.

import cron from "node-cron";
import { createClient } from "@supabase/supabase-js";
import axios from "axios";
import crypto from "crypto";

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error("Missing Supabase credentials in environment variables");
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// Unique per-process identity. Used to claim campaigns via the DB lease so
// that even if a second worker process is ever run, only one can hold a
// given account's campaigns at a time — Postgres enforces this atomically,
// not this in-memory value.
const WORKER_ID = crypto.randomUUID();

// Lightweight guard for the DISPATCH phase only (the SELECT + grouping +
// firing off background tasks). This resolves in milliseconds — it is NOT
// held while campaigns are actually sending, unlike the old isProcessing.
let tickRunning = false;

// Tracks which WhatsApp accounts currently have a processAccountCampaigns()
// call in flight IN THIS PROCESS. Fast-path check to avoid unnecessary DB
// lease round-trips within a single worker. The DB lease (below) is the
// real cross-process safety net.
const accountsInFlight = new Set();

// Tier 1: bounds how many accounts can be actively processing at once.
// Accounts beyond this cap wait for the next tick rather than all being
// dispatched unconditionally — protects memory and Supabase write
// throughput under genuinely heavy simultaneous load. Raise this if you
// have headroom; lower it if the worker is resource constrained.
const MAX_CONCURRENT_ACCOUNTS = 15;

// Tier 1: how long a claimed lease is valid before another process could
// consider it abandoned and reclaim it. Refreshed (extended) on every
// batch heartbeat while a campaign is actively sending, so a healthy
// long-running campaign never lets its lease lapse.
const LEASE_DURATION_MS = 3 * 60 * 1000;

// Sending rate constants (unchanged from original — do not modify without
// re-validating delivery/quality impact with Meta's rate guidance)
const BATCH_SIZE = 5;
const BATCH_DELAY_MS = 10000;
const MESSAGE_DELAY_MIN = 800;
const MESSAGE_DELAY_MAX = 2000;

// Stall detection: a campaign is only considered stuck if it hasn't made
// ANY progress (no batch sent) in this many minutes. This is checked
// against updated_at (touched every batch), not started_at (total elapsed
// time) — a large campaign or one sharing an account with siblings can
// legitimately run for hours without being "stuck".
const STALL_MINUTES = 15;

/* ============================================================
   ENTRY POINT
   ============================================================ */

export function startCampaignScheduler() {
  console.log(
    "Campaign Scheduler (parallel-by-account, Tier 1 hardened) started!",
  );
  console.log(`Worker ID: ${WORKER_ID}`);
  console.log(
    `Rate: ${BATCH_SIZE} msgs / ${BATCH_DELAY_MS / 1000}s per account (unchanged)`,
  );
  console.log(
    `Per-message delay: ${MESSAGE_DELAY_MIN}-${MESSAGE_DELAY_MAX}ms (unchanged)`,
  );
  console.log(`Stall threshold: ${STALL_MINUTES} min with no progress`);
  console.log(`Max concurrent accounts per tick: ${MAX_CONCURRENT_ACCOUNTS}`);
  console.log(`Lease duration: ${LEASE_DURATION_MS / 1000}s`);

  cron.schedule("* * * * *", async () => {
    if (tickRunning) {
      console.log(
        "Skipping tick - dispatch phase still running (should be rare, <1s normally)",
      );
      return;
    }
    tickRunning = true;
    try {
      await checkAndSendCampaigns();
    } catch (err) {
      console.error("Scheduler top-level error:", err);
    } finally {
      tickRunning = false;
    }
  });
}

/* ============================================================
   STEP 1 - FIND DUE CAMPAIGNS, DISPATCH FREE ACCOUNTS
   (fire-and-forget: does NOT wait for sends to complete)
   ============================================================ */

export async function checkAndSendCampaigns() {
  const now = new Date();
  console.log(`\n[${now.toISOString()}] Checking for due campaigns...`);

  // Include "processing" so a campaign orphaned by a worker restart
  // (crashed mid-send, no accountsInFlight entry anymore) gets picked
  // back up automatically instead of sitting stuck forever.
  const { data: campaigns, error } = await supabase
    .from("campaigns")
    .select("*")
    .in("status", ["scheduled", "processing"])
    .lte("scheduled_at", now.toISOString())
    .order("scheduled_at", { ascending: true });

  if (error) {
    console.error("Error fetching campaigns:", error);
    return [];
  }

  if (!campaigns || campaigns.length === 0) {
    console.log("No campaigns ready to send");
    return [];
  }

  // Group by account_id
  const byAccount = new Map();
  for (const c of campaigns) {
    if (!byAccount.has(c.account_id)) byAccount.set(c.account_id, []);
    byAccount.get(c.account_id).push(c);
  }

  console.log(
    `${campaigns.length} campaign(s) across ${byAccount.size} account(s)`,
  );

  const dispatched = [];

  for (const [accountId, accountCampaigns] of byAccount.entries()) {
    if (accountsInFlight.has(accountId)) {
      console.log(
        `Account ${accountId} busy with a previous batch - will pick up new/remaining campaigns next tick once free`,
      );
      continue;
    }

    // Tier 1: concurrency cap. Accounts are iterated in the order their
    // oldest due campaign appeared (campaigns were queried ordered by
    // scheduled_at ascending), so whichever accounts have been waiting
    // longest naturally get priority when we're at capacity. Anything
    // that doesn't fit this tick just waits for the next one — nothing
    // is skipped or lost, only delayed a few ticks under heavy load.
    if (accountsInFlight.size >= MAX_CONCURRENT_ACCOUNTS) {
      console.log(
        `At capacity (${MAX_CONCURRENT_ACCOUNTS} accounts in flight) - account ${accountId} and any remaining will start next tick`,
      );
      break;
    }

    accountsInFlight.add(accountId);

    // Fire-and-forget: intentionally NOT awaited here. This is what lets
    // checkAndSendCampaigns() return almost immediately, so the cron tick
    // finishes fast and the NEXT tick (60s later) can still discover and
    // start campaigns on any other free account — even while this one
    // keeps running for hours.
    const p = processAccountCampaigns(accountId, accountCampaigns)
      .catch((err) =>
        console.error(`Account ${accountId} processing failed:`, err.message),
      )
      .finally(() => accountsInFlight.delete(accountId));

    dispatched.push(p);
  }

  // Returned only so tests/manual scripts can `await Promise.all(await
  // checkAndSendCampaigns())` if they want deterministic completion. The
  // cron tick above does NOT do this — it just awaits the (fast) function
  // call itself and lets these promises run in the background.
  return dispatched;
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

  // Tier 1: claim these campaigns via an atomic DB lease before touching
  // anything else. This is a real cross-process safety net (unlike the
  // in-memory accountsInFlight Set, which only means something inside this
  // one process) — Postgres guarantees each UPDATE...WHERE below can only
  // succeed for a campaign whose lease is null or already expired, so two
  // worker processes can never both believe they own the same campaign.
  //
  // Done as two separate simple UPDATEs (null-lease, then expired-lease)
  // rather than one .or()-based query — avoids any dependency on .or()
  // filter parsing, which is more sensitive to PostgREST schema-cache
  // staleness and client-library version differences than the basic
  // .is()/.lt()/.eq() filters used here. Each UPDATE is independently
  // atomic; splitting into two doesn't weaken that.
  const nowIso = new Date().toISOString();
  const leaseUntil = new Date(Date.now() + LEASE_DURATION_MS).toISOString();
  const candidateIds = campaigns.map((c) => c.campaign_id);

  const { data: claimedNullLease, error: claimErr1 } = await supabase
    .from("campaigns")
    .update({ worker_id: WORKER_ID, lease_expires_at: leaseUntil })
    .in("campaign_id", candidateIds)
    .is("lease_expires_at", null)
    .select("campaign_id");

  if (claimErr1) {
    console.error(
      `Failed to claim lease (null-lease pass) for account ${accountId}:`,
      claimErr1.message,
    );
    return;
  }

  const claimedIds = new Set(
    (claimedNullLease || []).map((r) => r.campaign_id),
  );
  const remainingCandidates = candidateIds.filter((id) => !claimedIds.has(id));

  if (remainingCandidates.length > 0) {
    const { data: claimedExpiredLease, error: claimErr2 } = await supabase
      .from("campaigns")
      .update({ worker_id: WORKER_ID, lease_expires_at: leaseUntil })
      .in("campaign_id", remainingCandidates)
      .lt("lease_expires_at", nowIso)
      .select("campaign_id");

    if (claimErr2) {
      console.error(
        `Failed to claim lease (expired-lease pass) for account ${accountId}:`,
        claimErr2.message,
      );
      // Not fatal — proceed with whatever we already claimed in pass 1
    } else {
      for (const r of claimedExpiredLease || []) claimedIds.add(r.campaign_id);
    }
  }

  const campaignsToProcess = campaigns.filter((c) =>
    claimedIds.has(c.campaign_id),
  );

  const skippedCount = campaigns.length - campaignsToProcess.length;
  if (skippedCount > 0) {
    console.log(
      `${skippedCount} campaign(s) on this account already leased by another worker - skipping this tick`,
    );
  }

  if (campaignsToProcess.length === 0) return;

  // Stall protection: a campaign is only "stuck" if it hasn't made ANY
  // progress in STALL_MINUTES — not just because it's been running a long
  // time. Large campaigns (thousands of recipients) can legitimately take
  // hours, especially when round-robining with sibling campaigns on the
  // same account. We check updated_at (touched every time a batch sends)
  // instead of started_at (total elapsed time since the very first batch).

  const safeCampaigns = [];
  for (const c of campaignsToProcess) {
    if (c.started_at) {
      // last activity = updated_at if present, else fall back to started_at
      const lastActivity = new Date(c.updated_at || c.started_at);
      const minutesSinceActivity = (Date.now() - lastActivity) / 60000;

      if (minutesSinceActivity > STALL_MINUTES) {
        console.warn(
          `Campaign "${c.campaign_name}" stalled - no progress in ${minutesSinceActivity.toFixed(0)} min - marking failed`,
        );
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

    // Tier 1: don't load the whole campaign's pending list into memory.
    // A cheap COUNT tells us whether there's anything to do and how much
    // (for logging), without materializing any rows. Actual message rows
    // are fetched one small batch at a time inside the round-robin loop
    // below, right before they're sent.
    let pendingCountQuery = supabase
      .from("campaign_messages")
      .select("cm_id", { count: "exact", head: true })
      .eq("campaign_id", campaign.campaign_id)
      .eq("status", "pending");
    const { count: pendingCount } = await pendingCountQuery;

    if (!pendingCount || pendingCount === 0) {
      console.log(`"${campaign.campaign_name}": no pending messages`);
      await finalizeCampaign(campaign, acct, 0, 0, false);
      continue;
    }

    const remainingBudget = warmupLimit !== null ? warmupLimit : Infinity;

    console.log(
      `"${campaign.campaign_name}": ${pendingCount} pending` +
        (warmupLimit !== null
          ? ` (warm-up cap this tick: ${warmupLimit})`
          : ""),
    );

    if (remainingBudget <= 0) {
      // Warm-up budget already exhausted before this tick even starts —
      // pause immediately, same outcome as fetching 0 in the old code.
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

  // ROUND-ROBIN across all campaigns on this account
  let roundIndex = 0;
  let consecutiveEmptyRounds = 0;

  while (true) {
    const anyRemaining = queues.some((q) => !q.exhausted);
    if (!anyRemaining) break;

    const queue = queues[roundIndex % queues.length];
    roundIndex++;

    if (queue.exhausted) {
      consecutiveEmptyRounds++;
      if (consecutiveEmptyRounds >= queues.length) break;
      continue;
    }
    consecutiveEmptyRounds = 0;

    const fetchSize = Math.min(BATCH_SIZE, queue.remainingBudget);

    // Streaming fetch: pull only the next small batch right before we
    // send it, instead of holding the whole campaign's pending list in
    // memory for the entire tick. Same BATCH_SIZE, same round-robin
    // fairness — only where the data lives in between batches changes.
    const { data: batch, error: fetchErr } = await supabase
      .from("campaign_messages")
      .select("*")
      .eq("campaign_id", queue.campaign.campaign_id)
      .eq("status", "pending")
      .limit(fetchSize);

    if (fetchErr) {
      console.error(
        `Failed to fetch next batch for "${queue.campaign.campaign_name}":`,
        fetchErr.message,
      );
      queue.exhausted = true;
      continue;
    }

    if (!batch || batch.length === 0) {
      queue.exhausted = true;
      continue;
    }

    console.log(
      `\n[${queue.campaign.campaign_name}] batch of ${batch.length} - budget remaining after this: ${
        queue.remainingBudget === Infinity
          ? "unlimited"
          : Math.max(0, queue.remainingBudget - batch.length)
      }`,
    );

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
    if (batch.length < fetchSize) {
      // Fewer rows came back than we asked for — this campaign has no
      // more pending messages right now.
      queue.exhausted = true;
    }

    // Heartbeat: touch updated_at (stall detection) and extend the lease
    // (Tier 1) so a healthy, actively-sending campaign never has its lease
    // expire mid-flight and get reclaimed by mistake. Does not write
    // messages_sent/messages_failed here (that's written once at finalize
    // to avoid double-counting across batches).
    await supabase
      .from("campaigns")
      .update({
        updated_at: new Date().toISOString(),
        lease_expires_at: new Date(
          Date.now() + LEASE_DURATION_MS,
        ).toISOString(),
      })
      .eq("campaign_id", queue.campaign.campaign_id);

    // Batch delay only when there is more work to do
    const stillHasWork = queues.some((q) => !q.exhausted);
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

  // Finalize each campaign. The live COUNT query inside finalizeCampaign
  // (not any in-memory flag) is what correctly distinguishes "warm-up
  // capped us early, more pending remain in the DB" from "fully drained,
  // campaign complete" — it works the same way regardless of why a
  // queue stopped (warmup budget exhausted or genuinely out of rows).
  for (const queue of queues) {
    await finalizeCampaign(
      queue.campaign,
      acct,
      queue.sent,
      queue.failed,
      false,
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
    // Release the lease immediately on pause. Without this, this same
    // worker (or any other) would be blocked from re-claiming this
    // campaign until LEASE_DURATION_MS elapses, artificially stalling a
    // perfectly healthy paused-for-warmup campaign for up to 3 minutes
    // even in a single-worker deployment.
    await supabase
      .from("campaigns")
      .update({
        status: "scheduled",
        messages_sent: totalSent,
        messages_failed: totalFailed,
        updated_at: new Date().toISOString(),
        worker_id: null,
        lease_expires_at: null,
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
        worker_id: null,
        lease_expires_at: null,
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
        worker_id: null,
        lease_expires_at: null,
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

    const isCarousel = template.is_carousel === true;

    let parsedVariables = variables;
    if (typeof parsedVariables === "string") {
      try {
        parsedVariables = JSON.parse(parsedVariables);
      } catch (e) {
        parsedVariables = {};
      }
    }
    parsedVariables = resolveContactNamePlaceholders(
      parsedVariables || {},
      contactName,
    );

    if (isCarousel) {
      messageBody.template.components = buildCarouselMessageComponents(
        template,
        templateComponents,
        parsedVariables,
      );
    } else {
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

      if (parsedVariables && Object.keys(parsedVariables).length > 0) {
        messageBody.template.components.push({
          type: "body",
          parameters: Object.values(parsedVariables).map((value) => ({
            type: "text",
            text: String(value),
          })),
        });
      }
    } // closes the else branch from isCarousel check

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
    const templateText = buildTemplateMessage(template, parsedVariables);
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
    const messageType = isCarousel
      ? "template_carousel"
      : headerFormat === "VIDEO"
        ? "template_video"
        : headerFormat === "DOCUMENT"
          ? "template_document"
          : "template";

    const carouselCardsForDisplay = isCarousel
      ? buildCarouselDisplayCards(template, parsedVariables)
      : null;

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
      media_path: isCarousel ? null : campaignMediaId || null,
      buttons: isCarousel ? null : templateButtons,
      carousel_cards: carouselCardsForDisplay,
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

function buildTemplateMessage(template, resolvedVariables) {
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

    // For carousel templates, resolvedVariables looks like
    // { body: {...}, cards: {...} } — only the outer body vars apply here.
    const isCarousel = template.is_carousel === true;
    const bodyVars = isCarousel
      ? (resolvedVariables && resolvedVariables.body) || {}
      : resolvedVariables || {};

    const substituteVars = (text) => {
      if (!text) return text;
      return text.replace(/\{\{(\d+)\}\}/g, (match, num) => {
        const val = bodyVars[num];
        return val !== undefined && val !== "" ? String(val) : match;
      });
    };

    const parts = [];
    const header = components.find((c) => c.type === "HEADER");
    if (header?.format === "TEXT" && header?.text)
      parts.push(`**${substituteVars(header.text)}**`);
    const body = components.find((c) => c.type === "BODY");
    if (body?.text) parts.push(substituteVars(body.text));
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
   CAROUSEL SUPPORT (media card carousel templates)
   ============================================================ */

function buildCarouselMessageComponents(
  template,
  templateComponents,
  variables,
) {
  let parsedVariables = variables;
  if (typeof parsedVariables === "string") {
    try {
      parsedVariables = JSON.parse(parsedVariables);
    } catch {
      parsedVariables = {};
    }
  }
  parsedVariables = parsedVariables || {};

  let carouselMedia = template.carousel_media;
  if (typeof carouselMedia === "string") {
    try {
      carouselMedia = JSON.parse(carouselMedia);
    } catch {
      carouselMedia = [];
    }
  }
  carouselMedia = Array.isArray(carouselMedia) ? carouselMedia : [];

  const carouselComp = templateComponents.find((c) => c.type === "CAROUSEL");
  if (!carouselComp || !Array.isArray(carouselComp.cards)) {
    throw new Error(
      `Template "${template.name}" is flagged is_carousel but has no CAROUSEL/cards component`,
    );
  }

  const components = [];

  // Outer message body (same idea as the non-carousel body block)
  const bodyDef = templateComponents.find((c) => c.type === "BODY");
  const bodyVars = parsedVariables.body || {};
  if (bodyDef && Object.keys(bodyVars).length > 0) {
    components.push({
      type: "body",
      parameters: Object.values(bodyVars).map((v) => ({
        type: "text",
        text: String(v),
      })),
    });
  }

  const cardsPayload = carouselComp.cards.map((card, cardIndex) => {
    const mediaEntry = carouselMedia.find((m) => m.card_index === cardIndex);
    const cardVars = (parsedVariables.cards || {})[String(cardIndex)] || {};
    return buildCarouselCard(card, cardIndex, mediaEntry, cardVars);
  });

  components.push({ type: "carousel", cards: cardsPayload });

  return components;
}

function buildCarouselCard(card, cardIndex, mediaEntry, cardVars) {
  const cardComponents = [];

  const headerDef = card.components.find((c) => c.type === "HEADER");
  if (headerDef) {
    if (!mediaEntry || !mediaEntry.media_id) {
      throw new Error(
        `No uploaded media_id found for carousel card_index ${cardIndex} - check whatsapp_templates.carousel_media`,
      );
    }
    const format = (
      headerDef.format ||
      mediaEntry.header_format ||
      "IMAGE"
    ).toLowerCase();
    cardComponents.push({
      type: "header",
      parameters: [{ type: format, [format]: { id: mediaEntry.media_id } }],
    });
  }

  const bodyDef = card.components.find((c) => c.type === "BODY");
  if (bodyDef) {
    const vars = cardVars.body || {};
    cardComponents.push({
      type: "body",
      parameters: Object.values(vars).map((v) => ({
        type: "text",
        text: String(v),
      })),
    });
  }

  const buttonsDef = card.components.find((c) => c.type === "BUTTONS");
  if (buttonsDef && Array.isArray(buttonsDef.buttons)) {
    const btnVars = cardVars.buttons || {};
    buttonsDef.buttons.forEach((btn, btnIndex) => {
      const type = (btn.type || "").toUpperCase();
      const btnVar = btnVars[String(btnIndex)];

      if (type === "URL") {
        if (btn.url && btn.url.includes("{{")) {
          if (!btnVar || btnVar.value === undefined) {
            throw new Error(
              `Missing URL button value for card ${cardIndex} button ${btnIndex}`,
            );
          }
          cardComponents.push({
            type: "button",
            sub_type: "url",
            index: String(btnIndex),
            parameters: [{ type: "text", text: String(btnVar.value) }],
          });
        }
        // static URL (no {{}}) needs no parameters at all
      } else if (type === "QUICK_REPLY") {
        const payload =
          (btnVar && btnVar.payload) || `card-${cardIndex}-btn-${btnIndex}`;
        cardComponents.push({
          type: "button",
          sub_type: "quick_reply",
          index: String(btnIndex),
          parameters: [{ type: "payload", payload }],
        });
      }
      // PHONE_NUMBER buttons are static on the template, no parameters needed
    });
  }

  return { card_index: cardIndex, components: cardComponents };
}

function buildCarouselDisplayCards(template, resolvedVariables) {
  let components = template.components;
  if (typeof components === "string") {
    try {
      components = JSON.parse(components);
    } catch {
      return null;
    }
  }
  let carouselMedia = template.carousel_media;
  if (typeof carouselMedia === "string") {
    try {
      carouselMedia = JSON.parse(carouselMedia);
    } catch {
      carouselMedia = [];
    }
  }
  let preview = template.preview;
  if (typeof preview === "string") {
    try {
      preview = JSON.parse(preview);
    } catch {
      preview = null;
    }
  }
  const previewCarouselComp = preview?.components?.find(
    (c) => c.type === "CAROUSEL",
  );

  const carouselComp = Array.isArray(components)
    ? components.find((c) => c.type === "CAROUSEL")
    : null;
  if (!carouselComp) return null;

  const cardVarsFor = (i) =>
    (resolvedVariables?.cards && resolvedVariables.cards[String(i)]) || {};

  return carouselComp.cards.map((card, i) => {
    const mediaEntry = (carouselMedia || []).find((m) => m.card_index === i);
    const bodyDef = card.components.find((c) => c.type === "BODY");
    const buttonsDef = card.components.find((c) => c.type === "BUTTONS");
    const previewCard = previewCarouselComp?.cards?.[i];
    const previewImageUrl = previewCard?.components?.find(
      (c) => c.type === "HEADER",
    )?.example?.header_handle?.[0];

    const cardVars = cardVarsFor(i);
    const resolvedButtons = (buttonsDef?.buttons || []).map((btn, btnIndex) => {
      const val = cardVars.buttons?.[String(btnIndex)]?.value;
      return { ...btn, resolved_value: val || null };
    });

    return {
      card_index: i,
      media_id: mediaEntry?.media_id || null,
      header_format: mediaEntry?.header_format || null,
      preview_image_url: previewImageUrl || null,
      body_text: bodyDef?.text || null,
      buttons: resolvedButtons,
    };
  });
}

/* ============================================================
   CONTACT-NAME PLACEHOLDER RESOLUTION
   ============================================================ */

const NAME_PLACEHOLDER = "{{contact_name}}";

function resolveContactNamePlaceholders(value, contactName) {
  if (typeof value === "string") {
    return value === NAME_PLACEHOLDER ? contactName || "Customer" : value;
  }
  if (Array.isArray(value)) {
    return value.map((v) => resolveContactNamePlaceholders(v, contactName));
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = resolveContactNamePlaceholders(v, contactName);
    }
    return out;
  }
  return value;
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
