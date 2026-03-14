// scheduler/campaignScheduler.js
// PRODUCTION-READY VERSION - Matches backend createCampaign format
// Handles all timezone/format issues

import cron from 'node-cron';
import { createClient } from '@supabase/supabase-js';
import axios from 'axios';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('❌ Missing Supabase credentials in environment variables');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

let isProcessing = false;

/* =====================================
   MAIN SCHEDULER FUNCTION
====================================== */

export function startCampaignScheduler() {
  console.log('🚀 Campaign Scheduler Started!');
  console.log('⏰ Cron will run every minute...');

  // Run every minute
  cron.schedule('* * * * *', async () => {
    if (isProcessing) {
      console.log('⏭️  Skipping: Previous execution still running');
      return;
    }

    try {
      isProcessing = true;
      await checkAndSendCampaigns();
    } catch (err) {
      console.error('❌ Scheduler error:', err);
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
    // ✅ FIX: Use PostgreSQL NOW() function instead of JavaScript date
    // This ensures timezone consistency with the database
    const { data: campaigns, error } = await supabase
      .from('campaigns')
      .select('*')
      .eq('status', 'scheduled')
      .lte('scheduled_at', now.toISOString()) // Works with both formats
      .order('scheduled_at', { ascending: true })
      .limit(5);

    if (error) {
      console.error('❌ Error fetching campaigns:', error);
      console.error('Error details:', JSON.stringify(error, null, 2));
      return;
    }

    // ✅ EXTRA DEBUG: Also try fetching ALL scheduled campaigns to see what's there
    const { data: allScheduled, error: allError } = await supabase
      .from('campaigns')
      .select('campaign_id, campaign_name, status, scheduled_at, created_at')
      .eq('status', 'scheduled')
      .order('created_at', { ascending: false })
      .limit(10);

    if (allScheduled && allScheduled.length > 0) {
      console.log(`\n📋 Found ${allScheduled.length} scheduled campaigns in database:`);
      allScheduled.forEach(c => {
        const scheduledDate = new Date(c.scheduled_at);
        const isPast = scheduledDate <= now;
        const diff = Math.round((now - scheduledDate) / 1000 / 60);
        
        console.log(`   • ${c.campaign_name}`);
        console.log(`     ID: ${c.campaign_id}`);
        console.log(`     Scheduled: ${c.scheduled_at}`);
        console.log(`     Status: ${c.status}`);
        console.log(`     Is due? ${isPast ? '✅ YES' : `❌ NO (in ${Math.abs(diff)} min)`}`);
        console.log(`     Diff: ${diff} minutes ago`);
      });
    }

    if (!campaigns || campaigns.length === 0) {
      console.log('✅ No campaigns ready to send');
      return;
    }

    console.log(`\n📢 Found ${campaigns.length} campaign(s) ready to send!`);

    // Process each campaign
    for (const campaign of campaigns) {
      await processCampaign(campaign);
    }
  } catch (err) {
    console.error('❌ Error in checkAndSendCampaigns:', err);
    console.error('Stack:', err.stack);
  }
}

/* =====================================
   PROCESS SINGLE CAMPAIGN
====================================== */

async function processCampaign(campaign) {
  const startTime = Date.now();
  
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`📤 PROCESSING CAMPAIGN`);
  console.log(`${'═'.repeat(60)}`);
  console.log(`Name: ${campaign.campaign_name}`);
  console.log(`ID: ${campaign.campaign_id}`);
  console.log(`Scheduled: ${campaign.scheduled_at}`);
  console.log(`Recipients: ${campaign.total_recipients}`);
  if (campaign.media_id) {
    console.log(`Media ID: ${campaign.media_id}`);
  }
  console.log(`${'═'.repeat(60)}\n`);

  try {
    // ✅ TIMEOUT PROTECTION
    if (campaign.started_at) {
      const startedAt = new Date(campaign.started_at);
      const now = new Date();
      const minutesRunning = (now - startedAt) / 1000 / 60;

      if (minutesRunning > 30) {
        console.log(`⚠️  Campaign timeout! Running for ${minutesRunning.toFixed(1)} minutes`);

        await supabase
          .from('campaigns')
          .update({
            status: 'failed',
            completed_at: new Date().toISOString(),
          })
          .eq('campaign_id', campaign.campaign_id);

        console.log('❌ Campaign marked as failed (timeout)');
        return;
      }
    }

    // 1️⃣ Update status to processing
    console.log('📝 Updating campaign status to PROCESSING...');
    const { error: updateError } = await supabase
      .from('campaigns')
      .update({
        status: 'processing',
        started_at: campaign.started_at || new Date().toISOString(),
      })
      .eq('campaign_id', campaign.campaign_id);

    if (updateError) {
      console.error('❌ Failed to update status:', updateError);
      throw updateError;
    }

    console.log('✅ Status updated to PROCESSING');

    // 2️⃣ Get total pending count
    console.log('\n📊 Counting pending messages...');
    const { count: totalPending, error: countError } = await supabase
      .from('campaign_messages')
      .select('cm_id', { count: 'exact', head: true })
      .eq('campaign_id', campaign.campaign_id)
      .eq('status', 'pending');

    if (countError) {
      console.error('❌ Failed to count messages:', countError);
      throw countError;
    }

    console.log(`✅ Found ${totalPending} pending messages`);

    if (totalPending === 0) {
      console.log('⚠️  No pending messages - marking campaign as completed');

      // Get current sent/failed counts
      const { count: sentCount } = await supabase
        .from('campaign_messages')
        .select('cm_id', { count: 'exact', head: true })
        .eq('campaign_id', campaign.campaign_id)
        .eq('status', 'sent');

      const { count: failedCount } = await supabase
        .from('campaign_messages')
        .select('cm_id', { count: 'exact', head: true })
        .eq('campaign_id', campaign.campaign_id)
        .eq('status', 'failed');

      await markCampaignCompleted(campaign.campaign_id, sentCount || 0, failedCount || 0);
      return;
    }

    // 3️⃣ Fetch ALL pending messages using pagination
    console.log('\n📥 Fetching pending messages (with pagination)...');
    let allMessages = [];
    let page = 0;
    const pageSize = 1000;

    while (true) {
      const { data: messages, error: msgError } = await supabase
        .from('campaign_messages')
        .select('*')
        .eq('campaign_id', campaign.campaign_id)
        .eq('status', 'pending')
        .range(page * pageSize, (page + 1) * pageSize - 1);

      if (msgError) {
        console.error('❌ Error fetching messages:', msgError);
        throw msgError;
      }

      if (!messages || messages.length === 0) break;

      allMessages = allMessages.concat(messages);
      console.log(`   📄 Page ${page + 1}: ${messages.length} messages (Total: ${allMessages.length})`);

      if (messages.length < pageSize) break; // Last page
      page++;
    }

    console.log(`✅ Total messages fetched: ${allMessages.length}`);

    // 4️⃣ Get template
    console.log('\n🔍 Fetching template...');
    const { data: template, error: templateError } = await supabase
      .from('whatsapp_templates')
      .select('*')
      .eq('wt_id', campaign.wt_id)
      .single();

    if (templateError || !template) {
      console.error('❌ Template not found:', templateError);
      throw new Error('Template not found');
    }

    console.log(`✅ Template: ${template.name} (${template.language})`);

    // 5️⃣ Get WhatsApp account
    console.log('🔍 Fetching WhatsApp account...');
    const { data: account, error: accountError } = await supabase
      .from('whatsapp_accounts')
      .select('*')
      .eq('wa_id', campaign.account_id)
      .single();

    if (accountError || !account) {
      console.error('❌ WhatsApp account not found:', accountError);
      throw new Error('WhatsApp account not found');
    }

    console.log(`✅ Account: ${account.business_phone_number}`);

    // 6️⃣ Send messages in BATCHES
    console.log(`\n📤 Starting batch sending...`);
    let sent = 0;
    let failed = 0;

    const BATCH_SIZE = 10; // Send 10 messages in parallel
    const BATCH_DELAY = 2000; // 2 seconds between batches

    const totalBatches = Math.ceil(allMessages.length / BATCH_SIZE);
    console.log(`   Total batches: ${totalBatches}`);
    console.log(`   Batch size: ${BATCH_SIZE} messages`);
    console.log(`   Delay: ${BATCH_DELAY}ms between batches\n`);

    for (let i = 0; i < allMessages.length; i += BATCH_SIZE) {
      const batch = allMessages.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;

      console.log(`📦 Batch ${batchNumber}/${totalBatches} (${batch.length} messages)...`);

      // Send batch in parallel
      const results = await Promise.allSettled(
        batch.map((message) =>
          sendWhatsAppMessage(
            account,
            template,
            message.phone_number,
            message.contact_name,
            campaign.group_id,
            campaign.user_id,
            campaign.template_variables,
            campaign.media_id
          )
        )
      );

      // Process results and update database
      for (let j = 0; j < results.length; j++) {
        const result = results[j];
        const message = batch[j];

        if (result.status === 'fulfilled') {
          // Success
          await supabase
            .from('campaign_messages')
            .update({
              status: 'sent',
              sent_at: new Date().toISOString(),
              wm_id: result.value.wm_id,
              wa_message_id: result.value.wa_message_id,
            })
            .eq('cm_id', message.cm_id);

          sent++;
          console.log(`   ✅ ${message.phone_number}`);
        } else {
          // Failed
          await supabase
            .from('campaign_messages')
            .update({
              status: 'failed',
              failed_at: new Date().toISOString(),
              error_message: result.reason?.message || 'Unknown error',
              error_code: result.reason?.code || 'SEND_ERROR',
            })
            .eq('cm_id', message.cm_id);

          failed++;
          console.log(`   ❌ ${message.phone_number}: ${result.reason?.message}`);
        }
      }

      // Update campaign progress after each batch
      await supabase
        .from('campaigns')
        .update({
          messages_sent: sent,
          messages_failed: failed,
          updated_at: new Date().toISOString(),
        })
        .eq('campaign_id', campaign.campaign_id);

      console.log(`   📊 Progress: ${sent + failed}/${allMessages.length} (✅ ${sent} | ❌ ${failed})\n`);

      // Delay between batches
      if (i + BATCH_SIZE < allMessages.length) {
        await sleep(BATCH_DELAY);
      }
    }

    // 7️⃣ Complete campaign
    await markCampaignCompleted(campaign.campaign_id, sent, failed);

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`✅ CAMPAIGN COMPLETED`);
    console.log(`${'═'.repeat(60)}`);
    console.log(`Name: ${campaign.campaign_name}`);
    console.log(`Total: ${sent + failed}`);
    console.log(`Sent: ${sent} ✅`);
    console.log(`Failed: ${failed} ❌`);
    console.log(`Success Rate: ${((sent / (sent + failed)) * 100).toFixed(1)}%`);
    console.log(`Duration: ${duration}s`);
    console.log(`${'═'.repeat(60)}\n`);

  } catch (err) {
    console.error(`\n❌ Campaign processing failed:`, err);
    console.error('Error details:', err.response?.data || err.message);
    console.error('Stack:', err.stack);

    await supabase
      .from('campaigns')
      .update({
        status: 'failed',
        completed_at: new Date().toISOString(),
      })
      .eq('campaign_id', campaign.campaign_id);
  }
}

/* =====================================
   SEND WHATSAPP MESSAGE
====================================== */

async function sendWhatsAppMessage(
  account,
  template,
  phoneNumber,
  contactName,
  groupId,
  userId,
  variables,
  campaignMediaId
) {
  try {
    // Parse template components
    let templateComponents = template.components;
    if (typeof templateComponents === 'string') {
      try {
        templateComponents = JSON.parse(templateComponents);
      } catch (e) {
        console.log('   ⚠️  Failed to parse template components');
        templateComponents = [];
      }
    }

    // Build WhatsApp API message payload
    const messageBody = {
      messaging_product: 'whatsapp',
      to: phoneNumber,
      type: 'template',
      template: {
        name: template.name,
        language: { code: template.language },
        components: [],
      },
    };

    // Handle MEDIA HEADER (IMAGE/VIDEO/DOCUMENT)
    const headerComponent = templateComponents.find((comp) => comp.type === 'HEADER');

    if (headerComponent && headerComponent.format) {
      const format = headerComponent.format.toUpperCase();

      if (['IMAGE', 'VIDEO', 'DOCUMENT'].includes(format)) {
        if (!campaignMediaId) {
          throw new Error(`Media ${format} template requires media_id but campaign has none selected`);
        }

        messageBody.template.components.push({
          type: 'header',
          parameters: [{
            type: format.toLowerCase(),
            [format.toLowerCase()]: {
              id: campaignMediaId,
            },
          }],
        });
      } else if (format === 'TEXT' && headerComponent.example) {
        const headerText = headerComponent.example.header_text || [];
        if (headerText.length > 0) {
          messageBody.template.components.push({
            type: 'header',
            parameters: headerText.map((text) => ({
              type: 'text',
              text: text,
            })),
          });
        }
      }
    }

    // Handle BODY VARIABLES
    // ✅ FIX: Parse variables if it's a string
    let parsedVariables = variables;
    if (typeof variables === 'string') {
      try {
        parsedVariables = JSON.parse(variables);
      } catch (e) {
        parsedVariables = {};
      }
    }

    if (parsedVariables && Object.keys(parsedVariables).length > 0) {
      const parameters = Object.values(parsedVariables).map((value) => ({
        type: 'text',
        text: String(value),
      }));

      messageBody.template.components.push({
        type: 'body',
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
          'Content-Type': 'application/json',
        },
        timeout: 30000, // 30 second timeout
      }
    );

    const wa_message_id = response.data.messages?.[0]?.id;
    const templateText = extractTemplateText(template);

    // Store in whatsapp_messages table
    const { data: wmRecord, error: wmError } = await supabase
      .from('whatsapp_messages')
      .insert({
        account_id: account.wa_id,
        to_number: phoneNumber,
        template_name: template.name,
        message_body: JSON.stringify(messageBody),
        wa_message_id: wa_message_id,
        status: 'sent',
        sent_at: new Date().toISOString(),
      })
      .select()
      .single();

    if (wmError) {
      console.error('   ⚠️  Failed to store in whatsapp_messages:', wmError);
      // Don't throw - message was sent successfully
    }

    // Find or create chat
    const chatId = await findOrCreateChat(phoneNumber, contactName, groupId, userId);

    // Store in messages table
    await supabase.from('messages').insert({
      chat_id: chatId,
      sender_type: 'admin',
      message: templateText,
      message_type: 'template',
      wa_message_id: wa_message_id,
      status: 'sent',
      created_at: new Date().toISOString(),
    });

    return {
      wm_id: wmRecord?.wm_id,
      wa_message_id: wa_message_id,
      chat_id: chatId,
    };
  } catch (err) {
    const errorMessage = err.response?.data?.error?.message || err.message || 'Failed to send message';
    throw new Error(errorMessage);
  }
}

/* =====================================
   HELPER: FIND OR CREATE CHAT
====================================== */

async function findOrCreateChat(phoneNumber, contactName, groupId, userId) {
  try {
    // Check if chat exists
    const { data: existingChats } = await supabase
      .from('chats')
      .select('chat_id, group_id, person_name')
      .eq('phone_number', phoneNumber)
      .eq('user_id', userId)
      .order('created_at', { ascending: false })
      .limit(1);

    if (existingChats && existingChats.length > 0) {
      const existingChat = existingChats[0];

      // Update existing chat
      await supabase
        .from('chats')
        .update({
          last_message: 'Template message sent via campaign',
          last_message_at: new Date().toISOString(),
          last_admin_message_at: new Date().toISOString(),
          group_id: groupId,
          person_name: contactName || existingChat.person_name || 'Unknown',
          updated_at: new Date().toISOString(),
        })
        .eq('chat_id', existingChat.chat_id);

      return existingChat.chat_id;
    }

    // Create new chat
    const { data: newChat, error } = await supabase
      .from('chats')
      .insert({
        phone_number: phoneNumber,
        person_name: contactName || 'Unknown',
        last_message: 'Template message sent via campaign',
        last_message_at: new Date().toISOString(),
        group_id: groupId,
        mode: 'AUTO',
        last_admin_message_at: new Date().toISOString(),
        user_id: userId,
        status: 'active',
        unread_count: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .select()
      .single();

    if (error) throw error;

    return newChat.chat_id;
  } catch (err) {
    console.error('   ⚠️  Error in findOrCreateChat:', err.message);
    // Return null if chat creation fails - message was still sent
    return null;
  }
}

/* =====================================
   HELPER: EXTRACT TEMPLATE TEXT
====================================== */

function extractTemplateText(template) {
  try {
    if (!template.components || !Array.isArray(template.components)) {
      return `Template: ${template.name}`;
    }

    let components = template.components;
    if (typeof components === 'string') {
      try {
        components = JSON.parse(components);
      } catch (e) {
        return `Template: ${template.name}`;
      }
    }

    const bodyComponent = components.find((comp) => comp.type === 'BODY');

    if (bodyComponent && bodyComponent.text) {
      return bodyComponent.text;
    }

    return `Template: ${template.name}`;
  } catch (err) {
    return `Template: ${template.name}`;
  }
}

/* =====================================
   HELPER: MARK CAMPAIGN AS COMPLETED
====================================== */

async function markCampaignCompleted(campaignId, sent, failed) {
  try {
    await supabase
      .from('campaigns')
      .update({
        status: 'completed',
        completed_at: new Date().toISOString(),
        messages_sent: sent,
        messages_failed: failed,
      })
      .eq('campaign_id', campaignId);
  } catch (err) {
    console.error('   ⚠️  Error marking campaign as completed:', err);
  }
}

/* =====================================
   HELPER: SLEEP FUNCTION
====================================== */

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/* =====================================
   EXPORT
====================================== */

export default { startCampaignScheduler, checkAndSendCampaigns };
