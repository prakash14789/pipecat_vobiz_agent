const fs = require('fs');
const path = require('path');

function getRealEstateServerDir() {
  const candidates = [
    process.env.REALESTATE_SERVER_DIR,
    path.resolve(__dirname, '../../crm(corely)/realestate/server'),
    path.resolve(__dirname, '../crm(corely)/realestate/server'),
    path.resolve(__dirname, '../../realestate/server'),
    path.resolve(__dirname, '../realestate/server'),
    '/root/Aibot/crm(corely)/realestate/server',
    '/root/Aibot/realestate/server',
    '/root/Aibot/outbound-ai/crm(corely)/realestate/server',
    '/Users/prakash/Documents/kpro/crm(corely)/realestate/server',
    'C:/Users/an941/Desktop/KPro Stuffs/D-Pro/realestate/server',
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return candidates[0] || '';
}

const serverDir = getRealEstateServerDir();
const mongoosePath = path.join(serverDir, 'node_modules', 'mongoose');
const workflowPath = path.join(serverDir, 'features', 'workflows', 'missedCalls.workflow.js');

let mongoose;
try {
  mongoose = require(fs.existsSync(mongoosePath) ? mongoosePath : 'mongoose');
} catch (e) {
  mongoose = require('mongoose');
}

let getInboundMissedCallsAnalytics;
try {
  getInboundMissedCallsAnalytics = require(workflowPath).getInboundMissedCallsAnalytics;
} catch (err) {
  console.error(`[CRM Service] Cannot find missedCalls.workflow.js at "${workflowPath}". Set REALESTATE_SERVER_DIR in .env to the realestate server path.`);
  process.exit(1);
}

const MONGO_URI = process.env.REAL_MONGO_URI || 'mongodb://mongo_admin:Realestate123%24@76.13.242.179:27017/realestate_db?authSource=admin';

let isConnected = false;

// Connect to MongoDB realestate_db
async function connect() {
  if (isConnected) return;
  await mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 8000 });
  isConnected = true;
}

// Fetch pending missed calls from CRM workflow
async function fetchPendingCalls(startDate, endDate) {
  await connect();
  const query = {
    callbackStatus: 'pending',
    callbackStatusFilter: 'pending',
    limit: 100,
  };
  if (startDate) query.startDate = startDate;
  if (endDate) query.endDate = endDate;
  if (!startDate && !endDate) query.timeRange = 'today';

  let allCalls = [];
  let page = 1;
  let totalReported = 0;
  let lastKpis = {};

  // Loop through all pages because CRM workflow clamps limit to 100 per page
  while (true) {
    query.page = page;
    const res = await getInboundMissedCallsAnalytics(null, query);
    lastKpis = res.kpis || lastKpis;
    totalReported = res.pagination?.total || totalReported;
    const batch = res.calls || [];
    if (batch.length === 0) break;
    allCalls.push(...batch);
    if (batch.length < 100 || allCalls.length >= totalReported || page >= 10) break;
    page++;
  }
  
  // Format clean results for voice outbound queue
  const calls = allCalls.map(c => ({
    id: c.id,
    callId: c.callId,
    phone_number: c.customerNumber,
    customer_name: c.customerName !== 'nt' && c.customerName !== 'Unknown' ? c.customerName : '',
    source: c.source,
    dialedDid: c.dialedDid,
    campaign: c.campaign,
    missedAgents: c.missedAgents || [],
    duration: c.duration,
    circle: c.circle,
    operator: c.operator,
    createdAt: c.createdAt,
    status: 'pending',
  }));

  // Deduplicate by 10-digit unique phone number
  const seenNumbers = new Set();
  const uniqueCalls = [];
  for (const c of calls) {
    const rawDigits = String(c.phone_number || '').replace(/\D/g, '').slice(-10);
    if (rawDigits && !seenNumbers.has(rawDigits)) {
      seenNumbers.add(rawDigits);
      uniqueCalls.push(c);
    }
  }

  return {
    kpis: lastKpis,
    calls: calls,
    uniqueCalls: uniqueCalls,
    total: calls.length,
    uniqueTotal: uniqueCalls.length,
    totalReported: totalReported,
  };
}

if (require.main === module) {
  const args = process.argv.slice(2);
  const start = args[0] ? args[0].trim() : '';
  const end = args[1] ? args[1].trim() : '';
  fetchPendingCalls(start, end).then(data => {
    process.stdout.write(JSON.stringify(data) + '\n', () => {
      process.exit(0);
    });
  }).catch(err => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { fetchPendingCalls };
