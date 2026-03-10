// ============================================================================
// Trackster Orchestrator – OEM CAN/FDFD Pipeline
// ============================================================================
// Purpose:
//   Convert "summary" mode input into many SQS messages.
//   Each vehicle = ONE SQS message (the new worker generates ONE .bin per message).
// ============================================================================

const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");
const REGION = process.env.AWS_REGION || "eu-north-1";
const WORK_QUEUE_URL = process.env.WORK_QUEUE_URL;
const SQS_BATCH = 10;                     // SQS limit
const MAX_VEHICLES = 150000;              // protection

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With"
};

const MAX_VIN_SEQ = 99999;
const DEFAULT_VIN_PREFIX = "JH4DA1";
const VEH_SUFFIX = "RX3H9F";

function normalizeVinPrefix(raw) {
  const cleaned = String(raw || DEFAULT_VIN_PREFIX).trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
  return cleaned.slice(0, 6) || DEFAULT_VIN_PREFIX;
}

function makeVin(prefix, seq) {
  return `${normalizeVinPrefix(prefix)}${pad5(seq)}${VEH_SUFFIX}`;
}

function pad5(n) {
  return String(n).padStart(5, "0");
}

function makeRunIdUTC() {
  const d = new Date();

  const p2 = n => String(n).padStart(2, "0");

  const year  = d.getUTCFullYear();
  const month = p2(d.getUTCMonth() + 1);
  const day   = p2(d.getUTCDate());
  const hour  = p2(d.getUTCHours());
  const min   = p2(d.getUTCMinutes());
  const sec   = p2(d.getUTCSeconds());

  const ms  = p2(Math.floor(d.getUTCMilliseconds()));

  return `${year}${month}${day}T${hour}${min}${sec}${ms}`;
}

function httpResp(code, obj) {
  return {
    statusCode: code,
    headers: { "Content-Type": "application/json", ...CORS },
    body: JSON.stringify(obj)
  };
}

function cors204() {
  return { statusCode: 204, headers: { ...CORS }, body: "" };
}

function getHttpMethod(event) {
  return (event?.httpMethod || event?.requestContext?.http?.method || "").toUpperCase();
}

function parseBody(event) {
  const method = getHttpMethod(event);
  if (method === "OPTIONS") return { __preflight: true };

  if (typeof event?.body === "string") {
    try { return JSON.parse(event.body); }
    catch { return {}; }
  }

  return event?.body || event || {};
}

// const VIN_PREFIX_BY_TYPE = {
//   car: "CAR",
//   bus: "BUS",
//   truck: "TRK",
//   bike: "BIK",
//   train: "TRN"
// };

const TYPE_ORDER = ["car", "bus", "truck", "bike", "train"];

function expandVehiclesFromSummary(p) {
  if (p.mode !== "summary" || !p.summary?.byType) return [];

  const byType = p.summary.byType;

  // Deterministic order (always same output)
  const entries = TYPE_ORDER
    .filter(t => byType[t] !== undefined)
    .map(t => [t, byType[t]]);

  // Also include any unknown types at the end (stable)
  for (const [k, v] of Object.entries(byType)) {
    const key = String(k).toLowerCase();
    if (!TYPE_ORDER.includes(key)) entries.push([key, v]);
  }

  const list = [];
  let seq = 1;

  for (const [type, count] of entries) {
    const n = Math.max(0, parseInt(count || 0, 10));

    for (let i = 0; i < n; i++) {
      if (seq > MAX_VIN_SEQ) {
        // You can throw here; handler will catch and return 500/400 depending on your preference.
        throw new Error(`VIN sequence overflow: requested more than ${MAX_VIN_SEQ} vehicles but VIN format supports only 5 digits`);
      }

      list.push({
        vin: makeVin(p.vinPrefix, seq++),
        type
      });
    }
  }

  return list;
}

function sanitizeError(err) {
  return {
    name: err?.name || "Error",
    message: err?.message || String(err),
    stack: err?.stack || null
  };
}

module.exports.handler = async (event, context) => {
  const requestId = context?.awsRequestId;
  const runId = makeRunIdUTC();

  const epochMs = Date.now();
  const epochSec = Math.floor(epochMs / 1000);

  try {
    const method = getHttpMethod(event);
    if (method === "OPTIONS") return cors204();

    const p = parseBody(event);
    if (p.__preflight) return cors204();

    if (p.mode !== "summary") {
      return httpResp(400, {
        requestId,
        error: "Only 'summary' mode is supported here. Use direct lambda for detailed ≤20 vehicles."
      });
    }

    if (!WORK_QUEUE_URL) {
      return httpResp(500, { requestId, error: "WORK_QUEUE_URL not set" });
    }

    const intervalSec = Math.max(1, parseInt(p.intervalSec || 5, 10));
    const durationSec = Math.max(1, parseInt(p.durationSec || 60, 10));

    const vehicles = expandVehiclesFromSummary(p);
    if (!vehicles.length) {
      return httpResp(400, { requestId, error: "summary.byType resolved to 0 vehicles" });
    }

    if (vehicles.length > MAX_VEHICLES) {
      return httpResp(400, {
        requestId,
        error: `Vehicle count too large (${vehicles.length}). Max allowed is ${MAX_VEHICLES}.`
      });
    }

    console.log(`[ORCHESTRATOR] requestId=${requestId} vehicles=${vehicles.length} queue=${WORK_QUEUE_URL}`);

    const sqs = new SQSClient({ region: REGION });

    const allEntries = vehicles.map((v, idx) => ({
      Id: `v-${idx}`,
      MessageBody: JSON.stringify({
        vin: v.vin,
        type: v.type,
        intervalSec,
        durationSec,
        epochMs,
        epochSec,

        runId
      })
    }));

    let sentBatches = 0;
    for (let i = 0; i < allEntries.length; i += SQS_BATCH) {
      const batch = allEntries.slice(i, i + SQS_BATCH);

      const resp = await sqs.send(new SendMessageBatchCommand({
        QueueUrl: WORK_QUEUE_URL,
        Entries: batch
      }));

      sentBatches++;

      if (resp.Failed && resp.Failed.length) {
        console.error("[ORCHESTRATOR] SQS batch failures:", JSON.stringify(resp.Failed, null, 2));

        return httpResp(500, {
          requestId,
          error: "SQS SendMessageBatch failed",
          failed: resp.Failed
        });
      }
    }

    return httpResp(202, {
      requestId,
      enqueued_vehicles: vehicles.length,
      sentBatches,
      queue_url: WORK_QUEUE_URL,
      intervalSec,
      durationSec,
      epochSec,
      runId
    });

  } catch (err) {
    console.error("[ORCHESTRATOR] Unhandled error:", err);
    return httpResp(500, {
      requestId,
      error: "Unhandled exception in orchestrator",
      details: sanitizeError(err)
    });
  }
};
