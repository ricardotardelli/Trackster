// ============================================================================
// Trackster Orchestrator - OEM CAN/FDFD Pipeline
// ============================================================================
// Purpose:
//   Convert frontend payload into many SQS messages.
//   Each vehicle = ONE SQS message (the worker generates ONE .bin per message).
// ============================================================================

const REGION = "us-east-1";
const SQS_BATCH = 10;
const MAX_VEHICLES = 150000;
const SEND_TO_SQS = false;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With"
};

const MAX_VIN_SEQ = 999999;
const VIN_SEQ_LENGTH = String(MAX_VIN_SEQ).length;
const VIN_PREFIX_LENGTH = 6;
const VIN_SUFFIX_LENGTH = 17 - VIN_PREFIX_LENGTH - VIN_SEQ_LENGTH;

function normalizeVinPrefix(raw) {
  return String(raw || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, VIN_PREFIX_LENGTH);
}

function normalizeVinSuffix(raw) {
  return String(raw || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, VIN_SUFFIX_LENGTH);
}

function padSeq(n) {
  return String(n).padStart(VIN_SEQ_LENGTH, "0");
}

function makeVin(prefix, suffix, seq) {
  return `${normalizeVinPrefix(prefix)}${padSeq(seq)}${normalizeVinSuffix(suffix)}`;
}

function makeRunIdUTC() {
  const d = new Date();
  const p2 = (n) => String(n).padStart(2, "0");

  const year = d.getUTCFullYear();
  const month = p2(d.getUTCMonth() + 1);
  const day = p2(d.getUTCDate());
  const hour = p2(d.getUTCHours());
  const min = p2(d.getUTCMinutes());
  const sec = p2(d.getUTCSeconds());
  const ms = p2(Math.floor(d.getUTCMilliseconds()));

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
    try {
      return JSON.parse(event.body);
    } catch {
      return {};
    }
  }

  return event?.body || event || {};
}

function parsePositiveInt(value) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parsePositiveNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function expandVehicles(totalVehicles, vinPrefix, vinSuffix) {
  const total = Math.max(0, parsePositiveInt(totalVehicles));
  const list = [];
  let seq = 1;

  for (let i = 0; i < total; i++) {
    if (seq > MAX_VIN_SEQ) {
      throw new Error(`VIN sequence overflow: requested more than ${MAX_VIN_SEQ} vehicles but VIN format supports only ${VIN_SEQ_LENGTH} digits`);
    }

    list.push({
      vin: makeVin(vinPrefix, vinSuffix, seq++),
      type: "car"
    });
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

function resolveWorkQueueUrl(payload) {
  return String(payload?.workQueueUrl || "").trim();
}

module.exports.handler = async (event, context) => {
  const requestId = context?.awsRequestId;
  const runId = makeRunIdUTC();

  try {
    const method = getHttpMethod(event);
    if (method === "OPTIONS") return cors204();

    const p = parseBody(event);
    if (p.__preflight) return cors204();

    const amountOfVehicles = Math.max(0, parsePositiveInt(p.amountOfVehicles));
    const amountOfTime = parsePositiveNumber(p.amountOfTime);
    const generationType = String(p.generationType || "").trim();
    const numberOfBlocks = Math.max(0, parsePositiveInt(p.numberOfBlocks));
    const blocksSize = Math.max(0, parsePositiveInt(p.blocksSize ?? p.blocks_size));
    const gpsArea = String(p.gpsArea || "").trim();
    const canFrames = Array.isArray(p.canFrames) ? p.canFrames : [];
    const dbcFiles = Array.isArray(p.dbcFiles) ? p.dbcFiles : [];
    const vinPrefix = p.vinPrefix;
    const vinSuffix = p.vinSuffix ?? p.vinSufix;
    const initialDateTime = String(p.initialDateTime || "").trim();
    const latencyTime = Math.max(1, parsePositiveInt(p.latencyTime));
    const s3Bucket = String(p.s3Bucket || "").trim();
    const workQueueUrl = resolveWorkQueueUrl(p);

    if (SEND_TO_SQS && !workQueueUrl) {
      return httpResp(400, { requestId, error: "workQueueUrl is required" });
    }

    if (!amountOfVehicles) {
      return httpResp(400, { requestId, error: "amountOfVehicles is required" });
    }

    if (!amountOfTime) {
      return httpResp(400, { requestId, error: "amountOfTime is required" });
    }

    if (!normalizeVinPrefix(vinPrefix) || !normalizeVinSuffix(vinSuffix)) {
      return httpResp(400, { requestId, error: "vinPrefix and vinSuffix are required" });
    }

    if (!initialDateTime) {
      return httpResp(400, { requestId, error: "initialDateTime is required" });
    }

    const epochMs = Date.parse(initialDateTime);
    if (!Number.isFinite(epochMs)) {
      return httpResp(400, { requestId, error: "initialDateTime is invalid" });
    }

    const epochSec = Math.floor(epochMs / 1000);
    const intervalSec = latencyTime;
    const durationSec = Math.max(1, Math.round(amountOfTime * 3600));

    const vehicles = expandVehicles(amountOfVehicles, vinPrefix, vinSuffix);
    if (!vehicles.length) {
      return httpResp(400, { requestId, error: "amountOfVehicles resolved to 0 vehicles" });
    }

    if (vehicles.length > MAX_VEHICLES) {
      return httpResp(400, {
        requestId,
        error: `Vehicle count too large (${vehicles.length}). Max allowed is ${MAX_VEHICLES}.`
      });
    }

    console.log(`[ORCHESTRATOR] requestId=${requestId} vehicles=${vehicles.length} queue=${workQueueUrl || "mock"} send_to_sqs=${SEND_TO_SQS}`);

    const allEntries = vehicles.map((v, idx) => ({
      Id: `v-${idx}`,
      MessageBody: JSON.stringify({
        vin: v.vin,
        type: v.type,
        intervalSec,
        durationSec,
        epochMs,
        epochSec,
        amountOfVehicles,
        amountOfTime,
        generationType,
        numberOfBlocks,
        blocksSize,
        gpsArea,
        canFrames,
        dbcFiles,
        vinPrefix,
        vinSuffix,
        initialDateTime,
        latencyTime,
        s3Bucket,
        runId
      })
    }));

    let sentBatches = 0;
    for (let i = 0; i < allEntries.length; i += SQS_BATCH) {
      const batch = allEntries.slice(i, i + SQS_BATCH);
      let resp = { Failed: [] };

      if (SEND_TO_SQS) {
        const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");
        const sqs = new SQSClient({ region: REGION });

        resp = await sqs.send(new SendMessageBatchCommand({
          QueueUrl: workQueueUrl,
          Entries: batch
        }));
      }

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
      vehicles,
      sentBatches,
      queue_url: workQueueUrl,
      intervalSec,
      durationSec,
      amountOfVehicles,
      amountOfTime,
      generationType,
      numberOfBlocks,
      blocksSize,
      gpsArea,
      canFrames,
      dbcFiles,
      vinPrefix,
      vinSuffix,
      initialDateTime,
      latencyTime,
      s3Bucket,
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
