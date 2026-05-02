// ============================================================================
// Trackster Orchestrator - OEM CAN/FDFD Pipeline
// ============================================================================
// Purpose:
//   Receive frontend payload.
//   Load compiled DBC JSON files from S3.
//   Build a compact runtime using only selected CAN frames.
//   Allow the same CAN ID across different DBC files/messages.
//   Reject only true duplicates: same CAN ID + same message name + same signals.
//   Send one SQS message per vehicle.
// ============================================================================

const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");

const REGION = "us-east-1";
const SQS_BATCH = 10;
const MAX_VEHICLES = 150000;
const MAX_CAN_IDS = 1024;
const MAX_SAFE_SQS_BYTES = 900 * 1024;

const CUSTOMER_ID = process.env.CUSTOMER_ID || "00000000";
const COMPILED_DBC_BUCKET = process.env.COMPILED_DBC_BUCKET || "trackster-customer-dbc";

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
  return String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, VIN_PREFIX_LENGTH);
}

function normalizeVinSuffix(raw) {
  return String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, VIN_SUFFIX_LENGTH);
}

function normalizeUnity(raw) {
  return String(raw || "").trim().toUpperCase() === "MI" ? "Mi" : "Km";
}

function normalizeOutputFormat(raw) {
  const value = String(raw || "").trim().toUpperCase();

  if (value === "JSON") return "JSON";
  if (value === "CSV") return "CSV";

  return "BIN";
}

function normalizeDriverProfile(raw) {
  return String(raw || "").trim();
}

function normalizeStringArray(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function normalizeCanId(raw) {
  const value = String(raw || "").trim().toLowerCase();

  if (!value) {
    return "";
  }

  if (value.startsWith("0x")) {
    return value;
  }

  if (/^[0-9a-f]+$/i.test(value)) {
    return `0x${value.toLowerCase()}`;
  }

  return value;
}

function normalizeGpsCoordinates(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((item) => {
      if (typeof item === "string") {
        const coord = item.trim();
        return coord ? [coord, 1] : null;
      }

      if (Array.isArray(item) && item.length >= 2) {
        const coord = String(item[0] || "").trim();
        const repeat = Number.parseInt(item[1], 10);

        if (!coord || !Number.isFinite(repeat) || repeat < 1) {
          return null;
        }

        return [coord, repeat];
      }

      return null;
    })
    .filter(Boolean);
}

function countGpsCoordinateBlocks(gpsCoordinates) {
  return gpsCoordinates.reduce((total, item) => {
    const repeat = Number.parseInt(item?.[1], 10);
    return total + (Number.isFinite(repeat) && repeat > 0 ? repeat : 0);
  }, 0);
}

function normalizeCanFrames(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((frame) => {
      if (typeof frame === "string") {
        const canId = normalizeCanId(frame);

        return canId
          ? {
              dbcFile: "",
              canId,
              messageName: ""
            }
          : null;
      }

      if (!frame || typeof frame !== "object") {
        return null;
      }

      const canId = normalizeCanId(frame.canId);

      if (!canId) {
        return null;
      }

      return {
        dbcFile: String(frame.dbcFile || "").trim(),
        canId,
        messageName: String(frame.messageName || "").trim()
      };
    })
    .filter(Boolean);
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
  const p3 = (n) => String(n).padStart(3, "0");

  return (
    `${d.getUTCFullYear()}` +
    `${p2(d.getUTCMonth() + 1)}` +
    `${p2(d.getUTCDate())}` +
    `T${p2(d.getUTCHours())}` +
    `${p2(d.getUTCMinutes())}` +
    `${p2(d.getUTCSeconds())}` +
    `${p3(d.getUTCMilliseconds())}`
  );
}

function httpResp(code, obj) {
  return {
    statusCode: code,
    headers: {
      "Content-Type": "application/json",
      ...CORS
    },
    body: JSON.stringify(obj)
  };
}

function cors204() {
  return {
    statusCode: 204,
    headers: { ...CORS },
    body: ""
  };
}

function getHttpMethod(event) {
  return (event?.httpMethod || event?.requestContext?.http?.method || "").toUpperCase();
}

function parseBody(event) {
  const method = getHttpMethod(event);

  if (method === "OPTIONS") {
    return { __preflight: true };
  }

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

  for (let i = 0; i < total; i++) {
    const seq = i + 1;

    if (seq > MAX_VIN_SEQ) {
      throw new Error(
        `VIN sequence overflow: requested more than ${MAX_VIN_SEQ} vehicles but VIN format supports only ${VIN_SEQ_LENGTH} digits`
      );
    }

    list.push({
      vin: makeVin(vinPrefix, vinSuffix, seq),
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

function resolveS3Bucket(payload) {
  return String(
    payload?.s3Bucket ||
    payload?.bucket ||
    payload?.s3_bucket ||
    ""
  ).trim();
}

function makeCompiledDbcKey(dbcFile) {
  const cleanName = String(dbcFile || "").trim().split("/").pop();

  if (!cleanName) {
    return "";
  }

  const baseName = cleanName.replace(/\.[^.]+$/, "");

  return `dbc-files/${CUSTOMER_ID}/${baseName}.json`;
}

async function streamToString(stream) {
  if (!stream) {
    return "";
  }

  if (typeof stream.transformToString === "function") {
    return await stream.transformToString();
  }

  return await new Promise((resolve, reject) => {
    const chunks = [];

    stream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
}

async function readCompiledDbc(s3, dbcFile) {
  const key = makeCompiledDbcKey(dbcFile);

  if (!key) {
    throw new Error(`Invalid DBC file name: ${dbcFile}`);
  }

  const response = await s3.send(
    new GetObjectCommand({
      Bucket: COMPILED_DBC_BUCKET,
      Key: key
    })
  );

  const text = await streamToString(response.Body);

  if (!text) {
    throw new Error(`Compiled DBC is empty: s3://${COMPILED_DBC_BUCKET}/${key}`);
  }

  const parsed = JSON.parse(text);

  if (!parsed || typeof parsed !== "object") {
    throw new Error(`Invalid compiled DBC JSON: s3://${COMPILED_DBC_BUCKET}/${key}`);
  }

  if (parsed.st && parsed.st !== "validated") {
    throw new Error(`Compiled DBC is not validated: ${dbcFile}`);
  }

  if (!parsed.m || typeof parsed.m !== "object") {
    throw new Error(`Compiled DBC does not contain message map: ${dbcFile}`);
  }

  return {
    dbcFile,
    key,
    compiled: parsed
  };
}

function buildSelectedCanFrameMap(canFrames) {
  const map = new Map();

  for (const frame of canFrames) {
    const dbcFile = String(frame.dbcFile || "").trim();
    const canId = normalizeCanId(frame.canId);
    const messageName = String(frame.messageName || "").trim();

    if (!dbcFile || !canId) {
      continue;
    }

    if (!map.has(dbcFile)) {
      map.set(dbcFile, new Map());
    }

    map.get(dbcFile).set(canId, {
      dbcFile,
      canId,
      messageName
    });
  }

  return map;
}

function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }

  const keys = Object.keys(value).sort();

  return `{${keys
    .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
    .join(",")}}`;
}

function buildSignalSignature(frame) {
  if (!frame || typeof frame !== "object") {
    return stableStringify(frame);
  }

  if (Array.isArray(frame.s)) {
    return stableStringify(frame.s);
  }

  if (Array.isArray(frame.signals)) {
    return stableStringify(frame.signals);
  }

  return stableStringify(frame);
}

function buildDuplicateSignature(canId, messageName, frame) {
  const normalizedMessageName = String(messageName || "").trim().toLowerCase();
  const signalSignature = buildSignalSignature(frame);

  return `${normalizeCanId(canId)}::${normalizedMessageName}::${signalSignature}`;
}

async function buildRuntimeCompiledDbc(s3, dbcFiles, canFrames) {
  const selectedByDbc = buildSelectedCanFrameMap(canFrames);

  const selectedFrameKeys = new Set(
    canFrames
      .map((frame) => {
        const dbcFile = String(frame.dbcFile || "").trim();
        const canId = normalizeCanId(frame.canId);

        return dbcFile && canId ? `${dbcFile}::${canId}` : "";
      })
      .filter(Boolean)
  );

  if (selectedFrameKeys.size > MAX_CAN_IDS) {
    throw new Error(`Too many CAN frames selected (${selectedFrameKeys.size}). Max allowed is ${MAX_CAN_IDS}.`);
  }

  const runtime = {
    v: 1,
    f: ["sb", "bl", "bo", "sg", "f", "o", "min", "max"],
    m: {}
  };

  const duplicateGuard = new Set();
  const sources = [];
  const missing = [];

  for (const dbcFile of dbcFiles) {
    const selectedForThisDbc = selectedByDbc.get(dbcFile) || new Map();

    if (!selectedForThisDbc.size) {
      continue;
    }

    const loaded = await readCompiledDbc(s3, dbcFile);
    const compiled = loaded.compiled;

    if (Array.isArray(compiled.f) && compiled.f.length > 0) {
      runtime.f = compiled.f;
    }

    sources.push({
      dbcFile,
      compiledKey: loaded.key,
      selectedCanIds: Array.from(selectedForThisDbc.keys())
    });

    for (const [canId, selectedFrame] of selectedForThisDbc.entries()) {
      const frame = compiled.m[canId];

      if (!frame) {
        missing.push({
          dbcFile,
          canId,
          messageName: selectedFrame.messageName
        });
        continue;
      }

      const duplicateSignature = buildDuplicateSignature(
        canId,
        selectedFrame.messageName,
        frame
      );

      if (duplicateGuard.has(duplicateSignature)) {
        throw new Error(
          `Duplicate CAN message found: ${canId} ${selectedFrame.messageName}. Same CAN ID, same message name and same signal layout.`
        );
      }

      duplicateGuard.add(duplicateSignature);

      if (!Array.isArray(runtime.m[canId])) {
        runtime.m[canId] = [];
      }

      runtime.m[canId].push({
        dbcFile,
        canId,
        messageName: selectedFrame.messageName,
        frame
      });
    }
  }

  const resolvedCanFrames = Object.values(runtime.m).flatMap((entries) => entries);
  const resolvedCanIds = Object.keys(runtime.m);

  if (!resolvedCanFrames.length) {
    throw new Error("No selected CAN IDs were found in the compiled DBC files.");
  }

  return {
    compiledDbc: runtime,
    compiledSources: sources,
    resolvedCanIds,
    resolvedCanFrames,
    missingCanIds: missing
  };
}

function assertSafeSqsMessageSize(messageBody) {
  const sizeBytes = Buffer.byteLength(messageBody, "utf8");

  if (sizeBytes > MAX_SAFE_SQS_BYTES) {
    throw new Error(
      `SQS message body too large (${sizeBytes} bytes). Safe limit is ${MAX_SAFE_SQS_BYTES} bytes.`
    );
  }

  return sizeBytes;
}

module.exports.handler = async (event, context) => {
  const requestId = context?.awsRequestId;
  const runId = makeRunIdUTC();

  try {
    const method = getHttpMethod(event);

    if (method === "OPTIONS") {
      return cors204();
    }

    const p = parseBody(event);

    if (p.__preflight) {
      return cors204();
    }

    const amountOfVehicles = Math.max(0, parsePositiveInt(p.amountOfVehicles));
    const amountOfTime = parsePositiveNumber(p.amountOfTime);
    const generationType = String(p.generationType || "").trim();
    const requestedNumberOfBlocks = Math.max(0, parsePositiveInt(p.numberOfBlocks));
    const blocksSize = Math.max(0, parsePositiveInt(p.blocksSize ?? p.blocks_size));
    const gpsCoordinates = normalizeGpsCoordinates(p.gpsCoordinates);
    const gpsBlockCount = countGpsCoordinateBlocks(gpsCoordinates);
    const numberOfBlocks = requestedNumberOfBlocks > 0 ? requestedNumberOfBlocks : gpsBlockCount;
    const canFrames = normalizeCanFrames(p.canFrames);
    const dbcFiles = normalizeStringArray(p.dbcFiles);
    const vinPrefix = p.vinPrefix;
    const vinSuffix = p.vinSuffix ?? p.vinSufix;
    const initialDateTime = String(p.initialDateTime || "").trim();
    const latencyTime = Math.max(1, parsePositiveInt(p.latencyTime));
    const speed = parsePositiveNumber(p.speed);
    const unity = normalizeUnity(p.unity);
    const driverProfile = normalizeDriverProfile(p.driverProfile);
    const outputFormat = normalizeOutputFormat(p.outputFormat);
    const s3Bucket = resolveS3Bucket(p);
    const workQueueUrl = resolveWorkQueueUrl(p);

    if (!workQueueUrl) {
      return httpResp(400, { requestId, error: "workQueueUrl is required" });
    }

    if (!s3Bucket) {
      return httpResp(400, { requestId, error: "s3Bucket is required" });
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

    if (!speed) {
      return httpResp(400, { requestId, error: "speed is required" });
    }

    if (!gpsCoordinates.length) {
      return httpResp(400, { requestId, error: "gpsCoordinates is required" });
    }

    if (!numberOfBlocks) {
      return httpResp(400, { requestId, error: "numberOfBlocks could not be resolved from gpsCoordinates" });
    }

    if (!blocksSize) {
      return httpResp(400, { requestId, error: "blocksSize is required" });
    }

    if (!dbcFiles.length) {
      return httpResp(400, { requestId, error: "dbcFiles is required" });
    }

    if (!canFrames.length) {
      return httpResp(400, { requestId, error: "canFrames is required" });
    }

    const framesWithoutDbc = canFrames.filter((frame) => !frame.dbcFile);

    if (framesWithoutDbc.length) {
      return httpResp(400, {
        requestId,
        error: "Every selected CAN frame must include dbcFile",
        invalidFrames: framesWithoutDbc
      });
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

    const s3 = new S3Client({ region: REGION });

    const {
      compiledDbc,
      compiledSources,
      resolvedCanIds,
      resolvedCanFrames,
      missingCanIds
    } = await buildRuntimeCompiledDbc(s3, dbcFiles, canFrames);

    const baseMessage = {
      runId,

      intervalSec,
      durationSec,
      epochMs,
      epochSec,

      numberOfBlocks,
      blocksSize,

      gpsCoordinates,
      canFrames,

      speed,
      unity,
      driverProfile,
      outputFormat,

      s3Bucket,

      compiledDbc
    };

    const probeBody = JSON.stringify({
      ...baseMessage,
      vin: makeVin(vinPrefix, vinSuffix, 1),
      type: "car",
      vehicleIndex: 0
    });

    const messageSizeBytes = assertSafeSqsMessageSize(probeBody);

    console.log(
      `[ORCHESTRATOR] requestId=${requestId} runId=${runId} vehicles=${vehicles.length} canIds=${resolvedCanIds.length} canFrames=${resolvedCanFrames.length} messageSizeBytes=${messageSizeBytes} queue=${workQueueUrl}`
    );

    const sqs = new SQSClient({ region: REGION });

    const allEntries = vehicles.map((v, idx) => ({
      Id: `v-${idx}`,
      MessageBody: JSON.stringify({
        ...baseMessage,
        vin: v.vin,
        type: v.type,
        vehicleIndex: idx
      })
    }));

    let sentBatches = 0;

    for (let i = 0; i < allEntries.length; i += SQS_BATCH) {
      const batch = allEntries.slice(i, i + SQS_BATCH);

      const resp = await sqs.send(
        new SendMessageBatchCommand({
          QueueUrl: workQueueUrl,
          Entries: batch
        })
      );

      // const resp = {
      //   Successful: batch.map((entry) => ({
      //     Id: entry.Id
      //   })),
      //   Failed: []
      // };

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
      runId,

      enqueued_vehicles: vehicles.length,
      sentBatches,
      queue_url: workQueueUrl,

      intervalSec,
      durationSec,
      epochMs,
      epochSec,

      amountOfVehicles,
      amountOfTime,
      generationType,
      requestedNumberOfBlocks,
      numberOfBlocks,
      blocksSize,

      gpsCoordinateRuns: gpsCoordinates.length,
      gpsBlockCount,
      selectedCanFrames: canFrames.length,
      resolvedCanIds: resolvedCanIds.length,
      resolvedCanFrames: resolvedCanFrames.length,
      missingCanIds,

      compiledSources,
      messageSizeBytes,

      speed,
      unity,
      driverProfile,
      outputFormat,
      s3Bucket
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