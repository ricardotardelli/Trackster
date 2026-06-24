// ============================================================================
// Trackster Orchestrator - OEM CAN/FDFD Pipeline
// ============================================================================
// Purpose:
//   Receive frontend payload.
//   Load compiled DBC JSON files from S3.
//   Build a compact runtime using only selected CAN frames.
//   Allow the same CAN ID across different DBC files/messages.
//   Reject only true duplicates: same CAN ID + same message name + same signals.
//   Preserve CAN ID format from compiled DBC:
//     idf = "standard" | "extended"
//   Store run-manifest.json in:
//     <clientId>/<timestamp>/run-manifest.json
//   Send one SQS message per vehicle.
// ============================================================================

const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");
const { S3Client, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");

const REGION = "us-east-1";
const SQS_BATCH = 10;
const MAX_VEHICLES = 150000;
const MAX_CAN_IDS = 1024;
const MAX_SAFE_SQS_BYTES = 900 * 1024;

const DEFAULT_CLIENT_ID = process.env.CUSTOMER_ID || process.env.CLIENT_ID || "00000000";
const COMPILED_DBC_BUCKET = process.env.COMPILED_DBC_BUCKET || "trackster-customer-dbc";
const AI_ASSIST_LAMBDA_NAME = process.env.AI_ASSIST_LAMBDA_NAME || "trackster-simulator-ai-assist";

const RUNTIME_DBC_VERSION = 2;

const RUNTIME_SIGNAL_FIELDS = [
  "sb",
  "bl",
  "bo",
  "sg",
  "f",
  "o",
  "min",
  "max",
  "n",
  "mx",
  "mv"
];

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With"
};

const MAX_VIN_SEQ = 999999;
const VIN_SEQ_LENGTH = String(MAX_VIN_SEQ).length;
const VIN_PREFIX_LENGTH = 6;
const VIN_SUFFIX_LENGTH = 17 - VIN_PREFIX_LENGTH - VIN_SEQ_LENGTH;

function normalizeClientId(raw) {
  return String(raw || "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "");
}

function resolveClientId(payload) {
  return normalizeClientId(
    payload?.clientId ||
    payload?.customerId ||
    payload?.customerID ||
    DEFAULT_CLIENT_ID
  );
}

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

function normalizeCanIdFormat(raw) {
  const value = String(raw || "").trim().toLowerCase();

  if (value === "extended") {
    return "extended";
  }

  return "standard";
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

  return (
    `${d.getUTCFullYear()}` +
    `${p2(d.getUTCMonth() + 1)}` +
    `${p2(d.getUTCDate())}` +
    `${p2(d.getUTCHours())}` +
    `${p2(d.getUTCMinutes())}` +
    `${p2(d.getUTCSeconds())}`
  );
}

function makeRunManifestKey(clientId, runId) {
  return `${clientId}/${runId}/run-manifest.json`;
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

function makeCompiledDbcKey(clientId, dbcFile) {
  const cleanName = String(dbcFile || "").trim().split("/").pop();

  if (!cleanName) {
    return "";
  }

  const baseName = cleanName.replace(/\.[^.]+$/, "");

  return `dbc-files/${clientId}/${baseName}.json`;
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

async function readCompiledDbc(s3, clientId, dbcFile) {
  const key = makeCompiledDbcKey(clientId, dbcFile);

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

async function writeRunManifest(s3, bucket, key, manifest) {
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: JSON.stringify(manifest, null, 2),
      ContentType: "application/json"
    })
  );
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
  const idf = normalizeCanIdFormat(frame?.idf);
  const signalSignature = buildSignalSignature(frame);

  return `${normalizeCanId(canId)}::${idf}::${normalizedMessageName}::${signalSignature}`;
}

function getCompiledFieldIndex(compiledFields, fieldName) {
  if (!Array.isArray(compiledFields)) {
    return -1;
  }

  return compiledFields.indexOf(fieldName);
}

function readCompiledSignalField(signal, compiledFields, fieldName, fallbackIndex, fallbackValue = null) {
  if (Array.isArray(signal)) {
    const index = getCompiledFieldIndex(compiledFields, fieldName);

    if (index >= 0 && index < signal.length) {
      return signal[index];
    }

    if (fallbackIndex >= 0 && fallbackIndex < signal.length) {
      return signal[fallbackIndex];
    }

    return fallbackValue;
  }

  if (signal && typeof signal === "object") {
    if (Object.prototype.hasOwnProperty.call(signal, fieldName)) {
      return signal[fieldName];
    }

    return fallbackValue;
  }

  return fallbackValue;
}

function normalizeCompiledSignalForRuntime(signal, compiledFields) {
  const sb = readCompiledSignalField(signal, compiledFields, "sb", 0);
  const bl = readCompiledSignalField(signal, compiledFields, "bl", 1);
  const bo = readCompiledSignalField(signal, compiledFields, "bo", 2);
  const sg = readCompiledSignalField(signal, compiledFields, "sg", 3);
  const factor = readCompiledSignalField(signal, compiledFields, "f", 4, 1);
  const offset = readCompiledSignalField(signal, compiledFields, "o", 5, 0);
  const min = readCompiledSignalField(signal, compiledFields, "min", 6, 0);
  const max = readCompiledSignalField(signal, compiledFields, "max", 7, 0);

  const name =
    readCompiledSignalField(signal, compiledFields, "n", 8, "") ||
    readCompiledSignalField(signal, compiledFields, "name", -1, "");

  const mx = readCompiledSignalField(signal, compiledFields, "mx", 9, null);
  const mv = readCompiledSignalField(signal, compiledFields, "mv", 10, null);

  return [
    sb,
    bl,
    bo,
    sg,
    factor,
    offset,
    min,
    max,
    name ? String(name) : "",
    mx === undefined ? null : mx,
    mv === undefined ? null : mv
  ];
}

function normalizeCompiledFrameForRuntime(frame, compiledFields) {
  if (!frame || typeof frame !== "object") {
    return frame;
  }

  const rawSignals = Array.isArray(frame.s)
    ? frame.s
    : Array.isArray(frame.signals)
      ? frame.signals
      : [];

  return {
    ...frame,
    idf: normalizeCanIdFormat(frame.idf),
    s: rawSignals.map((signal) => normalizeCompiledSignalForRuntime(signal, compiledFields))
  };
}

function parseCanIdNumber(canId) {
  const normalized = normalizeCanId(canId);

  if (!/^0x[0-9a-f]+$/i.test(normalized)) {
    return NaN;
  }

  return Number.parseInt(normalized.slice(2), 16);
}

function validateRuntimeCanId(canId, idf, dbcFile, messageName) {
  const numericCanId = parseCanIdNumber(canId);

  if (!Number.isFinite(numericCanId)) {
    throw new Error(
      `Invalid CAN ID "${canId}" in ${dbcFile} ${messageName || ""}`.trim()
    );
  }

  if (idf === "standard" && (numericCanId < 0 || numericCanId > 0x7ff)) {
    throw new Error(
      `CAN ID ${canId} in ${dbcFile} ${messageName || ""} is marked as standard but exceeds 11-bit range.`.trim()
    );
  }

  if (idf === "extended" && (numericCanId < 0 || numericCanId > 0x1fffffff)) {
    throw new Error(
      `CAN ID ${canId} in ${dbcFile} ${messageName || ""} is marked as extended but exceeds 29-bit range.`.trim()
    );
  }
}

async function buildRuntimeCompiledDbc(s3, clientId, dbcFiles, canFrames) {
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
    v: RUNTIME_DBC_VERSION,
    f: RUNTIME_SIGNAL_FIELDS,
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

    const loaded = await readCompiledDbc(s3, clientId, dbcFile);
    const compiled = loaded.compiled;

    sources.push({
      dbcFile,
      compiledKey: loaded.key,
      selectedCanIds: Array.from(selectedForThisDbc.keys())
    });

    for (const [canId, selectedFrame] of selectedForThisDbc.entries()) {
      const originalFrame = compiled.m[canId];

      if (!originalFrame) {
        missing.push({
          dbcFile,
          canId,
          messageName: selectedFrame.messageName
        });
        continue;
      }

      const frame = normalizeCompiledFrameForRuntime(
        originalFrame,
        compiled.f
      );

      const idf = normalizeCanIdFormat(frame.idf);

      validateRuntimeCanId(
        canId,
        idf,
        dbcFile,
        selectedFrame.messageName || frame.n || frame.name || ""
      );

      const duplicateSignature = buildDuplicateSignature(
        canId,
        selectedFrame.messageName,
        frame
      );

      if (duplicateGuard.has(duplicateSignature)) {
        throw new Error(
          `Duplicate CAN message found: ${canId} ${selectedFrame.messageName}. Same CAN ID, same ID format, same message name and same signal layout.`
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
        idf,
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


function buildAiScenarioName(payload, runId) {
  const explicitName = String(payload?.aiScenarioName || payload?.scenarioName || "").trim();

  if (explicitName) {
    return explicitName;
  }

  return `Trackster Simulator Scenario ${runId}`;
}

function buildAiRequestedContext(payload, durationSec, speed, unity, driverProfile) {
  const explicitContext = String(
    payload?.aiRequestedContext ||
    payload?.requestedContext ||
    payload?.scenarioContext ||
    payload?.scenarioDescription ||
    ""
  ).trim();

  if (explicitContext) {
    return explicitContext;
  }

  return [
    `Generate a realistic simulator behavior plan for a ${durationSec} second drive.`,
    `Driver profile: ${driverProfile || "Balanced"}.`,
    `Reference speed from simulator request: ${speed} ${unity}/h.`,
    "The vehicle should behave like a realistic urban driving session with parking lot departure, city traffic, traffic lights, congestion, steady urban cruising, and final parking."
  ].join(" ");
}

function parseInvokedLambdaPayload(payload) {
  if (!payload) {
    return null;
  }

  const text = Buffer.from(payload).toString("utf8");

  if (!text) {
    return null;
  }

  return JSON.parse(text);
}

function parseInvokedLambdaBody(lambdaResponse) {
  if (!lambdaResponse || typeof lambdaResponse !== "object") {
    throw new Error("AI assist Lambda returned an empty response.");
  }

  if (typeof lambdaResponse.body === "string") {
    return JSON.parse(lambdaResponse.body);
  }

  if (lambdaResponse.body && typeof lambdaResponse.body === "object") {
    return lambdaResponse.body;
  }

  return lambdaResponse;
}

function validateAiBehaviorPlan(aiResult, expectedDurationSec) {
  const scenario = aiResult?.scenario || aiResult?.aiBehaviorPlan || aiResult;

  if (!scenario || typeof scenario !== "object") {
    throw new Error("AI assist response does not contain a scenario object.");
  }

  if (Number(scenario.durationSeconds) !== Number(expectedDurationSec)) {
    throw new Error(
      `AI behavior plan duration mismatch. Expected ${expectedDurationSec}, received ${scenario.durationSeconds}.`
    );
  }

  if (!Array.isArray(scenario.behaviorPlan) || !scenario.behaviorPlan.length) {
    throw new Error("AI behavior plan is empty or invalid.");
  }

  let expectedStart = 0;

  for (let index = 0; index < scenario.behaviorPlan.length; index++) {
    const phase = scenario.behaviorPlan[index];

    if (!phase || typeof phase !== "object") {
      throw new Error(`AI behavior phase ${index} is invalid.`);
    }

    if (Number(phase.startTimeSeconds) !== expectedStart) {
      throw new Error(
        `AI behavior phase ${index} has invalid startTimeSeconds. Expected ${expectedStart}, received ${phase.startTimeSeconds}.`
      );
    }

    const duration = Number(phase.durationSeconds);

    if (!Number.isFinite(duration) || duration <= 0) {
      throw new Error(`AI behavior phase ${index} has invalid durationSeconds.`);
    }

    expectedStart += duration;
  }

  if (expectedStart !== Number(expectedDurationSec)) {
    throw new Error(
      `AI behavior plan does not end at expected duration. Expected ${expectedDurationSec}, received ${expectedStart}.`
    );
  }

  return scenario;
}

async function invokeAiAssist(lambda, payload, durationSec, speed, unity, driverProfile, runId) {
  const aiRequestBody = {
    scenarioName: buildAiScenarioName(payload, runId),
    durationSeconds: durationSec,
    requestedContext: buildAiRequestedContext(payload, durationSec, speed, unity, driverProfile)
  };

  const aiEvent = {
    requestContext: {
      http: {
        method: "POST"
      }
    },
    httpMethod: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(aiRequestBody)
  };

  const response = await lambda.send(
    new InvokeCommand({
      FunctionName: AI_ASSIST_LAMBDA_NAME,
      InvocationType: "RequestResponse",
      Payload: Buffer.from(JSON.stringify(aiEvent), "utf8")
    })
  );

  if (response.FunctionError) {
    throw new Error(`AI assist Lambda failed with FunctionError=${response.FunctionError}`);
  }

  const lambdaResponse = parseInvokedLambdaPayload(response.Payload);
  const statusCode = Number(lambdaResponse?.statusCode || 200);
  const parsedBody = parseInvokedLambdaBody(lambdaResponse);

  if (statusCode < 200 || statusCode >= 300) {
    throw new Error(`AI assist Lambda returned HTTP ${statusCode}: ${JSON.stringify(parsedBody)}`);
  }

  if (parsedBody?.success === false) {
    throw new Error(`AI assist Lambda returned success=false: ${JSON.stringify(parsedBody)}`);
  }

  return {
    source: "trackster-simulator-ai-assist",
    lambdaName: AI_ASSIST_LAMBDA_NAME,
    modelId: parsedBody?.modelId || null,
    requestedAt: new Date().toISOString(),
    request: aiRequestBody,
    scenario: validateAiBehaviorPlan(parsedBody, durationSec)
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

    const clientId = resolveClientId(p);
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

    if (!clientId) {
      return httpResp(400, { requestId, error: "clientId is required" });
    }

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

    const lambda = new LambdaClient({ region: REGION });

    const aiBehaviorPlan = await invokeAiAssist(
      lambda,
      p,
      durationSec,
      speed,
      unity,
      driverProfile,
      runId
    );

    const s3 = new S3Client({ region: REGION });

    const {
      compiledDbc,
      compiledSources,
      resolvedCanIds,
      resolvedCanFrames,
      missingCanIds
    } = await buildRuntimeCompiledDbc(s3, clientId, dbcFiles, canFrames);

    const baseMessage = {
      runId,
      customerId: clientId,
      clientId,

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

      compiledDbc,
      aiBehaviorPlan
    };

    const probeBody = JSON.stringify({
      ...baseMessage,
      vin: makeVin(vinPrefix, vinSuffix, 1),
      type: "car",
      vehicleIndex: 0
    });

    const messageSizeBytes = assertSafeSqsMessageSize(probeBody);

    const sqsPayloadPreview = JSON.parse(probeBody);

    const runManifestKey = makeRunManifestKey(clientId, runId);

    const runManifest = {
      manifestVersion: 1,
      createdAt: new Date().toISOString(),

      requestId,
      runId,
      timestamp: runId,
      customerId: clientId,
      clientId,

      output: {
        bucket: s3Bucket,
        runFolder: `${clientId}/${runId}`,
        manifestKey: runManifestKey,
        outputFormat
      },

      simulation: {
        amountOfVehicles,
        amountOfTime,
        generationType,
        requestedNumberOfBlocks,
        numberOfBlocks,
        blocksSize,
        intervalSec,
        durationSec,
        epochMs,
        epochSec,
        initialDateTime,
        speed,
        unity,
        driverProfile
      },

      gps: {
        gpsCoordinates,
        gpsCoordinateRuns: gpsCoordinates.length,
        gpsBlockCount
      },

      ai: {
        aiBehaviorPlan
      },

      dbc: {
        dbcFiles,
        canFrames,
        selectedCanFrames: canFrames.length,
        resolvedCanIds,
        resolvedCanFrames,
        resolvedCanIdCount: resolvedCanIds.length,
        resolvedCanFrameCount: resolvedCanFrames.length,
        missingCanIds,
        compiledSources,
        compiledDbc
      },

      vehicles,

      sqs: {
        queueUrl: workQueueUrl,
        messageSizeBytes
      }
    };

    await writeRunManifest(s3, s3Bucket, runManifestKey, runManifest);

    console.log(
      `[ORCHESTRATOR] run manifest written: s3://${s3Bucket}/${runManifestKey}`
    );

    console.log(
      `[ORCHESTRATOR] requestId=${requestId} runId=${runId} clientId=${clientId} vehicles=${vehicles.length} canIds=${resolvedCanIds.length} canFrames=${resolvedCanFrames.length} messageSizeBytes=${messageSizeBytes} queue=${workQueueUrl}`
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
      timestamp: runId,
      customerId: clientId,
      clientId,

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
      s3Bucket,

      runManifestKey,
      runFolder: `${clientId}/${runId}`,

      aiBehaviorPlan,
      sqsPayloadPreview
    });
  }
  catch (err) {
    console.error("[ORCHESTRATOR] Unhandled error:", err);

    return httpResp(500, {
      requestId,
      error: "Unhandled exception in orchestrator",
      details: sanitizeError(err)
    });
  }
};