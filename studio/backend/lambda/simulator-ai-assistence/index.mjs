import { Sha256 } from '@aws-crypto/sha256-js';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand
} from '@aws-sdk/client-s3';
import { HttpRequest } from '@smithy/protocol-http';
import { SignatureV4 } from '@smithy/signature-v4';
import { SQSClient, SendMessageBatchCommand } from '@aws-sdk/client-sqs';

const defaultHeaders = {
  'Access-Control-Allow-Origin': process.env.ALLOWED_ORIGIN || '*',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS,POST',
  'Content-Type': 'application/json'
};

const region = process.env.BEDROCK_REGION || process.env.AWS_REGION || 'us-east-1';
const modelId = process.env.BEDROCK_MODEL_ID || 'google.gemma-4-31b';
const mantleHost = `bedrock-mantle.${region}.api.aws`;
const mantlePath = '/openai/v1/chat/completions';

const s3Client = new S3Client({ region });

const DEFAULT_PHASE_COUNT = Number.parseInt(process.env.AI_PHASE_COUNT || '6', 10);
const DEFAULT_MAX_TOKENS = Number.parseInt(process.env.BEDROCK_MAX_TOKENS || '8192', 10);
const DEFAULT_SQS_BATCH = Number.parseInt(process.env.SQS_BATCH || '10', 10);
const DEFAULT_MAX_SAFE_SQS_BYTES = Number.parseInt(process.env.MAX_SAFE_SQS_BYTES || String(900 * 1024), 10);

export const handler = async (event) => {
  if (event.requestContext?.http?.method === 'OPTIONS' || event.httpMethod === 'OPTIONS') {
    return buildResponse(200, { success: true });
  }

  if (event.requestContext?.http?.method !== 'POST' && event.httpMethod !== 'POST') {
    return buildResponse(405, {
      success: false,
      message: 'Method not allowed.'
    });
  }

  try {
    const body = parseBody(event.body);

    if (isDelegatedGenerationRequest(body)) {
      const result = await handleDelegatedGeneration(body);
      return buildResponse(202, result);
    }

    const scenarioResult = await generateValidatedSignalDeltaScenario(body);

    return buildResponse(200, {
      success: true,
      modelId,
      schemaVersion: scenarioResult.scenarioRequest.schemaVersion,
      controlledSignals: scenarioResult.scenarioRequest.controlledSignals,
      scenario: scenarioResult.scenario
    });
  } catch (error) {
    console.error('Simulator AI assistance failed:', {
      name: error?.name,
      message: error?.message,
      stack: error?.stack,
      validationErrors: error?.validationErrors || null
    });

    return buildResponse(500, {
      success: false,
      message: 'Simulator AI assistance failed.',
      error: error?.message || 'Unknown error'
    });
  }
};

function isDelegatedGenerationRequest(body) {
  return (
    body?.action === 'generate_ai_behavior_and_enqueue_worker_messages' ||
    Boolean(body?.generation?.baseMessage && Array.isArray(body?.generation?.vehicles))
  );
}

async function handleDelegatedGeneration(body) {
  const generation = body?.generation || {};
  const baseMessage = generation.baseMessage;
  const vehicles = Array.isArray(generation.vehicles) ? generation.vehicles : [];
  const workQueueUrl = String(generation.workQueueUrl || '').trim();
  const sqsBatch = toPositiveInteger(generation.sqsBatch, DEFAULT_SQS_BATCH);
  const maxSafeSqsBytes = toPositiveInteger(generation.maxSafeSqsBytes, DEFAULT_MAX_SAFE_SQS_BYTES);

  if (!baseMessage || typeof baseMessage !== 'object') {
    throw new Error('Delegated generation payload is missing generation.baseMessage.');
  }

  if (!vehicles.length) {
    throw new Error('Delegated generation payload is missing generation.vehicles.');
  }

  if (!workQueueUrl) {
    throw new Error('Delegated generation payload is missing generation.workQueueUrl.');
  }

  const aiRequestBody = buildDelegatedScenarioRequestBody(body, baseMessage, vehicles);
  const scenarioResult = await generateValidatedSignalDeltaScenario(aiRequestBody);

  const scenarioS3 = await saveScenarioJson({
    body,
    generation,
    baseMessage,
    scenarioResult
  });

  const aiBehaviorPlan = {
    source: 'trackster-simulator-ai-assist',
    lambdaName: process.env.AWS_LAMBDA_FUNCTION_NAME || 'trackster-simulator-ai-assist',
    modelId,
    requestedAt: new Date().toISOString(),
    request: scenarioResult.scenarioRequest,
    scenario: scenarioResult.scenario,
    scenarioS3
  };

  const enqueueResult = await enqueueWorkerMessages({
    baseMessage,
    vehicles,
    workQueueUrl,
    sqsBatch,
    maxSafeSqsBytes,
    aiBehaviorPlan
  });

  console.log('Simulator AI assistance completed delegated generation:', {
    requestId: body?.requestId || null,
    runId: body?.runId || baseMessage?.runId || null,
    clientId: body?.clientId || baseMessage?.clientId || null,
    vehicles: vehicles.length,
    sentBatches: enqueueResult.sentBatches,
    messageSizeBytes: enqueueResult.messageSizeBytes,
    scenarioBucket: scenarioS3.bucket,
    scenarioKey: scenarioS3.key,
    promptBucket: scenarioResult.promptSource.bucket,
    promptKey: scenarioResult.promptSource.key
  });

  return {
    success: true,
    status: 'enqueued',
    requestId: body?.requestId || null,
    runId: body?.runId || baseMessage?.runId || null,
    clientId: body?.clientId || baseMessage?.clientId || null,
    modelId,
    controlledSignals: scenarioResult.scenarioRequest.controlledSignals,
    enqueuedVehicles: vehicles.length,
    sentBatches: enqueueResult.sentBatches,
    messageSizeBytes: enqueueResult.messageSizeBytes,
    scenarioS3
  };
}

function buildDelegatedScenarioRequestBody(body, baseMessage, vehicles) {
  const aiRequest = body?.aiRequest || {};
  const availableSignals = normalizeStringArray(aiRequest.availableSignals).length
    ? normalizeStringArray(aiRequest.availableSignals)
    : extractAvailableSignalNamesFromCompiledDbc(baseMessage.compiledDbc);

  return {
    ...aiRequest,
    durationSeconds: toPositiveInteger(aiRequest.durationSeconds, baseMessage.durationSec || 1200),
    sampleIntervalSeconds: toPositiveInteger(aiRequest.sampleIntervalSeconds, baseMessage.intervalSec || 5),
    requestedContext: sanitizeText(aiRequest.requestedContext) || 'realistic urban driving signal delta scenario',
    driverProfile: sanitizeText(aiRequest.driverProfile) || sanitizeText(baseMessage.driverProfile) || 'Balanced',
    targetSpeed: Number.isFinite(Number(aiRequest.targetSpeed)) ? Number(aiRequest.targetSpeed) : Number(baseMessage.speed),
    distanceUnit: sanitizeText(aiRequest.distanceUnit) || sanitizeText(baseMessage.unity) || 'Km',
    simulationMode: sanitizeText(aiRequest.simulationMode) || '',
    generationType: sanitizeText(aiRequest.generationType) || '',
    routeRegion: sanitizeText(aiRequest.routeRegion) || '',
    initialDateTime: sanitizeText(aiRequest.initialDateTime) || '',
    amountOfVehicles: vehicles.length,
    selectedCanFrames: Array.isArray(baseMessage.canFrames) ? baseMessage.canFrames.length : 0,
    availableSignals
  };
}

async function saveScenarioJson({ body, generation, baseMessage, scenarioResult }) {
  const bucket = resolveScenarioBucket(generation, baseMessage);
  const key = resolveScenarioKey(body, generation, baseMessage);

  if (!bucket) {
    throw new Error('Unable to save scenario.json because no S3 bucket was resolved.');
  }

  if (!key) {
    throw new Error('Unable to save scenario.json because no S3 key or output folder was resolved.');
  }

  const scenarioDocument = {
    schemaVersion: 'trackster-ai-scenario-file-v1',
    generatedAt: new Date().toISOString(),
    source: 'trackster-simulator-ai-assist',
    lambdaName: process.env.AWS_LAMBDA_FUNCTION_NAME || 'trackster-simulator-ai-assist',
    modelId,
    prompt: scenarioResult.promptSource,
    requestId: body?.requestId || null,
    runId: body?.runId || baseMessage?.runId || null,
    clientId: body?.clientId || baseMessage?.clientId || null,
    request: scenarioResult.scenarioRequest,
    scenario: scenarioResult.scenario
  };

  await s3Client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: JSON.stringify(scenarioDocument, null, 2),
      ContentType: 'application/json'
    })
  );

  return {
    bucket,
    key,
    fileName: 'scenario.json'
  };
}

function resolveScenarioBucket(generation, baseMessage) {
  return sanitizeText(
    generation?.scenarioBucketName ||
      generation?.bucketName ||
      generation?.outputBucketName ||
      generation?.rawBucketName ||
      baseMessage?.scenarioBucketName ||
      baseMessage?.bucketName ||
      baseMessage?.s3Bucket ||
      baseMessage?.outputBucketName ||
      baseMessage?.rawBucketName ||
      baseMessage?.s3BucketName ||
      process.env.SCENARIO_BUCKET_NAME ||
      process.env.OUTPUT_BUCKET_NAME ||
      process.env.RAW_BUCKET_NAME ||
      process.env.BIN_BUCKET_NAME ||
      process.env.S3_BUCKET_NAME
  );
}

function resolveScenarioKey(body, generation, baseMessage) {
  const explicitKey = sanitizeText(
    generation?.scenarioKey ||
      generation?.scenarioJsonKey ||
      baseMessage?.scenarioKey ||
      baseMessage?.scenarioJsonKey
  );

  if (explicitKey) {
    return normalizeS3Key(explicitKey);
  }

  const existingArtifactKey = sanitizeText(
    generation?.manifestKey ||
      generation?.runManifestKey ||
      generation?.binKey ||
      generation?.s3Key ||
      generation?.objectKey ||
      baseMessage?.manifestKey ||
      baseMessage?.runManifestKey ||
      baseMessage?.binKey ||
      baseMessage?.s3Key ||
      baseMessage?.objectKey
  );

  const artifactFolder = extractFolderFromS3Key(existingArtifactKey);
  if (artifactFolder) {
    return `${normalizeS3Prefix(artifactFolder)}scenario.json`;
  }

  const folder = sanitizeText(
    generation?.outputPrefix ||
      generation?.s3Prefix ||
      generation?.prefix ||
      generation?.folderKey ||
      generation?.folderPath ||
      generation?.runPrefix ||
      generation?.runFolder ||
      generation?.outputFolder ||
      generation?.destinationPrefix ||
      baseMessage?.outputPrefix ||
      baseMessage?.s3Prefix ||
      baseMessage?.prefix ||
      baseMessage?.folderKey ||
      baseMessage?.folderPath ||
      baseMessage?.runPrefix ||
      baseMessage?.runFolder ||
      baseMessage?.outputFolder ||
      baseMessage?.destinationPrefix
  );

  if (folder) {
    return `${normalizeS3Prefix(folder)}scenario.json`;
  }

  const clientId = sanitizeText(body?.clientId || baseMessage?.clientId);
  const runId = sanitizeText(body?.runId || baseMessage?.runId || body?.requestId || baseMessage?.requestId);

  if (clientId && runId) {
    return `${normalizeS3Prefix(clientId)}${normalizeS3Prefix(runId)}scenario.json`;
  }

  return '';
}

function extractFolderFromS3Key(key) {
  const normalized = normalizeS3Key(key);

  if (!normalized) {
    return '';
  }

  const lastSlashIndex = normalized.lastIndexOf('/');

  if (lastSlashIndex < 0) {
    return '';
  }

  return normalized.slice(0, lastSlashIndex + 1);
}

function normalizeS3Prefix(value) {
  const normalized = normalizeS3Key(value);

  if (!normalized) {
    return '';
  }

  return normalized.endsWith('/') ? normalized : `${normalized}/`;
}

function normalizeS3Key(value) {
  return String(value || '')
    .trim()
    .replace(/^\/+/g, '')
    .replace(/\/{2,}/g, '/');
}

async function generateValidatedSignalDeltaScenario(body) {
  const scenarioRequest = buildScenarioRequest(body);

  const phasePlanPromptTemplate = await loadPromptTemplate('PHASE_PLAN_PROMPT_KEY');
  const phasePlanPrompt = buildPhasePlanPrompt(phasePlanPromptTemplate.template, scenarioRequest);
  const phasePlanResponse = await callBedrockMantle(phasePlanPrompt);
  const phasePlanText = extractTextFromMantleResponse(phasePlanResponse);
  const phasePlan = parseJsonResponse(phasePlanText);

  const phasePlanValidation = validatePhasePlan(phasePlan, scenarioRequest);
  if (!phasePlanValidation.valid) {
    const error = new Error('Bedrock returned an invalid phase plan.');
    error.validationErrors = phasePlanValidation.errors;
    error.rawResponse = phasePlanText;
    throw error;
  }

  const phaseBehaviorPromptTemplate = await loadPromptTemplate('PHASE_BEHAVIOR_PROMPT_KEY');
  const behaviorPhases = [];
  const rawBehaviorResponses = [];

  for (let phaseIndex = 0; phaseIndex < phasePlan.ph.length; phaseIndex += 1) {
    const phase = phasePlan.ph[phaseIndex];
    const previousPhase = behaviorPhases[phaseIndex - 1] || null;

    const phaseBehaviorPrompt = buildPhaseBehaviorPrompt(
      phaseBehaviorPromptTemplate.template,
      scenarioRequest,
      phasePlan,
      phase,
      phaseIndex,
      previousPhase
    );

    const phaseBehaviorResponse = await callBedrockMantle(phaseBehaviorPrompt);
    const phaseBehaviorText = extractTextFromMantleResponse(phaseBehaviorResponse);
    const phaseBehavior = parseJsonResponse(phaseBehaviorText);

    const normalizedPhaseBehavior = normalizePhaseBehaviorResponse(phaseBehavior, phase);
    const phaseBehaviorValidation = validateSinglePhaseBehavior(
      normalizedPhaseBehavior,
      scenarioRequest,
      phase,
      phaseIndex,
      previousPhase
    );

    if (!phaseBehaviorValidation.valid) {
      const error = new Error(`Bedrock returned an invalid behavior for phase ${phaseIndex}.`);
      error.validationErrors = phaseBehaviorValidation.errors;
      error.rawResponse = phaseBehaviorText;
      throw error;
    }

    behaviorPhases.push(normalizedPhaseBehavior);
    rawBehaviorResponses.push({
      phaseIndex,
      phaseName: phase.n,
      rawResponse: phaseBehaviorText
    });
  }

  const scenario = {
    n: scenarioRequest.scenarioName,
    dur: scenarioRequest.durationSeconds,
    dt: scenarioRequest.sampleIntervalSeconds,
    ph: behaviorPhases
  };

  const validation = validateSignalDeltaScenario(scenario, scenarioRequest);

  if (!validation.valid) {
    const error = new Error('Merged signal delta scenario is invalid.');
    error.validationErrors = validation.errors;
    error.rawResponse = JSON.stringify({ phasePlan, behaviorPhases }, null, 2);
    throw error;
  }

  return {
    scenarioRequest,
    phasePlan,
    scenario,
    rawResponse: JSON.stringify({ phasePlan, behaviorPhases }, null, 2),
    rawBehaviorResponses,
    promptSource: {
      phasePlan: phasePlanPromptTemplate.source,
      phaseBehavior: phaseBehaviorPromptTemplate.source
    }
  };
}

async function loadPromptTemplate(promptKeyEnvironmentVariableName) {
  const bucket = sanitizeText(process.env.PROMPT_BUCKET);
  const key = normalizeS3Key(process.env[promptKeyEnvironmentVariableName]);

  if (!bucket) {
    throw new Error('PROMPT_BUCKET environment variable was not defined.');
  }

  if (!key) {
    throw new Error(`${promptKeyEnvironmentVariableName} environment variable was not defined.`);
  }

  const response = await s3Client.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key
    })
  );

  if (!response.Body) {
    throw new Error(`Prompt file is empty or unreadable: s3://${bucket}/${key}`);
  }

  const template = await response.Body.transformToString('utf8');

  if (!template.trim()) {
    throw new Error(`Prompt file is empty: s3://${bucket}/${key}`);
  }

  return {
    template,
    source: {
      bucket,
      key
    }
  };
}

function buildPhasePlanPrompt(template, request) {
  return renderPromptTemplate(template, {
    scenarioName: request.scenarioName,
    durationSeconds: String(request.durationSeconds),
    sampleIntervalSeconds: String(request.sampleIntervalSeconds),
    phaseCount: String(request.phaseCount),
    requestedContext: request.requestedContext,
    driverProfile: request.driverProfile,
    targetSpeed: formatTargetSpeed(request),
    simulationMode: request.simulationMode || 'not specified',
    generationType: request.generationType || 'not specified',
    routeRegion: request.routeRegion || 'not specified',
    initialDateTime: request.initialDateTime || 'not specified',
    selectedCanFrames: String(request.selectedCanFrames),
    amountOfVehicles: String(request.amountOfVehicles)
  });
}

function buildPhaseBehaviorPrompt(template, request, phasePlan, phase, phaseIndex, previousPhase) {
  const availableSignalsText = request.controlledSignals.map((signal) => `- ${signal}`).join('\n');

  return renderPromptTemplate(template, {
    scenarioName: request.scenarioName,
    durationSeconds: String(request.durationSeconds),
    sampleIntervalSeconds: String(request.sampleIntervalSeconds),
    phaseCount: String(request.phaseCount),
    requestedContext: request.requestedContext,
    driverProfile: request.driverProfile,
    targetSpeed: formatTargetSpeed(request),
    simulationMode: request.simulationMode || 'not specified',
    generationType: request.generationType || 'not specified',
    routeRegion: request.routeRegion || 'not specified',
    initialDateTime: request.initialDateTime || 'not specified',
    selectedCanFrames: String(request.selectedCanFrames),
    amountOfVehicles: String(request.amountOfVehicles),
    availableSignals: availableSignalsText,
    phasePlanJson: JSON.stringify(phasePlan, null, 2),
    phaseJson: JSON.stringify(phase, null, 2),
    phaseIndex: String(phaseIndex),
    phaseName: String(phase.n || ''),
    phaseFrom: String(phase.from),
    phaseTo: String(phase.to),
    previousPhaseJson: previousPhase ? JSON.stringify(previousPhase, null, 2) : 'null'
  });
}

function renderPromptTemplate(template, values) {
  let rendered = String(template || '');

  for (const [key, value] of Object.entries(values)) {
    rendered = rendered.replaceAll(`{{${key}}}`, String(value ?? ''));
  }

  return rendered.trim();
}

function formatTargetSpeed(request) {
  return Number.isFinite(request.targetSpeed)
    ? `${request.targetSpeed} ${request.distanceUnit}/h`
    : 'not specified';
}

function normalizePhaseBehaviorResponse(phaseBehavior, phasePlanPhase) {
  if (phaseBehavior?.s && typeof phaseBehavior.s === 'object') {
    return {
      n: String(phaseBehavior.n || phasePlanPhase.n || '').trim(),
      from: Number(phaseBehavior.from),
      to: Number(phaseBehavior.to),
      s: phaseBehavior.s
    };
  }

  if (phaseBehavior?.phase?.s && typeof phaseBehavior.phase.s === 'object') {
    return {
      n: String(phaseBehavior.phase.n || phasePlanPhase.n || '').trim(),
      from: Number(phaseBehavior.phase.from),
      to: Number(phaseBehavior.phase.to),
      s: phaseBehavior.phase.s
    };
  }

  return phaseBehavior;
}

function validatePhasePlan(phasePlan, request) {
  const errors = [];

  if (!phasePlan || typeof phasePlan !== 'object') {
    errors.push('Phase plan must be an object.');
    return { valid: false, errors };
  }

  if (typeof phasePlan.n !== 'string' || !phasePlan.n.trim()) {
    errors.push('Phase plan n must be a non-empty string.');
  }

  if (Number(phasePlan.dur) !== Number(request.durationSeconds)) {
    errors.push(`Phase plan dur must be exactly ${request.durationSeconds}.`);
  }

  if (Number(phasePlan.dt) !== Number(request.sampleIntervalSeconds)) {
    errors.push(`Phase plan dt must be exactly ${request.sampleIntervalSeconds}.`);
  }

  if (!Array.isArray(phasePlan.ph) || phasePlan.ph.length !== request.phaseCount) {
    errors.push(`Phase plan ph must contain exactly ${request.phaseCount} phases.`);
    return { valid: false, errors };
  }

  let expectedFrom = 0;

  for (let phaseIndex = 0; phaseIndex < phasePlan.ph.length; phaseIndex += 1) {
    const phase = phasePlan.ph[phaseIndex];

    if (!phase || typeof phase !== 'object') {
      errors.push(`Phase plan ph[${phaseIndex}] must be an object.`);
      continue;
    }

    if (typeof phase.n !== 'string' || !phase.n.trim()) {
      errors.push(`Phase plan ph[${phaseIndex}].n must be a non-empty string.`);
    }

    const from = Number(phase.from);
    const to = Number(phase.to);

    if (!Number.isFinite(from) || from !== expectedFrom) {
      errors.push(`Phase plan ph[${phaseIndex}].from must be ${expectedFrom}.`);
    }

    if (!Number.isFinite(to) || to <= from) {
      errors.push(`Phase plan ph[${phaseIndex}].to must be greater than from.`);
    }

    if (phase.s !== undefined) {
      errors.push(`Phase plan ph[${phaseIndex}] must not contain signal behavior map s.`);
    }

    expectedFrom = to;
  }

  if (expectedFrom !== Number(request.durationSeconds)) {
    errors.push(`Final phase plan phase must end at ${request.durationSeconds}.`);
  }

  return {
    valid: errors.length === 0,
    errors
  };
}

function validateSinglePhaseBehavior(phaseBehavior, request, phasePlanPhase, phaseIndex, previousPhase) {
  const errors = [];

  if (!phaseBehavior || typeof phaseBehavior !== 'object') {
    errors.push(`Phase behavior ${phaseIndex} must be an object.`);
    return { valid: false, errors };
  }

  if (typeof phaseBehavior.n !== 'string' || !phaseBehavior.n.trim()) {
    errors.push(`Phase behavior ${phaseIndex}.n must be a non-empty string.`);
  }

  if (String(phaseBehavior.n || '').trim() !== String(phasePlanPhase.n || '').trim()) {
    errors.push(`Phase behavior ${phaseIndex}.n must match phase plan name ${phasePlanPhase.n}.`);
  }

  if (Number(phaseBehavior.from) !== Number(phasePlanPhase.from)) {
    errors.push(`Phase behavior ${phaseIndex}.from must be ${phasePlanPhase.from}.`);
  }

  if (Number(phaseBehavior.to) !== Number(phasePlanPhase.to)) {
    errors.push(`Phase behavior ${phaseIndex}.to must be ${phasePlanPhase.to}.`);
  }

  if (!phaseBehavior.s || typeof phaseBehavior.s !== 'object' || Array.isArray(phaseBehavior.s)) {
    errors.push(`Phase behavior ${phaseIndex}.s must be a signal behavior map.`);
    return { valid: false, errors };
  }

  const expectedSignals = new Set(request.controlledSignals);
  const previousSignalEnds = new Map();

  if (previousPhase?.s && typeof previousPhase.s === 'object') {
    for (const [signalName, behavior] of Object.entries(previousPhase.s)) {
      const end = Number(behavior?.end);
      if (Number.isFinite(end)) {
        previousSignalEnds.set(signalName, end);
      }
    }
  }

  for (const signalName of expectedSignals) {
    if (!Object.prototype.hasOwnProperty.call(phaseBehavior.s, signalName)) {
      errors.push(`Phase behavior ${phaseIndex}.s is missing signal ${signalName}.`);
    }
  }

  for (const signalName of Object.keys(phaseBehavior.s)) {
    if (!expectedSignals.has(signalName)) {
      errors.push(`Phase behavior ${phaseIndex}.s contains unexpected signal ${signalName}.`);
    }
  }

  for (const [signalName, behavior] of Object.entries(phaseBehavior.s)) {
    validateSignalBehavior(errors, behavior, signalName, phaseIndex, previousSignalEnds.get(signalName));
  }

  return {
    valid: errors.length === 0,
    errors
  };
}

function extractAvailableSignalNamesFromCompiledDbc(compiledDbc) {
  const names = [];
  const fields = Array.isArray(compiledDbc?.f) ? compiledDbc.f : [];
  const nameIndex = fields.indexOf('n');

  if (!compiledDbc?.m || typeof compiledDbc.m !== 'object') {
    return names;
  }

  for (const entries of Object.values(compiledDbc.m)) {
    if (!Array.isArray(entries)) {
      continue;
    }

    for (const entry of entries) {
      const signals = Array.isArray(entry?.frame?.s) ? entry.frame.s : [];

      for (const signal of signals) {
        let signalName = '';

        if (Array.isArray(signal)) {
          const candidate = nameIndex >= 0 ? signal[nameIndex] : signal[8];
          signalName = String(candidate || '').trim();
        } else if (signal && typeof signal === 'object') {
          signalName = String(signal.n || signal.name || '').trim();
        }

        if (signalName) {
          names.push(signalName);
        }
      }
    }
  }

  return Array.from(new Set(names)).sort((a, b) => a.localeCompare(b));
}

async function enqueueWorkerMessages({
  baseMessage,
  vehicles,
  workQueueUrl,
  sqsBatch,
  maxSafeSqsBytes,
  aiBehaviorPlan
}) {
  const sqs = new SQSClient({ region });

  const allEntries = vehicles.map((vehicle, index) => {
    const messageBody = JSON.stringify({
      ...baseMessage,
      aiBehaviorPlan,
      vin: vehicle.vin,
      type: vehicle.type || 'car',
      vehicleIndex: index
    });

    return {
      Id: `v-${index}`,
      MessageBody: messageBody
    };
  });

  const probeBody = allEntries[0]?.MessageBody || '';
  const messageSizeBytes = assertSafeSqsMessageSize(probeBody, maxSafeSqsBytes);

  let sentBatches = 0;

  for (let index = 0; index < allEntries.length; index += sqsBatch) {
    const batch = allEntries.slice(index, index + sqsBatch);

    const response = await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: workQueueUrl,
        Entries: batch
      })
    );

    sentBatches += 1;

    if (response.Failed && response.Failed.length) {
      console.error('Simulator AI assistance SQS batch failures:', JSON.stringify(response.Failed, null, 2));
      throw new Error(`SQS SendMessageBatch failed with ${response.Failed.length} failed message(s).`);
    }
  }

  return {
    sentBatches,
    messageSizeBytes
  };
}

function assertSafeSqsMessageSize(messageBody, maxSafeSqsBytes) {
  const sizeBytes = Buffer.byteLength(messageBody, 'utf8');

  if (sizeBytes > maxSafeSqsBytes) {
    throw new Error(
      `SQS message body too large (${sizeBytes} bytes). Safe limit is ${maxSafeSqsBytes} bytes.`
    );
  }

  return sizeBytes;
}

async function callBedrockMantle(prompt) {
  const requestBody = JSON.stringify({
    model: modelId,
    messages: [
      {
        role: 'user',
        content: prompt
      }
    ],
    temperature: 0.2,
    top_p: 0.9,
    max_tokens: DEFAULT_MAX_TOKENS
  });

  const unsignedRequest = new HttpRequest({
    protocol: 'https:',
    hostname: mantleHost,
    method: 'POST',
    path: mantlePath,
    headers: {
      host: mantleHost,
      'content-type': 'application/json'
    },
    body: requestBody
  });

  const signer = new SignatureV4({
    credentials: defaultProvider(),
    region,
    service: 'bedrock-mantle',
    sha256: Sha256
  });

  const signedRequest = await signer.sign(unsignedRequest);

  const response = await fetch(`https://${mantleHost}${mantlePath}`, {
    method: signedRequest.method,
    headers: signedRequest.headers,
    body: requestBody
  });

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(`Bedrock Mantle request failed with status ${response.status}: ${responseText}`);
  }

  return JSON.parse(responseText);
}

function extractTextFromMantleResponse(response) {
  const text = response?.choices?.[0]?.message?.content;

  if (typeof text !== 'string' || !text.trim()) {
    throw new Error('Bedrock Mantle response text is empty.');
  }

  return text.trim();
}

function buildScenarioRequest(body) {
  const durationSeconds = toPositiveInteger(body?.durationSeconds, 1200);
  const sampleIntervalSeconds = toPositiveInteger(body?.sampleIntervalSeconds, 5);
  const availableSignals = normalizeStringArray(body?.availableSignals);
  const controlledSignals = resolveControlledSignals(availableSignals);

  return {
    schemaVersion: 'trackster-ai-signal-delta-v1',
    scenarioName: sanitizeText(body?.scenarioName) || 'Urban Driving Scenario',
    durationSeconds,
    sampleIntervalSeconds,
    phaseCount: DEFAULT_PHASE_COUNT,
    requestedContext: sanitizeText(body?.requestedContext) || 'realistic urban driving signal delta scenario',
    driverProfile: sanitizeText(body?.driverProfile) || 'Balanced',
    targetSpeed: toOptionalNumber(body?.targetSpeed),
    distanceUnit: sanitizeText(body?.distanceUnit) || 'Km',
    simulationMode: sanitizeText(body?.simulationMode) || 'Time Window',
    generationType: sanitizeText(body?.generationType) || '',
    routeRegion: sanitizeText(body?.routeRegion) || '',
    initialDateTime: sanitizeText(body?.initialDateTime) || '',
    amountOfVehicles: toPositiveInteger(body?.amountOfVehicles, 1),
    selectedCanFrames: toPositiveInteger(body?.selectedCanFrames, 0),
    availableSignals,
    controlledSignals
  };
}

function resolveControlledSignals(availableSignals) {
  const uniqueSignals = Array.from(new Set(availableSignals));

  if (!uniqueSignals.length) {
    throw new Error('No available signals were found for AI scenario generation.');
  }

  return uniqueSignals;
}

function normalizeSignalNameForMatch(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

function parseBody(rawBody) {
  if (!rawBody) {
    return {};
  }

  if (typeof rawBody === 'object') {
    return rawBody;
  }

  return JSON.parse(rawBody);
}

function sanitizeText(value) {
  if (typeof value !== 'string') {
    return '';
  }

  return value.trim().slice(0, 2000);
}

function normalizeStringArray(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((item) => String(item || '').trim())
    .filter(Boolean);
}

function toPositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

function toOptionalNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseJsonResponse(text) {
  try {
    return JSON.parse(text);
  } catch {
    const firstBrace = text.indexOf('{');
    const lastBrace = text.lastIndexOf('}');

    if (firstBrace < 0 || lastBrace <= firstBrace) {
      throw new Error('Bedrock response is not valid JSON.');
    }

    const extractedJson = text.slice(firstBrace, lastBrace + 1);
    return JSON.parse(extractedJson);
  }
}

function validateSignalDeltaScenario(scenario, request) {
  const errors = [];

  if (!scenario || typeof scenario !== 'object') {
    errors.push('Scenario must be an object.');
    return { valid: false, errors };
  }

  if (typeof scenario.n !== 'string' || !scenario.n.trim()) {
    errors.push('n must be a non-empty string.');
  }

  if (Number(scenario.dur) !== Number(request.durationSeconds)) {
    errors.push(`dur must be exactly ${request.durationSeconds}.`);
  }

  if (Number(scenario.dt) !== Number(request.sampleIntervalSeconds)) {
    errors.push(`dt must be exactly ${request.sampleIntervalSeconds}.`);
  }

  if (!Array.isArray(scenario.ph) || scenario.ph.length !== request.phaseCount) {
    errors.push(`ph must contain exactly ${request.phaseCount} phases.`);
    return { valid: false, errors };
  }

  let expectedFrom = 0;
  const expectedSignals = new Set(request.controlledSignals);
  const previousSignalEnds = new Map();

  for (let phaseIndex = 0; phaseIndex < scenario.ph.length; phaseIndex += 1) {
    const phase = scenario.ph[phaseIndex];

    if (!phase || typeof phase !== 'object') {
      errors.push(`ph[${phaseIndex}] must be an object.`);
      continue;
    }

    if (typeof phase.n !== 'string' || !phase.n.trim()) {
      errors.push(`ph[${phaseIndex}].n must be a non-empty string.`);
    }

    const from = Number(phase.from);
    const to = Number(phase.to);

    if (!Number.isFinite(from) || from !== expectedFrom) {
      errors.push(`ph[${phaseIndex}].from must be ${expectedFrom}.`);
    }

    if (!Number.isFinite(to) || to <= from) {
      errors.push(`ph[${phaseIndex}].to must be greater than from.`);
    }

    if (!phase.s || typeof phase.s !== 'object' || Array.isArray(phase.s)) {
      errors.push(`ph[${phaseIndex}].s must be a signal behavior map.`);
      continue;
    }

    for (const signalName of expectedSignals) {
      if (!Object.prototype.hasOwnProperty.call(phase.s, signalName)) {
        errors.push(`ph[${phaseIndex}].s is missing signal ${signalName}.`);
      }
    }

    for (const signalName of Object.keys(phase.s)) {
      if (!expectedSignals.has(signalName)) {
        errors.push(`ph[${phaseIndex}].s contains unexpected signal ${signalName}.`);
      }
    }

    for (const [signalName, behavior] of Object.entries(phase.s)) {
      validateSignalBehavior(errors, behavior, signalName, phaseIndex, previousSignalEnds.get(signalName));

      if (behavior && typeof behavior === 'object') {
        const end = Number(behavior.end);
        if (Number.isFinite(end)) {
          previousSignalEnds.set(signalName, end);
        }
      }
    }

    expectedFrom = to;
  }

  if (expectedFrom !== Number(request.durationSeconds)) {
    errors.push(`Final phase must end at ${request.durationSeconds}.`);
  }

  return {
    valid: errors.length === 0,
    errors
  };
}

function validateSignalBehavior(errors, behavior, signalName, phaseIndex, previousEnd) {
  if (!behavior || typeof behavior !== 'object') {
    errors.push(`Signal behavior for ${signalName} in phase ${phaseIndex} must be an object.`);
    return;
  }

  const start = Number(behavior.start);
  const end = Number(behavior.end);
  const min = Number(behavior.min);
  const max = Number(behavior.max);

  if (!Number.isFinite(start)) errors.push(`${signalName} phase ${phaseIndex}: start must be numeric.`);
  if (!Number.isFinite(end)) errors.push(`${signalName} phase ${phaseIndex}: end must be numeric.`);
  if (!Number.isFinite(min)) errors.push(`${signalName} phase ${phaseIndex}: min must be numeric.`);
  if (!Number.isFinite(max)) errors.push(`${signalName} phase ${phaseIndex}: max must be numeric.`);

  if (Number.isFinite(min) && Number.isFinite(max) && min > max) {
    errors.push(`${signalName} phase ${phaseIndex}: min cannot be greater than max.`);
  }

  if (Number.isFinite(start) && Number.isFinite(min) && Number.isFinite(max) && (start < min || start > max)) {
    errors.push(`${signalName} phase ${phaseIndex}: start is outside min/max.`);
  }

  if (Number.isFinite(end) && Number.isFinite(min) && Number.isFinite(max) && (end < min || end > max)) {
    errors.push(`${signalName} phase ${phaseIndex}: end is outside min/max.`);
  }

  if (Number.isFinite(previousEnd) && Number.isFinite(start)) {
    const tolerance = Math.max(1, Math.abs(previousEnd) * 0.05);
    if (Math.abs(start - previousEnd) > tolerance) {
      errors.push(`${signalName} phase ${phaseIndex}: start is not coherent with previous phase end.`);
    }
  }

  if (!Array.isArray(behavior.d) || behavior.d.length < 4 || behavior.d.length > 12) {
    errors.push(`${signalName} phase ${phaseIndex}: d must contain between 4 and 12 numeric values.`);
    return;
  }

  for (let index = 0; index < behavior.d.length; index += 1) {
    if (!Number.isFinite(Number(behavior.d[index]))) {
      errors.push(`${signalName} phase ${phaseIndex}: d[${index}] must be numeric.`);
    }
  }

  if (isFuelLikeSignal(signalName)) {
    if (Number.isFinite(end) && Number.isFinite(start) && end > start) {
      errors.push(`${signalName} phase ${phaseIndex}: fuel-like signal end cannot be greater than start.`);
    }

    for (let index = 0; index < behavior.d.length; index += 1) {
      if (Number(behavior.d[index]) > 0) {
        errors.push(`${signalName} phase ${phaseIndex}: fuel-like signal deltas must never be positive.`);
      }
    }
  }
}

function isFuelLikeSignal(signalName) {
  const normalized = normalizeSignalNameForMatch(signalName);
  return normalized.includes('fuel') && (normalized.includes('level') || normalized.includes('percent') || normalized.includes('pct'));
}

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: defaultHeaders,
    body: JSON.stringify(body)
  };
}