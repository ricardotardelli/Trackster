import { Sha256 } from '@aws-crypto/sha256-js';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand
} from '@aws-sdk/client-s3';
import { HttpRequest } from '@smithy/protocol-http';
import { SignatureV4 } from '@smithy/signature-v4';

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

const DEFAULT_MAX_TOKENS = Number.parseInt(process.env.BEDROCK_MAX_TOKENS || '8192', 10);
const DEFAULT_PLANNER_RETRIES = Number.parseInt(process.env.PLANNER_RETRIES || '1', 10);
const DEFAULT_BEHAVIOR_RETRIES = Number.parseInt(process.env.BEHAVIOR_RETRIES || '1', 10);
const DEFAULT_TEST_OUTPUT_PREFIX = '20260510221008/';
const DEFAULT_SIGNAL_KNOWLEDGE_PROMPT_KEY = 'prompts/simulator-signal-knowledge-v1.txt';

export const handler = async (event) => {
  const startedAt = Date.now();

  if (event.requestContext?.http?.method === 'OPTIONS' || event.httpMethod === 'OPTIONS') {
    return buildResponse(200, { success: true });
  }

  if (event.requestContext?.http?.method !== 'POST' && event.httpMethod !== 'POST') {
    return buildResponse(405, { success: false, message: 'Method not allowed.' });
  }

  let body = {};

  try {
    body = parseBody(event.body);

    const explicitAiPhase = sanitizeText(body?.aiPhase || body?.phase || body?.generation?.aiPhase).toLowerCase();
    const eventSource = sanitizeText(event?.source);
    const eventAction = sanitizeText(event?.action);

    const aiPhase = explicitAiPhase || (
      eventSource === 'trackster-orchestrator' || eventAction === 'run_full_ai_pipeline'
        ? 'auto'
        : 'auto'
    );

    if (aiPhase === 'auto') {
      const autoResult = await runAutoPipeline(body, startedAt);
      const processingTimeMs = Date.now() - startedAt;

      return buildResponse(200, {
        success: true,
        status: autoResult.status,
        aiPhase: 'auto',
        processingTimeMs,
        modelId,
        scenarioS3: autoResult.scenarioS3,
        signalKnowledgeS3: autoResult.signalKnowledgeS3,
        signalKnowledge: autoResult.signalKnowledge,
        planner: autoResult.planner
      });
    }

    if (aiPhase === 'planner' || aiPhase === 'phase1' || aiPhase === 'plan') {
      const plannerResult = await runPlannerPhase(body, {
        processingTimeMs: Date.now() - startedAt
      });
      const processingTimeMs = Date.now() - startedAt;

      return buildResponse(200, {
        success: true,
        status: plannerResult.status,
        aiPhase: 'planner',
        processingTimeMs,
        modelId,
        scenarioS3: plannerResult.scenarioS3,
        signalKnowledgeS3: plannerResult.signalKnowledgeS3 || null,
        signalKnowledge: plannerResult.signalKnowledge || null,
        planner: plannerResult.planner
      });
    }

    if (aiPhase === 'status') {
      const statusResult = await readScenarioStatus(body);
      const processingTimeMs = Date.now() - startedAt;

      return buildResponse(200, {
        success: true,
        status: statusResult.scenarioDocument?.status || 'unknown',
        aiPhase: 'status',
        processingTimeMs,
        scenarioS3: statusResult.scenarioS3,
        progress: statusResult.scenarioDocument?.progress || null,
        currentStep: statusResult.scenarioDocument?.currentStep || null,
        logs: statusResult.scenarioDocument?.logs || [],
        scenario: statusResult.scenarioDocument
      });
    }

    if (aiPhase === 'signal_knowledge' || aiPhase === 'knowledge' || aiPhase === 'phase0') {
      const knowledgeResult = await runSignalKnowledgePhase(body, {
        processingTimeMs: Date.now() - startedAt
      });
      const processingTimeMs = Date.now() - startedAt;

      return buildResponse(200, {
        success: true,
        status: knowledgeResult.status,
        aiPhase: 'signal_knowledge',
        processingTimeMs,
        modelId,
        scenarioS3: knowledgeResult.scenarioS3,
        signalKnowledgeS3: knowledgeResult.signalKnowledgeS3,
        signalKnowledge: knowledgeResult.signalKnowledge
      });
    }

    if (aiPhase === 'behavior' || aiPhase === 'phase2' || aiPhase === 'behaviour' || aiPhase === 'next_behavior_phase') {
      return buildResponse(400, {
        success: false,
        message: 'Behavior phase is temporarily disabled. Current AI assist execution only generates signal knowledge.'
      });
    }

    return buildResponse(400, {
      success: false,
      message: 'Invalid aiPhase. Use auto or signal_knowledge.'
    });
  } catch (error) {
    const processingTimeMs = Date.now() - startedAt;

    console.error('Simulator AI assist failed:', {
      processingTimeMs,
      name: error?.name,
      message: error?.message,
      stack: error?.stack,
      validationErrors: error?.validationErrors || null,
      rawResponse: error?.rawResponse || null
    });

    await tryWriteFailureToScenario(body, error);

    return buildResponse(500, {
      success: false,
      message: 'Simulator AI assist failed.',
      processingTimeMs,
      error: error?.message || 'Unknown error',
      validationErrors: error?.validationErrors || null
    });
  }
};


async function runAutoPipeline(body, startedAt) {
  const knowledgeResult = await runSignalKnowledgePhase(body, {
    processingTimeMs: Date.now() - startedAt
  });

  const plannerResult = await runPlannerPhase(body, {
    signalKnowledge: knowledgeResult.signalKnowledge,
    signalKnowledgeS3: knowledgeResult.signalKnowledgeS3,
    processingTimeMs: Date.now() - startedAt
  });

  return {
    status: plannerResult.status,
    scenarioS3: plannerResult.scenarioS3,
    signalKnowledgeS3: knowledgeResult.signalKnowledgeS3,
    signalKnowledge: knowledgeResult.signalKnowledge,
    planner: plannerResult.planner,
    progress: plannerResult.progress,
    currentStep: plannerResult.currentStep
  };
}


async function runPlannerPhase(body, options = {}) {
  const signalKnowledgeContext = await resolveSignalKnowledgeForPlanner(body, options);
  const plannerGeneration = await generateValidatedDrivingPlanner(body, signalKnowledgeContext.signalKnowledge);
  const scenarioS3 = await savePlannerScenarioJson({
    body,
    plannerResult: plannerGeneration,
    signalKnowledge: signalKnowledgeContext.signalKnowledge,
    processingTimeMs: Math.max(0, Math.round(Number(options.processingTimeMs || 0)))
  });

  return {
    status: 'Driving planner completed. Waiting for behavior phase generation.',
    scenarioS3,
    signalKnowledgeS3: signalKnowledgeContext.signalKnowledgeS3 || null,
    signalKnowledge: signalKnowledgeContext.signalKnowledge,
    planner: plannerGeneration.planner,
    progress: buildProgress(plannerGeneration.planner, []),
    currentStep: 'Driving planner completed. Waiting for behavior phase generation.'
  };
}

async function resolveSignalKnowledgeForPlanner(body, options = {}) {
  if (options.signalKnowledge?.schemaVersion === 'trackster-signal-knowledge-v1') {
    return {
      signalKnowledge: options.signalKnowledge,
      signalKnowledgeS3: options.signalKnowledgeS3 || null
    };
  }

  if (body?.signalKnowledge?.schemaVersion === 'trackster-signal-knowledge-v1') {
    return {
      signalKnowledge: normalizeSignalKnowledge(
        body.signalKnowledge,
        extractRequestedSignalMetadata(body).map((signal) => signal.name)
      ),
      signalKnowledgeS3: null
    };
  }

  try {
    const { bucket, key } = resolveScenarioLocation(body);
    if (bucket && key) {
      const scenarioDocument = await safeReadJsonFromS3(bucket, key);
      if (scenarioDocument?.signalKnowledge?.schemaVersion === 'trackster-signal-knowledge-v1') {
        return {
          signalKnowledge: scenarioDocument.signalKnowledge,
          signalKnowledgeS3: scenarioDocument.signalKnowledgeS3 || null
        };
      }
    }
  } catch (error) {
    console.warn('Unable to reuse signal knowledge from scenario.json. Generating a new one.', {
      message: error?.message
    });
  }

  const knowledgeResult = await runSignalKnowledgePhase(body);
  return {
    signalKnowledge: knowledgeResult.signalKnowledge,
    signalKnowledgeS3: knowledgeResult.signalKnowledgeS3
  };
}

async function createInitialScenarioJson({ body, bucket, key }) {
  const scenarioDocument = {
    status: 'Trackster AI is preparing the simulation scenario.',
    progress: {
      totalPhases: 0,
      completedPhases: 0,
      failedPhases: 0,
      percent: 0
    },
    processingTimeMs: 0,
    scenario: null,
    signalKnowledge: null,
    behavior: {
      phases: []
    }
  };

  await putJsonToS3(bucket, key, scenarioDocument);

  return { bucket, key, fileName: 'scenario.json' };
}

async function readScenarioStatus(body) {
  const { bucket, key } = resolveScenarioLocation(body);

  if (!bucket) {
    throw new Error('Unable to read scenario.json because no S3 bucket was resolved.');
  }

  if (!key) {
    throw new Error('Unable to read scenario.json because no S3 key or output folder was resolved.');
  }

  const scenarioDocument = await readJsonFromS3(bucket, key);

  return {
    scenarioS3: { bucket, key, fileName: 'scenario.json' },
    scenarioDocument
  };
}


function resolveClientId(body) {
  const generation = body?.generation || {};
  const baseMessage = generation?.baseMessage || body?.baseMessage || {};

  return sanitizeText(
    body?.clientId ||
      body?.customerId ||
      generation?.clientId ||
      generation?.customerId ||
      baseMessage?.clientId ||
      baseMessage?.customerId
  );
}

function resolveSignalKnowledgeLocation(body) {
  const generation = body?.generation || {};
  const baseMessage = generation?.baseMessage || body?.baseMessage || {};
  const clientId = resolveClientId(body);

  if (!clientId) {
    throw new Error('Unable to resolve signal knowledge key because clientId was not provided.');
  }

  const configuredOutput = sanitizeText(
    process.env.SIGNAL_KNOWLEDGE_OUTPUT_KEY ||
      process.env.SIGNAL_KNOWLEDGE_OUTPUT ||
      process.env.SIGNAL_KNOWLEDGE_BUCKET ||
      process.env.SIGNAL_KNOWLEDGE_KEY
  );

  let bucket = resolveScenarioBucket(generation, baseMessage, body);
  let key = `${normalizeS3Prefix(clientId)}signal-knowledge.json`;

  if (configuredOutput) {
    const withoutS3Scheme = configuredOutput.replace(/^s3:\/\//i, '');
    const slashIndex = withoutS3Scheme.indexOf('/');

    if (configuredOutput.toLowerCase().startsWith('s3://')) {
      bucket = withoutS3Scheme.slice(0, slashIndex >= 0 ? slashIndex : undefined);
      if (slashIndex >= 0) {
        const configuredKey = normalizeS3Key(withoutS3Scheme.slice(slashIndex + 1));
        if (configuredKey) {
          key = configuredKey
            .replaceAll('{{clientId}}', clientId)
            .replaceAll('{clientId}', clientId)
            .replaceAll('${clientId}', clientId);
        }
      }
    } else if (configuredOutput.includes('/') || configuredOutput.toLowerCase().endsWith('.json')) {
      key = normalizeS3Key(
        configuredOutput
          .replaceAll('{{clientId}}', clientId)
          .replaceAll('{clientId}', clientId)
          .replaceAll('${clientId}', clientId)
      );
    } else {
      bucket = configuredOutput;
    }
  }

  if (!bucket) {
    throw new Error('Unable to resolve signal knowledge bucket. Send bucketName/rawBucketName, set SIGNAL_KNOWLEDGE_OUTPUT_KEY to the CAN bucket, or define BIN_BUCKET_NAME/RAW_BUCKET_NAME.');
  }

  key = normalizeS3Key(key);

  if (!key) {
    throw new Error('Unable to resolve signal knowledge key.');
  }

  console.log('Signal knowledge output location:', {
    bucket,
    key
  });

  return {
    bucket,
    key,
    fileName: key.split('/').filter(Boolean).pop() || 'signal-knowledge.json'
  };
}

async function putSignalKnowledgeToS3(body, signalKnowledge) {
  const location = resolveSignalKnowledgeLocation(body);

  await putJsonToS3(location.bucket, location.key, signalKnowledge);

  return location;
}

async function runSignalKnowledgePhase(body, options = {}) {
  const signalKnowledge = await generateValidatedSignalKnowledge(body);
  const signalKnowledgeS3 = await putSignalKnowledgeToS3(body, signalKnowledge);
  const scenarioS3 = await saveSignalKnowledgeCompletedScenarioJson({
    body,
    signalKnowledge,
    signalKnowledgeS3,
    processingTimeMs: Math.max(0, Math.round(Number(options.processingTimeMs || 0)))
  });

  return {
    scenarioS3,
    signalKnowledgeS3,
    status: 'Trackster AI completed the simulation behavior plan.',
    progress: {
      totalPhases: 1,
      completedPhases: 1,
      failedPhases: 0,
      processingPhases: 0,
      percent: 100
    },
    currentStep: 'Trackster AI completed the simulation behavior plan.',
    signalKnowledge
  };
}

async function saveSignalKnowledgeCompletedScenarioJson({ body, signalKnowledge, signalKnowledgeS3, processingTimeMs }) {
  const { bucket, key } = resolveScenarioLocation(body);

  if (!bucket) {
    throw new Error('Unable to save scenario.json because no S3 bucket was resolved.');
  }

  if (!key) {
    throw new Error('Unable to save scenario.json because no S3 key or output folder was resolved.');
  }

  const scenarioDocument = {
    status: 'Trackster AI completed the simulation behavior plan.',
    progress: {
      totalPhases: 1,
      completedPhases: 1,
      failedPhases: 0,
      processingPhases: 0,
      percent: 100
    },
    processingTimeMs,
    scenario: null,
    signalKnowledge,
    signalKnowledgeS3,
    behavior: {
      phases: []
    }
  };

  await putJsonToS3(bucket, key, scenarioDocument);

  return { bucket, key, fileName: 'scenario.json' };
}

async function ensureSignalKnowledgeForScenario({ body, bucket, key, scenarioDocument, planner }) {
  if (scenarioDocument?.signalKnowledge?.schemaVersion === 'trackster-signal-knowledge-v1') {
    return scenarioDocument;
  }

  const signalKnowledge = await generateValidatedSignalKnowledge(body);
  await putSignalKnowledgeToS3(body, signalKnowledge);

  const latestScenarioDocument = await safeReadScenarioDocument(bucket, key, scenarioDocument);
  const updatedDocument = attachSignalKnowledgeToScenarioDocument(
    ensureScenarioRuntimeFields(latestScenarioDocument, planner, extractRequestedSignalMetadata(body)),
    signalKnowledge
  );

  await putJsonToS3(bucket, key, updatedDocument);

  return updatedDocument;
}

function attachSignalKnowledgeToScenarioDocument(scenarioDocument, signalKnowledge) {
  return {
    ...scenarioDocument,
    signalKnowledge
  };
}

async function generateValidatedSignalKnowledge(body) {
  const requestedSignalMetadata = expandSignalsForKnowledgePrompt(extractRequestedSignalMetadata(body));

  if (!requestedSignalMetadata.length) {
    throw new Error('Unable to generate signal knowledge because no signals were resolved from the request.');
  }

  const promptTemplateResult = await loadPromptTemplate('SIGNAL_KNOWLEDGE_PROMPT_KEY');
  const prompt = buildSignalKnowledgePrompt(promptTemplateResult.template, requestedSignalMetadata);

  let lastError = null;
  let lastRawResponse = null;
  const attempts = Math.max(1, DEFAULT_PLANNER_RETRIES + 1);

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const effectivePrompt = attempt === 1 ? prompt : buildRetryPrompt(prompt, lastRawResponse, lastError);
      const mantleResponse = await callBedrockMantle(effectivePrompt);
      const responseText = extractTextFromMantleResponse(mantleResponse);
      lastRawResponse = responseText;

      const signalKnowledge = parseJsonResponse(responseText);
      const validation = validateSignalKnowledge(signalKnowledge, requestedSignalMetadata.map((signal) => signal.signalName));

      if (!validation.valid) {
        const validationError = new Error('Bedrock returned an invalid signal knowledge JSON.');
        validationError.validationErrors = validation.errors;
        validationError.rawResponse = responseText;
        throw validationError;
      }

      return normalizeSignalKnowledge(signalKnowledge, requestedSignalMetadata.map((signal) => signal.signalName));
    } catch (error) {
      if (!error.rawResponse && lastRawResponse) {
        error.rawResponse = lastRawResponse;
      }

      lastError = error;
      console.error('Signal knowledge attempt failed:', {
        attempt,
        maxAttempts: attempts,
        message: error?.message,
        validationErrors: error?.validationErrors || null
      });

      if (attempt >= attempts) {
        const finalError = new Error('Unable to generate a valid signal knowledge JSON.');
        finalError.validationErrors = error?.validationErrors || null;
        finalError.rawResponse = lastRawResponse;
        finalError.cause = error;
        throw finalError;
      }
    }
  }

  throw lastError || new Error('Unable to generate signal knowledge JSON.');
}

function buildSignalKnowledgePrompt(template, requestedSignalMetadata) {
  return template
    .replaceAll('{{signalsJson}}', JSON.stringify(requestedSignalMetadata || [], null, 2))
    .trim();
}

function expandSignalsForKnowledgePrompt(signalMetadata = []) {
  if (!Array.isArray(signalMetadata)) {
    return [];
  }

  return signalMetadata
    .map((signal) => {
      const signalName = sanitizeSignalName(signal?.n || signal?.name || signal?.signalName);

      if (!signalName) {
        return null;
      }

      const result = { signalName };

      const messageName = sanitizeText(signal?.messageName || signal?.frameName || signal?.canMessageName);
      if (messageName) result.messageName = messageName;

      const canId = sanitizeText(signal?.canId || signal?.canID || signal?.id);
      if (canId) result.canId = canId;

      const dbcFile = sanitizeText(signal?.dbcFile || signal?.dbc || signal?.sourceDbc);
      if (dbcFile) result.dbcFile = dbcFile;

      const engineeringUnit = sanitizeEngineeringUnit(signal?.u || signal?.unit || signal?.engineeringUnit);
      if (engineeringUnit) result.engineeringUnit = engineeringUnit;

      const physicalMin = firstFiniteNumber(signal?.mn, signal?.physicalMin);
      if (Number.isFinite(physicalMin)) result.physicalMin = physicalMin;

      const physicalMax = firstFiniteNumber(signal?.mx, signal?.physicalMax);
      if (Number.isFinite(physicalMax)) result.physicalMax = physicalMax;

      const factor = firstFiniteNumber(signal?.f, signal?.factor);
      if (Number.isFinite(factor)) result.factor = factor;

      const offset = firstFiniteNumber(signal?.o, signal?.offset);
      if (Number.isFinite(offset)) result.offset = offset;

      const bitLength = firstPositiveInteger(signal?.b, signal?.bitLength);
      if (Number.isInteger(bitLength)) result.bitLength = bitLength;

      return result;
    })
    .filter(Boolean);
}

function validateSignalKnowledge(signalKnowledge, requestedSignalNames) {
  const errors = [];

  if (!signalKnowledge || typeof signalKnowledge !== 'object' || Array.isArray(signalKnowledge)) {
    errors.push('Signal knowledge response must be an object.');
    return { valid: false, errors };
  }

  if (signalKnowledge.schemaVersion !== 'trackster-signal-knowledge-v1') {
    errors.push('schemaVersion must be trackster-signal-knowledge-v1.');
  }

  if (!Array.isArray(signalKnowledge.signals)) {
    errors.push('signals must be an array.');
    return { valid: false, errors };
  }

  const requestedSet = new Set(requestedSignalNames);
  const seen = new Set();

  for (let index = 0; index < signalKnowledge.signals.length; index += 1) {
    const signal = signalKnowledge.signals[index];

    if (!signal || typeof signal !== 'object' || Array.isArray(signal)) {
      errors.push(`signals[${index}] must be an object.`);
      continue;
    }

    const name = sanitizeSignalName(signal.name || signal.signalName || signal.n);

    if (!name) {
      errors.push(`signals[${index}].name must be a non-empty string.`);
      continue;
    }

    if (!requestedSet.has(name)) {
      errors.push(`signals[${index}].name is not in requested signals: ${name}.`);
    }

    if (seen.has(name)) {
      errors.push(`signal is duplicated: ${name}.`);
    }

    seen.add(name);

    if (!isNonEmptyString(signal.meaning)) {
      errors.push(`signals[${index}].meaning must be a non-empty string.`);
    }

    if (typeof signal.generateBehavior !== 'boolean') {
      errors.push(`signals[${index}].generateBehavior must be boolean.`);
    }

    if (!isNonEmptyString(signal.reason)) {
      errors.push(`signals[${index}].reason must be a non-empty string.`);
    }

    if (!Object.prototype.hasOwnProperty.call(signal, 'dependency')) {
      errors.push(`signals[${index}].dependency must be present and must be null or an exact requested signal name.`);
    } else if (signal.dependency !== null) {
      if (typeof signal.dependency !== 'string' || !signal.dependency.trim()) {
        errors.push(`signals[${index}].dependency must be null or a non-empty string.`);
      } else {
        const dependency = signal.dependency.trim();

        if (!requestedSet.has(dependency)) {
          errors.push(`signals[${index}].dependency is not in requested signals: ${dependency}.`);
        }

        if (dependency === name) {
          errors.push(`signals[${index}].dependency cannot reference the same signal: ${dependency}.`);
        }
      }
    }
  }

  for (const requestedSignalName of requestedSignalNames) {
    if (!seen.has(requestedSignalName)) {
      errors.push(`requested signal is missing from signal knowledge: ${requestedSignalName}.`);
    }
  }

  return {
    valid: errors.length === 0,
    errors
  };
}

function normalizeSignalDependency(value, requestedSet, signalName) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const dependency = value.trim();

  if (!dependency || dependency === signalName || !requestedSet.has(dependency)) {
    return null;
  }

  return dependency;
}

function normalizeSignalKnowledge(signalKnowledge, requestedSignalNames) {
  const requestedSet = new Set(requestedSignalNames);
  const seen = new Set();
  const signals = [];

  for (const signal of signalKnowledge.signals || []) {
    const name = sanitizeSignalName(signal?.name || signal?.signalName || signal?.n);

    if (!name || seen.has(name) || !requestedSet.has(name)) {
      continue;
    }

    seen.add(name);

    signals.push({
      name,
      meaning: sanitizeText(signal.meaning) || 'Unknown',
      generateBehavior: Boolean(signal.generateBehavior),
      reason: sanitizeText(signal.reason) || 'No reason provided.',
      dependency: normalizeSignalDependency(signal.dependency, requestedSet, name)
    });
  }

  return {
    schemaVersion: 'trackster-signal-knowledge-v1',
    signals
  };
}


async function runBehaviorPhase(body, options = {}) {
  const { bucket, key } = resolveScenarioLocation(body);

  if (!bucket) {
    throw new Error('Unable to read scenario.json because no S3 bucket was resolved.');
  }

  if (!key) {
    throw new Error('Unable to read scenario.json because no S3 key or output folder was resolved.');
  }

  let scenarioDocument = await readJsonFromS3(bucket, key);
  const planner = getScenarioPlanner(scenarioDocument);
  const plannerRequest = buildPlannerRequest(body);

  if (!planner || !Array.isArray(planner.phases) || !planner.phases.length) {
    throw new Error('scenario.json does not contain scenario.phases. Run aiPhase=planner first.');
  }

  const requestedSignals = resolveRequestedSignalsForBehavior(body, scenarioDocument);

  if (!requestedSignals.length) {
    throw new Error('Unable to generate signal behavior plan because no signals were resolved from the request. Send signals, selectedSignals or signalNames in the body.');
  }

  scenarioDocument = ensureScenarioRuntimeFields(scenarioDocument, planner);

  const phase = resolveBehaviorPhaseToProcess({
    body,
    planner,
    scenarioDocument,
    processNextPending: options.processNextPending
  });

  if (!phase) {
    scenarioDocument = updateScenarioProgress(scenarioDocument, planner);
    scenarioDocument.status = 'All behavior phases completed.';

    await putJsonToS3(bucket, key, scenarioDocument);

    return {
      scenarioS3: { bucket, key, fileName: 'scenario.json' },
      status: scenarioDocument.status,
      progress: scenarioDocument.progress,
      currentStep: scenarioDocument.status,
      phaseId: null,
      phaseStatus: 'completed',
      signalBehaviorPhase: null
    };
  }

  scenarioDocument = markBehaviorPhaseProcessing(scenarioDocument, planner, phase.id);
  await putJsonToS3(bucket, key, scenarioDocument);

  const phaseStartedAt = Date.now();

  try {
    const singlePhasePlanner = {
      ...planner,
      phases: [phase]
    };

    const behaviorGeneration = await generateValidatedSignalBehaviorPlan({
      body: { ...body, signals: requestedSignals },
      plannerRequest,
      planner: singlePhasePlanner,
      scenarioDocument,
      phaseId: phase.id
    });

    const behaviorPhase = behaviorGeneration.behaviorPlan.phases[0];
    scenarioDocument = await readJsonFromS3(bucket, key);
    scenarioDocument = ensureScenarioRuntimeFields(scenarioDocument, planner);
    scenarioDocument = markBehaviorPhaseCompleted({
      scenarioDocument,
      planner,
      phaseId: phase.id,
      behaviorPhase,
      processingTimeMs: Date.now() - phaseStartedAt
    });

    await putJsonToS3(bucket, key, scenarioDocument);

    return {
      scenarioS3: { bucket, key, fileName: 'scenario.json' },
      status: scenarioDocument.status,
      progress: scenarioDocument.progress,
      currentStep: scenarioDocument.status,
      phaseId: phase.id,
      phaseStatus: 'completed',
      signalBehaviorPhase: behaviorPhase
    };
  } catch (error) {
    scenarioDocument = await safeReadScenarioDocument(bucket, key, scenarioDocument);
    scenarioDocument = ensureScenarioRuntimeFields(scenarioDocument, planner);
    scenarioDocument = markBehaviorPhaseFailed({
      scenarioDocument,
      planner,
      phaseId: phase.id,
      error,
      processingTimeMs: Date.now() - phaseStartedAt
    });
    await putJsonToS3(bucket, key, scenarioDocument);
    throw error;
  }
}

async function safeReadJsonFromS3(bucket, key) {
  try {
    return await readJsonFromS3(bucket, key);
  } catch (error) {
    const name = String(error?.name || '');
    const message = String(error?.message || '');

    if (
      name === 'NoSuchKey' ||
      name === 'NotFound' ||
      message.includes('NoSuchKey') ||
      message.includes('not found')
    ) {
      return null;
    }

    throw error;
  }
}

function isFinalScenarioStatus(status) {
  const normalizedStatus = sanitizeText(status).toLowerCase();

  return (
    normalizedStatus === 'completed' ||
    normalizedStatus === 'success' ||
    normalizedStatus === 'done' ||
    normalizedStatus === 'finished' ||
    normalizedStatus === 'all behavior phases completed.' ||
    normalizedStatus === 'trackster ai completed the simulation behavior plan.'
  );
}

function getScenarioPlanner(scenarioDocument) {
  if (scenarioDocument?.scenario && Array.isArray(scenarioDocument.scenario.phases)) {
    return scenarioDocument.scenario;
  }

  if (scenarioDocument?.planner?.result && Array.isArray(scenarioDocument.planner.result.phases)) {
    return scenarioDocument.planner.result;
  }

  return null;
}

function getBehaviorPhases(scenarioDocument) {
  const phases =
    scenarioDocument?.behavior?.phases ||
    scenarioDocument?.signalBehaviorPlan?.result?.phases;

  return Array.isArray(phases) ? phases : [];
}

function isBehaviorPhaseCompleted(scenarioDocument, phaseId) {
  return getBehaviorPhases(scenarioDocument).some((phase) =>
    Number(phase?.phaseId) === Number(phaseId) &&
    String(phase?.status || '').toLowerCase() === 'completed'
  );
}

async function safeReadScenarioDocument(bucket, key, fallbackDocument) {
  try {
    return await readJsonFromS3(bucket, key);
  } catch {
    return fallbackDocument;
  }
}

function resolveScenarioLocation(body) {
  const generation = body?.generation || {};
  const baseMessage = generation?.baseMessage || body?.baseMessage || {};

  return {
    bucket: resolveScenarioBucket(generation, baseMessage, body),
    key: resolveScenarioKey(body, generation, baseMessage)
  };
}

function resolveRequestedSignalsForBehavior(body, scenarioDocument) {
  const bodySignals = extractRequestedSignals(body);
  if (bodySignals.length) return bodySignals;

  return [];
}

function resolveBehaviorPhaseToProcess({ body, planner, scenarioDocument, processNextPending }) {
  const explicitPhaseId = toPositiveInteger(body?.phaseId || body?.generation?.phaseId || body?.baseMessage?.phaseId, null);

  if (explicitPhaseId) {
    const phase = planner.phases.find((candidate) => Number(candidate.id) === explicitPhaseId);

    if (!phase) {
      throw new Error(`phaseId ${explicitPhaseId} does not exist in planner.`);
    }

    if (isBehaviorPhaseCompleted(scenarioDocument, explicitPhaseId)) {
      return null;
    }

    return phase;
  }

  if (!processNextPending) {
    throw new Error('Behavior phase requires phaseId, or use aiPhase=next_behavior_phase.');
  }

  const completedPhaseIds = new Set(
    getBehaviorPhases(scenarioDocument)
      .filter((phase) => phase?.status === 'completed')
      .map((phase) => Number(phase.phaseId))
  );

  return planner.phases.find((phase) => !completedPhaseIds.has(Number(phase.id))) || null;
}

function ensureScenarioRuntimeFields(scenarioDocument, planner) {
  return {
    status: scenarioDocument?.status || 'Planner completed.',
    progress: scenarioDocument?.progress || buildProgress(planner, getBehaviorPhases(scenarioDocument)),
    scenario: simplifyScenario(getScenarioPlanner(scenarioDocument) || planner),
    signalKnowledge: scenarioDocument?.signalKnowledge || null,
    behavior: {
      phases: getBehaviorPhases(scenarioDocument)
    }
  };
}

function markBehaviorPhaseProcessing(scenarioDocument, planner, phaseId) {
  const totalPhases = planner.phases.length;
  const previousPhase = getBehaviorPhases(scenarioDocument).find((phase) => Number(phase?.phaseId) === Number(phaseId));
  const phases = upsertBehaviorPhaseStatus(getBehaviorPhases(scenarioDocument), {
    phaseId,
    status: 'processing',
    processingTimeMs: Number(previousPhase?.processingTimeMs || 0),
    signals: previousPhase?.signals || {},
    error: null
  });

  const updatedDocument = {
    status: `Generating behavior for phase ${phaseId} of ${totalPhases}.`,
    progress: buildProgress(planner, phases),
    processingTimeMs: getTotalProcessingTimeMs(scenarioDocument, phases),
    scenario: simplifyScenario(getScenarioPlanner(scenarioDocument) || planner),
    signalKnowledge: scenarioDocument?.signalKnowledge || null,
    behavior: {
      phases
    }
  };

  return updatedDocument;
}

function markBehaviorPhaseCompleted({ scenarioDocument, planner, phaseId, behaviorPhase, processingTimeMs }) {
  const completedPhase = {
    phaseId,
    status: 'completed',
    processingTimeMs: Math.max(0, Math.round(Number(processingTimeMs || 0))),
    signals: buildSignalBehaviorFromDeltas({
      scenarioDocument,
      phaseId,
      signalDeltas: behaviorPhase?.signals || {}
    }),
    error: null
  };

  const phases = upsertBehaviorPhaseStatus(getBehaviorPhases(scenarioDocument), completedPhase);
  const completedCount = phases.filter((phase) => phase?.status === 'completed').length;
  const totalPhases = planner.phases.length;
  const allCompleted = completedCount >= totalPhases;

  const updatedDocument = {
    status: allCompleted
      ? 'All behavior phases completed.'
      : `Behavior phase ${phaseId} completed. Waiting for next phase.`,
    progress: buildProgress(planner, phases),
    processingTimeMs: getTotalProcessingTimeMs(scenarioDocument, phases),
    scenario: simplifyScenario(getScenarioPlanner(scenarioDocument) || planner),
    signalKnowledge: scenarioDocument?.signalKnowledge || null,
    behavior: {
      phases
    }
  };

  return updatedDocument;
}

function markBehaviorPhaseFailed({ scenarioDocument, planner, phaseId, error, processingTimeMs }) {
  const rawModelResponse =
    error?.rawResponse ||
    error?.cause?.rawResponse ||
    error?.cause?.cause?.rawResponse ||
    null;

  const failedPhase = {
    phaseId,
    status: 'failed',
    processingTimeMs: Math.max(0, Math.round(Number(processingTimeMs || 0))),
    signals: {},
    error: {
      message: error?.message || 'Unknown error',
      validationErrors: error?.validationErrors || null,
      rawModelResponse
    }
  };

  const phases = upsertBehaviorPhaseStatus(getBehaviorPhases(scenarioDocument), failedPhase);

  const updatedDocument = {
    status: `Behavior generation failed while processing phase ${phaseId}.`,
    progress: buildProgress(planner, phases),
    processingTimeMs: getTotalProcessingTimeMs(scenarioDocument, phases),
    scenario: simplifyScenario(getScenarioPlanner(scenarioDocument) || planner),
    signalKnowledge: scenarioDocument?.signalKnowledge || null,
    behavior: {
      phases
    }
  };

  return updatedDocument;
}

function buildSignalBehaviorFromDeltas({ scenarioDocument, phaseId, signalDeltas }) {
  const result = {};

  if (!signalDeltas || typeof signalDeltas !== 'object' || Array.isArray(signalDeltas)) {
    return result;
  }

  for (const [signalName, deltas] of Object.entries(signalDeltas)) {
    if (!Array.isArray(deltas) || !deltas.length) {
      continue;
    }

    const start = resolveSignalStartValue({ scenarioDocument, phaseId, signalName });
    const d = normalizeSignalDeltas({ signalName, start, deltas });
    const end = roundSignalValue(start + d.reduce((total, value) => total + value, 0));

    result[signalName] = {
      start,
      end,
      d
    };
  }

  return result;
}

function normalizeSignalDeltas({ signalName, start, deltas }) {
  const bounds = resolveSignalBounds(signalName);
  const normalizedDeltas = [];
  let currentValue = clampSignalValue(start, bounds);

  for (const rawDelta of deltas) {
    const delta = Number(rawDelta);

    if (!Number.isFinite(delta)) {
      continue;
    }

    const requestedValue = currentValue + delta;
    const nextValue = clampSignalValue(requestedValue, bounds);
    const effectiveDelta = roundSignalValue(nextValue - currentValue);

    normalizedDeltas.push(effectiveDelta);
    currentValue = nextValue;
  }

  return normalizedDeltas.length ? normalizedDeltas : [0];
}

function resolveSignalBounds(signalName) {
  const name = String(signalName || '').toUpperCase();

  if (
    name.includes('SPEED') ||
    (name.includes('WHL') && name.includes('_W_')) ||
    (name.includes('WHEEL') && name.includes('SPEED'))
  ) {
    return { min: 0, max: 300 };
  }

  if (
    name.includes('APED') ||
    name.includes('ACCELERATOR') ||
    name.includes('PEDAL')
  ) {
    return { min: 0, max: 100 };
  }

  if (
    name.includes('BRAKE') ||
    name.includes('FRICTIONBRAKE') ||
    name.includes('REGEN')
  ) {
    return { min: 0, max: 100 };
  }

  if (
    name.includes('BATT_PERCENT') ||
    name.includes('BATTERY_PERCENT') ||
    name.includes('SOC') ||
    name.includes('FUEL')
  ) {
    return { min: 0, max: 100 };
  }

  if (
    name.includes('BATT_VOLT') ||
    name.includes('BATTERY_VOLT') ||
    name.includes('VOLTAGE')
  ) {
    return { min: 0, max: 1000 };
  }

  if (
    name.includes('TEMP') ||
    name.includes('TEMPERATURE')
  ) {
    return { min: -40, max: 200 };
  }

  if (
    name.includes('STEER') ||
    name.includes('STEWHL') ||
    name.includes('ANGLE')
  ) {
    return { min: -720, max: 720 };
  }

  if (
    name.includes('TORQUE')
  ) {
    return { min: -1000, max: 1000 };
  }

  if (
    name.includes('LONG_A') ||
    name.includes('LATA') ||
    name.includes('LAT_A') ||
    name.includes('YAW') ||
    name.includes('ROL') ||
    name.includes('PTCH')
  ) {
    return { min: -20, max: 20 };
  }

  if (
    name.includes('IGNITION') ||
    name.includes('ENABLE') ||
    name.includes('FAULT') ||
    name.includes('OPEN') ||
    name.includes('LIGHT') ||
    name.includes('CONNECTED') ||
    name.includes('WARNING') ||
    name.includes('HANDS') ||
    name.includes('ACTION') ||
    name.includes('ALERT')
  ) {
    return { min: 0, max: 1 };
  }

  return null;
}

function clampSignalValue(value, bounds) {
  const numericValue = roundSignalValue(value);

  if (!bounds) {
    return numericValue;
  }

  return roundSignalValue(Math.min(bounds.max, Math.max(bounds.min, numericValue)));
}

function resolveSignalStartValue({ scenarioDocument, phaseId, signalName }) {
  const initialSignalState = scenarioDocument?.scenario?.initialSignalState;

  if (
    Number(phaseId) === 1 &&
    initialSignalState &&
    typeof initialSignalState === 'object' &&
    !Array.isArray(initialSignalState) &&
    Object.prototype.hasOwnProperty.call(initialSignalState, signalName)
  ) {
    const initialValue = Number(initialSignalState[signalName]);

    if (Number.isFinite(initialValue)) {
      return roundSignalValue(initialValue);
    }
  }

  const previousPhases = getBehaviorPhases(scenarioDocument)
    .filter((phase) =>
      Number(phase?.phaseId) < Number(phaseId) &&
      phase?.status === 'completed' &&
      phase?.signals &&
      Object.prototype.hasOwnProperty.call(phase.signals, signalName)
    )
    .sort((a, b) => Number(b.phaseId) - Number(a.phaseId));

  for (const previousPhase of previousPhases) {
    const previousSignal = previousPhase.signals[signalName];

    if (previousSignal && typeof previousSignal === 'object' && !Array.isArray(previousSignal)) {
      const previousEnd = Number(previousSignal.end);

      if (Number.isFinite(previousEnd)) {
        return roundSignalValue(previousEnd);
      }
    }
  }

  return 0;
}

function roundSignalValue(value) {
  if (!Number.isFinite(Number(value))) {
    return 0;
  }

  return Number(Number(value).toFixed(6));
}

function upsertBehaviorPhaseStatus(phases, phaseUpdate) {
  const result = Array.isArray(phases) ? [...phases] : [];
  const index = result.findIndex((phase) => Number(phase?.phaseId) === Number(phaseUpdate.phaseId));

  if (index >= 0) {
    result[index] = { ...result[index], ...phaseUpdate };
  } else {
    result.push(phaseUpdate);
  }

  return result.sort((a, b) => Number(a.phaseId) - Number(b.phaseId));
}

function simplifyScenario(planner) {
  if (!planner || typeof planner !== 'object') {
    return null;
  }

  return {
    scenarioName: planner.scenarioName,
    summary: planner.summary,
    duration: planner.duration,
    sampleInterval: planner.sampleInterval,
    environment: planner.environment || null,
    initialSignalState: normalizeInitialSignalState(planner.initialSignalState),
    phases: Array.isArray(planner.phases)
      ? planner.phases.map((phase) => ({
          id: phase.id,
          name: phase.name,
          from: phase.from,
          to: phase.to,
          speedTarget: phase.speedTarget,
          driverIntent: phase.driverIntent,
          roadType: phase.roadType,
          traffic: phase.traffic
        }))
      : []
  };
}

function normalizeInitialSignalState(initialSignalState) {
  const result = {};

  if (!initialSignalState || typeof initialSignalState !== 'object' || Array.isArray(initialSignalState)) {
    return result;
  }

  for (const [signalName, rawValue] of Object.entries(initialSignalState)) {
    const safeSignalName = sanitizeSignalName(signalName);
    const numericValue = Number(rawValue);

    if (!safeSignalName || !Number.isFinite(numericValue)) {
      continue;
    }

    result[safeSignalName] = clampSignalValue(numericValue, resolveSignalBounds(safeSignalName));
  }

  return result;
}

function getTotalProcessingTimeMs(scenarioDocument, behaviorPhases = []) {
  const phaseTotal = Array.isArray(behaviorPhases)
    ? behaviorPhases.reduce((total, phase) => total + Math.max(0, Math.round(Number(phase?.processingTimeMs || 0))), 0)
    : 0;

  const currentTotal = Math.max(0, Math.round(Number(scenarioDocument?.processingTimeMs || 0)));

  return Math.max(currentTotal, phaseTotal);
}

function buildProgress(planner, behaviorPhases = []) {
  const totalPhases = Array.isArray(planner?.phases) ? planner.phases.length : 0;
  const completedPhases = behaviorPhases.filter((phase) => phase?.status === 'completed').length;
  const failedPhases = behaviorPhases.filter((phase) => phase?.status === 'failed').length;
  const processingPhases = behaviorPhases.filter((phase) => phase?.status === 'processing').length;
  const percent = totalPhases > 0 ? Math.round((completedPhases / totalPhases) * 100) : 0;

  return {
    totalPhases,
    completedPhases,
    failedPhases,
    processingPhases,
    percent
  };
}

function appendScenarioLog(logs, entry) {
  const safeLogs = Array.isArray(logs) ? logs : [];
  const logEntry = {
    at: new Date().toISOString(),
    level: entry.level || 'info',
    step: entry.step || 'unknown',
    phaseId: entry.phaseId ?? null,
    message: entry.message || '',
    validationErrors: entry.validationErrors || null
  };

  if (entry.rawModelResponse) {
    logEntry.rawModelResponse = entry.rawModelResponse;
  }

  const nextLogs = [
    ...safeLogs,
    logEntry
  ];

  return nextLogs.slice(-200);
}

function updateScenarioProgress(scenarioDocument, planner) {
  const phases = getBehaviorPhases(scenarioDocument);

  return {
    status: scenarioDocument?.status || 'Processing simulation scenario.',
    progress: buildProgress(planner, phases),
    processingTimeMs: getTotalProcessingTimeMs(scenarioDocument, phases),
    scenario: simplifyScenario(getScenarioPlanner(scenarioDocument) || planner),
    signalKnowledge: scenarioDocument?.signalKnowledge || null,
    behavior: {
      phases
    }
  };
}

async function tryWriteFailureToScenario(body, error) {
  try {
    const { bucket, key } = resolveScenarioLocation(body);
    if (!bucket || !key) return;

    const scenarioDocument = await safeReadJsonFromS3(bucket, key) || {};
    const planner = getScenarioPlanner(scenarioDocument) || { phases: [] };
    const phaseId = toPositiveInteger(body?.phaseId || body?.generation?.phaseId || body?.baseMessage?.phaseId, null);
    const phases = getBehaviorPhases(scenarioDocument);

    if (phaseId) {
      const failedPhase = {
        phaseId,
        status: 'failed',
        signals: {},
        error: {
          message: error?.message || 'Unknown error',
          validationErrors: error?.validationErrors || null,
          rawModelResponse:
            error?.rawResponse ||
            error?.cause?.rawResponse ||
            error?.cause?.cause?.rawResponse ||
            null
        }
      };

      const updatedPhases = upsertBehaviorPhaseStatus(phases, failedPhase);

      await putJsonToS3(bucket, key, {
        status: error?.message || 'Simulator AI assist failed.',
        progress: buildProgress(planner, updatedPhases),
        processingTimeMs: getTotalProcessingTimeMs(scenarioDocument, updatedPhases),
        scenario: simplifyScenario(getScenarioPlanner(scenarioDocument) || planner),
        signalKnowledge: scenarioDocument?.signalKnowledge || null,
        behavior: {
          phases: updatedPhases
        }
      });

      return;
    }

    await putJsonToS3(bucket, key, {
      status: error?.message || 'Simulator AI assist failed.',
      progress: buildProgress(planner, phases),
      processingTimeMs: getTotalProcessingTimeMs(scenarioDocument, phases),
      scenario: simplifyScenario(getScenarioPlanner(scenarioDocument) || planner),
      signalKnowledge: scenarioDocument?.signalKnowledge || null,
      behavior: {
        phases
      },
      error: {
        message: error?.message || 'Unknown error',
        validationErrors: error?.validationErrors || null,
        rawModelResponse:
          error?.rawResponse ||
          error?.cause?.rawResponse ||
          error?.cause?.cause?.rawResponse ||
          null
      }
    });
  } catch (writeError) {
    console.error('Unable to write failure status to scenario.json:', {
      message: writeError?.message
    });
  }
}

async function generateValidatedDrivingPlanner(body, signalKnowledge = null) {
  const plannerRequest = buildPlannerRequest(body);
  const requestedSignalMetadata = extractRequestedSignalMetadata(body);
  const promptTemplateResult = await loadPromptTemplate('PHASE_PLAN_PROMPT_KEY');
  const prompt = buildPlannerPrompt(promptTemplateResult.template, plannerRequest, requestedSignalMetadata, signalKnowledge);

  let lastError = null;
  let lastRawResponse = null;
  const attempts = Math.max(1, DEFAULT_PLANNER_RETRIES + 1);

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const effectivePrompt = attempt === 1 ? prompt : buildRetryPrompt(prompt, lastRawResponse, lastError);
      const mantleResponse = await callBedrockMantle(effectivePrompt);
      const responseText = extractTextFromMantleResponse(mantleResponse);
      lastRawResponse = responseText;

      const planner = parseJsonResponse(responseText);
      const validation = validateDrivingPlanner(
        planner,
        plannerRequest,
        requestedSignalMetadata.map((signal) => signal.name)
      );

      if (!validation.valid) {
        const validationError = new Error('Bedrock returned an invalid driving planner JSON.');
        validationError.validationErrors = validation.errors;
        validationError.rawResponse = responseText;
        throw validationError;
      }

      return {
        plannerRequest,
        planner,
        rawResponse: responseText,
        promptSource: promptTemplateResult.source,
        attempts: attempt
      };
    } catch (error) {
      if (!error.rawResponse && lastRawResponse) {
        error.rawResponse = lastRawResponse;
      }

      lastError = error;
      console.error('Driving planner attempt failed:', {
        attempt,
        maxAttempts: attempts,
        message: error?.message,
        validationErrors: error?.validationErrors || null
      });

      if (attempt >= attempts) {
        const finalError = new Error('Unable to generate a valid driving planner JSON.');
        finalError.validationErrors = error?.validationErrors || null;
        finalError.rawResponse = lastRawResponse;
        finalError.cause = error;
        throw finalError;
      }
    }
  }

  throw lastError || new Error('Unable to generate driving planner JSON.');
}

async function generateValidatedSignalBehaviorPlan({
  body,
  plannerRequest,
  planner,
  scenarioDocument,
  phaseId
}) {
  const requestedSignalMetadata = extractRequestedSignalMetadata(body);
  const requestedSignals = requestedSignalMetadata.map((signal) => signal.name);
  const currentSignalState = buildCurrentSignalState({
    scenarioDocument,
    phaseId,
    requestedSignals
  });

  if (!requestedSignals.length) {
    throw new Error('Unable to generate signal behavior plan because no signals were resolved from the request. Send signals, selectedSignals or signalNames in the body.');
  }

  const promptTemplateResult = await loadPromptTemplate('PHASE_BEHAVIOR_PROMPT_KEY');
  const prompt = buildSignalBehaviorPrompt({
    template: promptTemplateResult.template,
    request: plannerRequest,
    planner,
    requestedSignals,
    requestedSignalMetadata,
    currentSignalState
  });

  let lastError = null;
  let lastRawResponse = null;
  const attempts = Math.max(1, DEFAULT_BEHAVIOR_RETRIES + 1);

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const effectivePrompt = attempt === 1 ? prompt : buildRetryPrompt(prompt, lastRawResponse, lastError);
      const mantleResponse = await callBedrockMantle(effectivePrompt);
      const responseText = extractTextFromMantleResponse(mantleResponse);
      lastRawResponse = responseText;

      const behaviorPlan = parseJsonResponse(responseText);
      const validation = validateSignalBehaviorPlan({ behaviorPlan, planner, requestedSignals });

      if (!validation.valid) {
        const validationError = new Error('Bedrock returned an invalid signal behavior plan JSON.');
        validationError.validationErrors = validation.errors;
        validationError.rawResponse = responseText;
        throw validationError;
      }

      return {
        requestedSignals,
        behaviorPlan,
        rawResponse: responseText,
        promptSource: promptTemplateResult.source,
        attempts: attempt
      };
    } catch (error) {
      if (!error.rawResponse && lastRawResponse) {
        error.rawResponse = lastRawResponse;
      }

      lastError = error;
      console.error('Signal behavior attempt failed:', {
        attempt,
        maxAttempts: attempts,
        message: error?.message,
        validationErrors: error?.validationErrors || null
      });

      if (attempt >= attempts) {
        const finalError = new Error('Unable to generate a valid signal behavior plan JSON.');
        finalError.validationErrors = error?.validationErrors || null;
        finalError.rawResponse = lastRawResponse;
        finalError.cause = error;
        throw finalError;
      }
    }
  }

  throw lastError || new Error('Unable to generate signal behavior plan JSON.');
}

function buildRetryPrompt(originalPrompt, previousResponse, previousError) {
  const errorText = previousError?.validationErrors?.length
    ? previousError.validationErrors.join('; ')
    : previousError?.message || 'Invalid JSON response.';

  return [
    'Your previous response was invalid.',
    '',
    'Return ONLY valid JSON.',
    'Do not include markdown.',
    'Do not include explanations.',
    'Do not include comments.',
    'Do not include any text outside the JSON.',
    '',
    `Validation error: ${errorText}`,
    '',
    previousResponse ? `Previous invalid response:\n${previousResponse}` : '',
    '',
    'Generate the response again using the original instructions below.',
    '',
    originalPrompt
  ].filter(Boolean).join('\n');
}

async function savePlannerScenarioJson({ body, plannerResult, signalKnowledge = null, processingTimeMs }) {
  const generation = body?.generation || {};
  const baseMessage = generation?.baseMessage || body?.baseMessage || {};
  const bucket = resolveScenarioBucket(generation, baseMessage, body);
  const key = resolveScenarioKey(body, generation, baseMessage);

  if (!bucket) {
    throw new Error('Unable to save scenario.json because no S3 bucket was resolved.');
  }

  if (!key) {
    throw new Error('Unable to save scenario.json because no S3 key or output folder was resolved.');
  }

  const existingDocument = await safeReadJsonFromS3(bucket, key);
  const existingBehaviorPhases = getBehaviorPhases(existingDocument);

  const scenarioDocument = {
    status: 'Driving planner completed. Waiting for behavior phase generation.',
    progress: buildProgress(plannerResult.planner, existingBehaviorPhases),
    processingTimeMs,
    scenario: simplifyScenario(plannerResult.planner),
    signalKnowledge: signalKnowledge || existingDocument?.signalKnowledge || null,
    behavior: {
      phases: existingBehaviorPhases
    }
  };

  await putJsonToS3(bucket, key, scenarioDocument);

  return { bucket, key, fileName: 'scenario.json' };
}

async function readJsonFromS3(bucket, key) {
  const response = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));

  if (!response.Body) {
    throw new Error(`S3 object is empty or unreadable: s3://${bucket}/${key}`);
  }

  const text = await response.Body.transformToString('utf8');
  return JSON.parse(text);
}

async function putJsonToS3(bucket, key, document) {
  await s3Client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: JSON.stringify(document, null, 2),
      ContentType: 'application/json'
    })
  );
}

function buildPlannerRequest(body) {
  const generation = body?.generation || {};
  const baseMessage = generation?.baseMessage || body?.baseMessage || {};

  const durationSeconds =
    toPositiveInteger(body?.durationSeconds, null) ||
    toPositiveInteger(body?.amountOfTimeSeconds, null) ||
    toPositiveInteger(baseMessage?.durationSeconds, null) ||
    toPositiveInteger(baseMessage?.durationSec, null) ||
    hoursToSeconds(body?.amountOfTime) ||
    hoursToSeconds(baseMessage?.amountOfTime) ||
    1200;

  const sampleIntervalSeconds =
    toPositiveInteger(body?.sampleIntervalSeconds, null) ||
    toPositiveInteger(body?.latencyTime, null) ||
    toPositiveInteger(baseMessage?.sampleIntervalSeconds, null) ||
    toPositiveInteger(baseMessage?.intervalSec, null) ||
    toPositiveInteger(baseMessage?.latencyTime, null) ||
    5;

  const amountOfVehicles =
    toPositiveInteger(body?.vehicleVolume, null) ||
    toPositiveInteger(body?.amountOfVehicles, null) ||
    toPositiveInteger(baseMessage?.vehicleVolume, null) ||
    toPositiveInteger(baseMessage?.amountOfVehicles, null) ||
    (Array.isArray(generation?.vehicles) ? generation.vehicles.length : null) ||
    1;

  const speedValue = toOptionalNumber(body?.speed ?? body?.targetSpeed ?? baseMessage?.speed ?? baseMessage?.targetSpeed);
  const unit = sanitizeText(body?.unity || body?.distanceUnit || baseMessage?.unity || baseMessage?.distanceUnit) || 'Km';

  return {
    scenarioName: sanitizeText(body?.scenarioName || baseMessage?.scenarioName) || 'Trackster AI Simulation',
    requestedContext:
      sanitizeText(body?.requestedContext || baseMessage?.requestedContext) ||
      buildDefaultRequestedContext(body, baseMessage),
    driverProfile: sanitizeText(body?.driverProfile || baseMessage?.driverProfile) || 'Balanced',
    targetSpeed: Number.isFinite(speedValue) ? speedValue : null,
    targetSpeedText: Number.isFinite(speedValue) ? `${speedValue} ${unit}/h` : 'not specified',
    distanceUnit: unit,
    simulationMode: sanitizeText(body?.simulationMode || baseMessage?.simulationMode) || 'Time Window',
    generationType: sanitizeText(body?.generationType || baseMessage?.generationType) || 'all_at_once',
    routeRegion:
      sanitizeText(body?.routeRegion || body?.gpsArea || baseMessage?.routeRegion || baseMessage?.gpsArea) ||
      'not specified',
    initialDateTime: sanitizeText(body?.initialDateTime || baseMessage?.initialDateTime) || 'not specified',
    durationSeconds,
    sampleIntervalSeconds,
    vehicleVolume: amountOfVehicles
  };
}

function buildDefaultRequestedContext(body, baseMessage) {
  const regionText = sanitizeText(body?.routeRegion || body?.gpsArea || baseMessage?.routeRegion || baseMessage?.gpsArea);
  const amountOfTime = toPositiveInteger(body?.amountOfTime, null) || toPositiveInteger(baseMessage?.amountOfTime, null);
  const timeText = amountOfTime ? `${amountOfTime} hour vehicle driving simulation` : 'vehicle driving simulation';
  return regionText ? `Generate a realistic ${timeText} in ${regionText}.` : `Generate a realistic ${timeText}.`;
}

function loadPromptTemplateFromEnvironment(promptEnvName) {
  const template = sanitizeText(process.env[promptEnvName]);

  if (!template) {
    throw new Error(`${promptEnvName} environment variable was not defined.`);
  }

  return {
    template,
    source: {
      type: 'environment',
      variable: promptEnvName
    }
  };
}

async function loadPromptTemplate(keyEnvName) {
  const bucket = sanitizeText(process.env.PROMPT_BUCKET);

  if (!bucket) {
    throw new Error('PROMPT_BUCKET environment variable was not defined.');
  }

  let key = normalizeS3Key(process.env[keyEnvName]);

  if (keyEnvName === 'SIGNAL_KNOWLEDGE_PROMPT_KEY') {
    const configuredSignalKnowledgePrompt = normalizeS3Key(process.env.SIGNAL_KNOWLEDGE_PROMPT);

    if (!key && configuredSignalKnowledgePrompt && configuredSignalKnowledgePrompt.toLowerCase().endsWith('.txt')) {
      key = configuredSignalKnowledgePrompt;
    }

    if (!key || key === bucket || key.endsWith('/') || !key.toLowerCase().endsWith('.txt')) {
      key = DEFAULT_SIGNAL_KNOWLEDGE_PROMPT_KEY;
    }
  }

  if (!key) {
    throw new Error(`${keyEnvName} environment variable was not defined.`);
  }

  console.log('Loading AI prompt template from S3:', {
    keyEnvName,
    bucket,
    key
  });

  const response = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));

  if (!response.Body) {
    throw new Error(`Prompt file is empty or unreadable: s3://${bucket}/${key}`);
  }

  const template = await response.Body.transformToString('utf8');

  if (!template.trim()) {
    throw new Error(`Prompt file is empty: s3://${bucket}/${key}`);
  }

  console.log('AI prompt template loaded:', {
    keyEnvName,
    bucket,
    key,
    length: template.length
  });

  return { template, source: { bucket, key } };
}

function buildPlannerPrompt(template, request, requestedSignalMetadata = [], signalKnowledge = null) {
  return replaceCommonPromptVariables(template, request)
    .replaceAll('{{signalsJson}}', JSON.stringify(requestedSignalMetadata || [], null, 2))
    .replaceAll('{{signalKnowledgeJson}}', JSON.stringify(signalKnowledge || { schemaVersion: 'trackster-signal-knowledge-v1', signals: [] }, null, 2))
    .trim();
}

function buildSignalBehaviorPrompt({
  template,
  request,
  planner,
  requestedSignals,
  requestedSignalMetadata,
  currentSignalState
}) {
  const signalsForPrompt = Array.isArray(requestedSignalMetadata) && requestedSignalMetadata.length
    ? requestedSignalMetadata
    : requestedSignals;

  const plannerJson = JSON.stringify(planner, null, 2);
  const phaseObjective = buildPhaseObjectiveText(planner);

  return replaceCommonPromptVariables(template, request)
    .replaceAll('{{plannerJson}}', phaseObjective ? `${plannerJson}\n\n${phaseObjective}` : plannerJson)
    .replaceAll('{{signalsJson}}', JSON.stringify(signalsForPrompt, null, 2))
    .replaceAll('{{currentSignalStateJson}}', JSON.stringify(currentSignalState || {}, null, 2))
    .trim();
}

function buildCurrentSignalState({ scenarioDocument, phaseId, requestedSignals }) {
  const currentSignalState = {};

  if (!Array.isArray(requestedSignals)) {
    return currentSignalState;
  }

  for (const signalName of requestedSignals) {
    currentSignalState[signalName] = resolveSignalStartValue({
      scenarioDocument,
      phaseId,
      signalName
    });
  }

  return currentSignalState;
}

function buildPhaseObjectiveText(planner) {
  const phases = Array.isArray(planner?.phases) ? planner.phases : [];

  if (!phases.length) {
    return '';
  }

  const lines = [
    'Current phase objective:',
    ''
  ];

  for (const phase of phases) {
    lines.push(`Phase ${phase.id}:`);
    lines.push(`* Target vehicle speed: ${Number.isFinite(Number(phase.speedTarget)) ? Number(phase.speedTarget) : 'not specified'} km/h`);
    lines.push(`* Driver intent: ${sanitizeText(phase.driverIntent) || 'not specified'}`);
    lines.push(`* Road type: ${sanitizeText(phase.roadType) || 'not specified'}`);
    lines.push(`* Traffic: ${sanitizeText(phase.traffic) || 'not specified'}`);
    lines.push(`* Phase time window: ${Number.isFinite(Number(phase.from)) ? Number(phase.from) : 'not specified'}s to ${Number.isFinite(Number(phase.to)) ? Number(phase.to) : 'not specified'}s`);
    lines.push('');
    lines.push('For signals whose engineering unit is km/h, generate deltas that reconstruct physical speed values in km/h and are consistent with the target vehicle speed above.');
    lines.push('For cruising phases, km/h signals should move toward and remain near the target vehicle speed.');
    lines.push('For congestion or stop-and-go phases, km/h signals should oscillate below the target vehicle speed with realistic slowdowns and stops.');
    lines.push('For parking phases, km/h signals should remain low and progressively reach zero when appropriate.');
    lines.push('');
  }

  return lines.join('\n').trim();
}

function replaceCommonPromptVariables(template, request) {
  return template
    .replaceAll('{{scenarioName}}', request.scenarioName)
    .replaceAll('{{requestedContext}}', request.requestedContext)
    .replaceAll('{{driverProfile}}', request.driverProfile)
    .replaceAll('{{targetSpeed}}', request.targetSpeedText)
    .replaceAll('{{simulationMode}}', request.simulationMode || 'not specified')
    .replaceAll('{{generationType}}', request.generationType || 'not specified')
    .replaceAll('{{routeRegion}}', request.routeRegion || 'not specified')
    .replaceAll('{{initialDateTime}}', request.initialDateTime || 'not specified')
    .replaceAll('{{durationSeconds}}', String(request.durationSeconds))
    .replaceAll('{{sampleIntervalSeconds}}', String(request.sampleIntervalSeconds))
    .replaceAll('{{vehicleVolume}}', String(request.vehicleVolume));
}

function extractRequestedSignals(body) {
  return extractRequestedSignalMetadata(body).map((signal) => signal.name);
}

function extractRequestedSignalMetadata(body) {
  const generation = body?.generation || {};
  const baseMessage = generation?.baseMessage || body?.baseMessage || {};

  const candidateCollections = [
    body?.signals,
    body?.selectedSignals,
    body?.signalNames,
    generation?.signals,
    generation?.selectedSignals,
    generation?.signalNames,
    baseMessage?.signals,
    baseMessage?.selectedSignals,
    baseMessage?.signalNames
  ];

  const signals = [];

  for (const collection of candidateCollections) {
    if (!Array.isArray(collection)) {
      continue;
    }

    for (const item of collection) {
      const signalName = resolveSignalName(item);

      if (signalName) {
        signals.push(buildRequestedSignalMetadata(item, signalName));
      }
    }
  }

  return uniqueSignalMetadata(signals);
}

function resolveSignalName(item) {
  if (typeof item === 'string') {
    return sanitizeSignalName(item);
  }

  if (!item || typeof item !== 'object' || Array.isArray(item)) {
    return '';
  }

  return sanitizeSignalName(item.signalName || item.name || item.signal || item.canSignalName || item.label);
}

function resolveSignalUnit(item) {
  if (!item || typeof item !== 'object' || Array.isArray(item)) {
    return '';
  }

  return sanitizeEngineeringUnit(
    item.unit ||
    item.units ||
    item.signalUnit ||
    item.engineeringUnit ||
    item.physicalUnit ||
    item.measurementUnit ||
    item.unitName ||
    item.displayUnit
  );
}

function buildRequestedSignalMetadata(item, signalName) {
  const result = { name: signalName };
  const unit = resolveSignalUnit(item);

  if (unit) result.unit = unit;

  if (item && typeof item === 'object' && !Array.isArray(item)) {
    const messageName = sanitizeText(
      item.messageName ||
        item.frameName ||
        item.canMessageName ||
        item.message?.name ||
        item.frame?.n ||
        item.frame?.name
    );
    if (messageName) result.messageName = messageName;

    const canId = sanitizeText(item.canId || item.canID || item.id || item.frameId || item.messageId);
    if (canId) result.canId = canId;

    const dbcFile = sanitizeText(item.dbcFile || item.dbc || item.sourceDbc || item.dbcName);
    if (dbcFile) result.dbcFile = dbcFile;

    const physicalMin = firstFiniteNumber(item.mn, item.physicalMin, item.min, item.minimum);
    if (Number.isFinite(physicalMin)) result.mn = physicalMin;

    const physicalMax = firstFiniteNumber(item.mx, item.physicalMax, item.max, item.maximum);
    if (Number.isFinite(physicalMax)) result.mx = physicalMax;

    const factor = firstFiniteNumber(item.f, item.factor);
    if (Number.isFinite(factor)) result.f = factor;

    const offset = firstFiniteNumber(item.o, item.offset);
    if (Number.isFinite(offset)) result.o = offset;

    const bitLength = firstPositiveInteger(item.b, item.bitLength, item.length);
    if (Number.isInteger(bitLength)) result.b = bitLength;
  }

  return result;
}

function sanitizeEngineeringUnit(value) {
  const text = sanitizeText(value);

  if (!text || text === '-' || text.toLowerCase() === 'none' || text.toLowerCase() === 'n/a') {
    return '';
  }

  return text.slice(0, 40);
}

function uniqueSignalMetadata(values) {
  const seen = new Set();
  const result = [];

  for (const value of values) {
    if (!value?.name || seen.has(value.name)) {
      continue;
    }

    seen.add(value.name);

    const item = { name: value.name };
    if (value.messageName) item.messageName = value.messageName;
    if (value.canId) item.canId = value.canId;
    if (value.dbcFile) item.dbcFile = value.dbcFile;
    if (value.unit) item.unit = value.unit;
    if (Number.isFinite(Number(value.mn))) item.mn = Number(value.mn);
    if (Number.isFinite(Number(value.mx))) item.mx = Number(value.mx);
    if (Number.isFinite(Number(value.f))) item.f = Number(value.f);
    if (Number.isFinite(Number(value.o))) item.o = Number(value.o);
    if (Number.isInteger(Number(value.b))) item.b = Number(value.b);

    result.push(item);
  }

  return result;
}

function firstFiniteNumber(...values) {
  for (const value of values) {
    if (value === null || value === undefined || value === '') {
      continue;
    }

    const parsed = Number(value);

    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return null;
}

function firstPositiveInteger(...values) {
  for (const value of values) {
    const parsed = Number.parseInt(value, 10);

    if (Number.isInteger(parsed) && parsed > 0) {
      return parsed;
    }
  }

  return null;
}


function sanitizeSignalName(value) {
  const text = sanitizeText(value);
  return text ? text.replace(/[^\w.-]/g, '_').slice(0, 120) : '';
}

function uniqueStrings(values) {
  const seen = new Set();
  const result = [];

  for (const value of values) {
    if (!value || seen.has(value)) {
      continue;
    }
    seen.add(value);
    result.push(value);
  }

  return result;
}

async function callBedrockMantle(prompt) {
  const requestBody = JSON.stringify({
    model: modelId,
    messages: [{ role: 'user', content: prompt }],
    temperature: 0.2,
    top_p: 0.9,
    max_tokens: DEFAULT_MAX_TOKENS
  });

  const unsignedRequest = new HttpRequest({
    protocol: 'https:',
    hostname: mantleHost,
    method: 'POST',
    path: mantlePath,
    headers: { host: mantleHost, 'content-type': 'application/json' },
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

function parseJsonResponse(text) {
  try {
    return JSON.parse(text);
  } catch (error) {
    const firstBrace = text.indexOf('{');

    if (firstBrace < 0) {
      const parseError = new Error('Bedrock response is not valid JSON.');
      parseError.rawResponse = text;
      parseError.cause = error;
      throw parseError;
    }

    let depth = 0;
    let inString = false;
    let escaped = false;

    for (let index = firstBrace; index < text.length; index += 1) {
      const character = text[index];

      if (escaped) {
        escaped = false;
        continue;
      }

      if (character === '\\') {
        escaped = true;
        continue;
      }

      if (character === '"') {
        inString = !inString;
        continue;
      }

      if (inString) {
        continue;
      }

      if (character === '{') {
        depth += 1;
      } else if (character === '}') {
        depth -= 1;
      }

      if (depth === 0) {
        try {
          return JSON.parse(text.slice(firstBrace, index + 1));
        } catch (innerError) {
          const parseError = new Error('Bedrock response JSON extraction failed.');
          parseError.rawResponse = text;
          parseError.cause = innerError;
          throw parseError;
        }
      }
    }

    const parseError = new Error('Bedrock response JSON extraction failed.');
    parseError.rawResponse = text;
    parseError.cause = error;
    throw parseError;
  }
}

function validateDrivingPlanner(planner, request, requestedSignals = []) {
  const errors = [];

  if (!planner || typeof planner !== 'object' || Array.isArray(planner)) {
    errors.push('Planner response must be an object.');
    return { valid: false, errors };
  }

  if (!isNonEmptyString(planner.scenarioName)) errors.push('scenarioName must be a non-empty string.');
  if (!isNonEmptyString(planner.summary)) errors.push('summary must be a non-empty string.');
  if (Number(planner.duration) !== Number(request.durationSeconds)) errors.push(`duration must be exactly ${request.durationSeconds}.`);
  if (Number(planner.sampleInterval) !== Number(request.sampleIntervalSeconds)) errors.push(`sampleInterval must be exactly ${request.sampleIntervalSeconds}.`);

  if (!planner.initialSignalState || typeof planner.initialSignalState !== 'object' || Array.isArray(planner.initialSignalState)) {
    errors.push('initialSignalState must be an object with numeric initial values by signal name.');
  } else {
    const initialSignalState = normalizeInitialSignalState(planner.initialSignalState);
    const initialSignalNames = Object.keys(initialSignalState);
    const extraInitialSignals = Array.isArray(requestedSignals) && requestedSignals.length
      ? initialSignalNames.filter((signalName) => !requestedSignals.includes(signalName))
      : [];
    const protocolInitialSignals = initialSignalNames.filter((signalName) => isOmittableProtocolSignal(signalName));

    if (!initialSignalNames.length) {
      errors.push('initialSignalState must contain at least one numeric signal value.');
    }

    if (extraInitialSignals.length) {
      errors.push(`initialSignalState contains extra signals: ${extraInitialSignals.join(', ')}.`);
    }

    if (protocolInitialSignals.length) {
      errors.push(`initialSignalState must omit protocol signals: ${protocolInitialSignals.join(', ')}.`);
    }
  }

  if (!planner.environment || typeof planner.environment !== 'object' || Array.isArray(planner.environment)) {
    errors.push('environment must be an object.');
  } else {
    if (!isNonEmptyString(planner.environment.country)) errors.push('environment.country must be a non-empty string.');
    if (!isNonEmptyString(planner.environment.weather)) errors.push('environment.weather must be a non-empty string.');
    if (!isNonEmptyString(planner.environment.roadCondition)) errors.push('environment.roadCondition must be a non-empty string.');
  }

  if (!Array.isArray(planner.phases) || !planner.phases.length) {
    errors.push('phases must be a non-empty array.');
    return { valid: false, errors };
  }

  let expectedFrom = 0;

  for (let index = 0; index < planner.phases.length; index += 1) {
    const phase = planner.phases[index];

    if (!phase || typeof phase !== 'object' || Array.isArray(phase)) {
      errors.push(`phases[${index}] must be an object.`);
      continue;
    }

    validateRequiredString(errors, phase, 'name', index);
    validateRequiredString(errors, phase, 'description', index);
    validateRequiredString(errors, phase, 'driverIntent', index);
    validateRequiredString(errors, phase, 'roadType', index);
    validateRequiredString(errors, phase, 'traffic', index);
    validateRequiredString(errors, phase, 'weather', index);
    validateRequiredString(errors, phase, 'roadCondition', index);
    validateRequiredString(errors, phase, 'expectedTurns', index);

    const id = Number(phase.id);
    const from = Number(phase.from);
    const to = Number(phase.to);
    const speedTarget = Number(phase.speedTarget);
    const expectedStops = Number(phase.expectedStops);
    const aggressiveness = Number(phase.aggressiveness);

    if (!Number.isInteger(id) || id !== index + 1) errors.push(`phases[${index}].id must be ${index + 1}.`);
    if (!Number.isInteger(from)) errors.push(`phases[${index}].from must be a whole number.`);
    if (!Number.isInteger(to)) errors.push(`phases[${index}].to must be a whole number.`);
    if (Number.isInteger(from) && from !== expectedFrom) errors.push(`phases[${index}].from must be ${expectedFrom}.`);
    if (Number.isInteger(from) && Number.isInteger(to) && to <= from) errors.push(`phases[${index}].to must be greater than from.`);
    if (Number.isInteger(from) && Number.isInteger(to)) expectedFrom = to;
    if (!Number.isFinite(speedTarget) || speedTarget < 0) errors.push(`phases[${index}].speedTarget must be a number greater than or equal to 0.`);
    if (!Number.isInteger(expectedStops) || expectedStops < 0) errors.push(`phases[${index}].expectedStops must be an integer greater than or equal to 0.`);
    if (!Number.isFinite(aggressiveness) || aggressiveness < 0 || aggressiveness > 1) errors.push(`phases[${index}].aggressiveness must be between 0.0 and 1.0.`);
  }

  if (expectedFrom !== Number(request.durationSeconds)) errors.push(`Final phase must end exactly at ${request.durationSeconds}.`);

  const finalPhase = planner.phases[planner.phases.length - 1];
  if (finalPhase && Number(finalPhase.speedTarget) !== 0) errors.push('Final phase must have speedTarget equal to 0.');

  return { valid: errors.length === 0, errors };
}

function isOmittableProtocolSignal(signalName) {
  const name = String(signalName || '').toUpperCase();

  return (
    name.includes('COUNTER') ||
    name.includes('ROLLING') ||
    name.includes('CHECKSUM') ||
    name.includes('CRC') ||
    name.includes('RESERVED') ||
    name.includes('CALIBRATION') ||
    name.includes('CONFIG') ||
    name.includes('PROTOCOL')
  );
}

function validateSignalBehaviorPlan({ behaviorPlan, planner, requestedSignals }) {
  const errors = [];

  if (!behaviorPlan || typeof behaviorPlan !== 'object' || Array.isArray(behaviorPlan)) {
    errors.push('Signal behavior plan response must be an object.');
    return { valid: false, errors };
  }

  if (behaviorPlan.schemaVersion !== 'trackster-signal-delta-plan-v1') {
    errors.push('schemaVersion must be trackster-signal-delta-plan-v1.');
  }

  if (!Array.isArray(behaviorPlan.phases)) {
    errors.push('phases must be an array.');
    return { valid: false, errors };
  }

  if (behaviorPlan.phases.length !== planner.phases.length) {
    errors.push(`phases length must be exactly ${planner.phases.length}.`);
  }

  const plannerPhaseById = new Map(planner.phases.map((phase) => [Number(phase.id), phase]));
  const seenPhaseIds = new Set();

  for (let index = 0; index < behaviorPlan.phases.length; index += 1) {
    const behaviorPhase = behaviorPlan.phases[index];

    if (!behaviorPhase || typeof behaviorPhase !== 'object' || Array.isArray(behaviorPhase)) {
      errors.push(`phases[${index}] must be an object.`);
      continue;
    }

    const phaseId = Number(behaviorPhase.phaseId);
    if (!Number.isInteger(phaseId)) {
      errors.push(`phases[${index}].phaseId must be an integer.`);
      continue;
    }

    if (seenPhaseIds.has(phaseId)) errors.push(`phaseId ${phaseId} is duplicated.`);
    seenPhaseIds.add(phaseId);
    if (!plannerPhaseById.has(phaseId)) errors.push(`phaseId ${phaseId} does not exist in planner.`);

    if (!behaviorPhase.signals || typeof behaviorPhase.signals !== 'object' || Array.isArray(behaviorPhase.signals)) {
      errors.push(`phases[${index}].signals must be an object.`);
      continue;
    }

    const signalNames = Object.keys(behaviorPhase.signals);
    const extraSignals = signalNames.filter((signalName) => !requestedSignals.includes(signalName));
    const protocolSignals = signalNames.filter((signalName) => isOmittableProtocolSignal(signalName));

    if (extraSignals.length) errors.push(`phases[${index}] contains extra signals: ${extraSignals.join(', ')}.`);
    if (protocolSignals.length) errors.push(`phases[${index}] must omit protocol signals: ${protocolSignals.join(', ')}.`);

    for (const signalName of signalNames) {
      const signalDeltas = behaviorPhase.signals[signalName];

      if (!Array.isArray(signalDeltas) || !signalDeltas.length) {
        errors.push(`phases[${index}].signals.${signalName} must be a non-empty numeric delta array.`);
        continue;
      }

      for (let deltaIndex = 0; deltaIndex < signalDeltas.length; deltaIndex += 1) {
        if (!Number.isFinite(Number(signalDeltas[deltaIndex]))) {
          errors.push(`phases[${index}].signals.${signalName}[${deltaIndex}] must be numeric.`);
        }
      }
    }
  }

  for (const plannerPhase of planner.phases) {
    if (!seenPhaseIds.has(Number(plannerPhase.id))) {
      errors.push(`planner phase ${plannerPhase.id} is missing from signal behavior plan.`);
    }
  }

  return {
    valid: errors.length === 0,
    errors
  };
}

function validateRequiredString(errors, phase, fieldName, index) {
  if (!isNonEmptyString(phase[fieldName])) {
    errors.push(`phases[${index}].${fieldName} must be a non-empty string.`);
  }
}

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

function resolveScenarioBucket(generation, baseMessage, body) {
  return sanitizeText(
    generation?.scenarioBucketName ||
      generation?.bucketName ||
      generation?.outputBucketName ||
      generation?.rawBucketName ||
      body?.scenarioBucketName ||
      body?.bucketName ||
      body?.s3Bucket ||
      body?.outputBucketName ||
      body?.rawBucketName ||
      body?.s3BucketName ||
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
      body?.scenarioKey ||
      body?.scenarioJsonKey ||
      baseMessage?.scenarioKey ||
      baseMessage?.scenarioJsonKey
  );

  if (explicitKey) return normalizeS3Key(explicitKey);

  const existingArtifactKey = sanitizeText(
    generation?.manifestKey ||
      generation?.runManifestKey ||
      generation?.binKey ||
      generation?.s3Key ||
      generation?.objectKey ||
      body?.manifestKey ||
      body?.runManifestKey ||
      body?.binKey ||
      body?.s3Key ||
      body?.objectKey ||
      baseMessage?.manifestKey ||
      baseMessage?.runManifestKey ||
      baseMessage?.binKey ||
      baseMessage?.s3Key ||
      baseMessage?.objectKey
  );

  const artifactFolder = extractFolderFromS3Key(existingArtifactKey);
  if (artifactFolder) return `${normalizeS3Prefix(artifactFolder)}scenario.json`;

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
      body?.outputPrefix ||
      body?.s3Prefix ||
      body?.prefix ||
      body?.folderKey ||
      body?.folderPath ||
      body?.runPrefix ||
      body?.runFolder ||
      body?.outputFolder ||
      body?.destinationPrefix ||
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

  if (folder) return `${normalizeS3Prefix(folder)}scenario.json`;

  return `${normalizeS3Prefix(DEFAULT_TEST_OUTPUT_PREFIX)}scenario.json`;
}

function extractFolderFromS3Key(key) {
  const normalized = normalizeS3Key(key);
  if (!normalized) return '';
  const lastSlashIndex = normalized.lastIndexOf('/');
  return lastSlashIndex < 0 ? '' : normalized.slice(0, lastSlashIndex + 1);
}

function normalizeS3Prefix(value) {
  const normalized = normalizeS3Key(value);
  if (!normalized) return '';
  return normalized.endsWith('/') ? normalized : `${normalized}/`;
}

function normalizeS3Key(value) {
  return String(value || '').trim().replace(/^\/+/g, '').replace(/\/{2,}/g, '/');
}

function parseBody(rawBody) {
  if (!rawBody) return {};
  if (typeof rawBody === 'object') return rawBody;
  return JSON.parse(rawBody);
}

function sanitizeText(value) {
  if (typeof value !== 'string') return '';
  return value.trim().slice(0, 2000);
}

function toPositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return !Number.isFinite(parsed) || parsed <= 0 ? fallback : parsed;
}

function toOptionalNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function hoursToSeconds(value) {
  const parsed = Number(value);
  return !Number.isFinite(parsed) || parsed <= 0 ? null : Math.round(parsed * 3600);
}

function buildResponse(statusCode, body) {
  return { statusCode, headers: defaultHeaders, body: JSON.stringify(body) };
}
