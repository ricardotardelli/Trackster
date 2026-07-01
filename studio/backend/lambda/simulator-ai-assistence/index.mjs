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
        : 'planner'
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
        progress: autoResult.progress,
        currentStep: autoResult.currentStep
      });
    }

    if (aiPhase === 'planner' || aiPhase === 'phase1' || aiPhase === 'plan') {
      const plannerResult = await generateValidatedDrivingPlanner(body);
      const scenarioS3 = await savePlannerScenarioJson({
        body,
        plannerResult,
        processingTimeMs: Date.now() - startedAt
      });
      const processingTimeMs = Date.now() - startedAt;

      return buildResponse(200, {
        success: true,
        status: 'planner_saved',
        aiPhase: 'planner',
        processingTimeMs,
        modelId,
        scenarioS3,
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

    if (aiPhase === 'behavior' || aiPhase === 'phase2' || aiPhase === 'behaviour' || aiPhase === 'next_behavior_phase') {
      const behaviorResult = await runBehaviorPhase(body, { processNextPending: aiPhase === 'next_behavior_phase' });
      const processingTimeMs = Date.now() - startedAt;

      return buildResponse(200, {
        success: true,
        status: behaviorResult.status,
        aiPhase: 'behavior',
        processingTimeMs,
        modelId,
        scenarioS3: behaviorResult.scenarioS3,
        progress: behaviorResult.progress,
        currentStep: behaviorResult.currentStep,
        phaseId: behaviorResult.phaseId,
        phaseStatus: behaviorResult.phaseStatus,
        signalBehaviorPhase: behaviorResult.signalBehaviorPhase || null
      });
    }

    return buildResponse(400, {
      success: false,
      message: 'Invalid aiPhase. Use auto, planner, status, behavior or next_behavior_phase.'
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
  const { bucket, key } = resolveScenarioLocation(body);

  if (!bucket) {
    throw new Error('Unable to run auto AI pipeline because no S3 bucket was resolved.');
  }

  if (!key) {
    throw new Error('Unable to run auto AI pipeline because no S3 key or output folder was resolved.');
  }

  await createInitialScenarioJson({ body, bucket, key });

  const plannerResult = await generateValidatedDrivingPlanner(body);
  const scenarioS3 = await savePlannerScenarioJson({
    body,
    plannerResult,
    processingTimeMs: Date.now() - startedAt
  });

  const planner = plannerResult.planner;
  const totalPhases = Array.isArray(planner?.phases) ? planner.phases.length : 0;

  for (const phase of planner.phases) {
    await runBehaviorPhase(
      {
        ...body,
        phaseId: Number(phase.id),
        scenarioBucketName: bucket,
        scenarioKey: key
      },
      { processNextPending: false }
    );
  }

  let scenarioDocument = await readJsonFromS3(bucket, key);
  scenarioDocument.status = 'ai_behavior_completed';
  scenarioDocument.currentStep = 'Trackster AI completed the simulation behavior plan.';
  scenarioDocument.updatedAt = new Date().toISOString();
  scenarioDocument.progress = scenarioDocument.progress || {};
  scenarioDocument.progress.totalPhases = totalPhases;
  scenarioDocument.progress.completedPhases = totalPhases;
  scenarioDocument.progress.failedPhases = 0;
  scenarioDocument.progress.processingPhases = 0;
  scenarioDocument.progress.percent = 100;
  scenarioDocument.logs = appendScenarioLog(scenarioDocument.logs, {
    level: 'info',
    step: 'auto',
    message: 'Auto AI pipeline completed.'
  });

  await putJsonToS3(bucket, key, scenarioDocument);

  return {
    scenarioS3,
    status: scenarioDocument.status,
    progress: scenarioDocument.progress,
    currentStep: scenarioDocument.currentStep
  };
}

async function createInitialScenarioJson({ body, bucket, key }) {
  const now = new Date().toISOString();
  const requestedSignals = extractRequestedSignals(body);
  const plannerRequest = buildPlannerRequest(body);

  const scenarioDocument = {
    schemaVersion: 'trackster-ai-scenario-file-v1',
    generatedAt: now,
    updatedAt: now,
    source: 'trackster-simulator-ai-assist',
    lambdaName: process.env.AWS_LAMBDA_FUNCTION_NAME || 'trackster-simulator-ai-assist',
    modelId,
    status: 'planner_processing',
    currentStep: 'Trackster AI is preparing the simulation scenario.',
    requestId: sanitizeText(body?.requestId),
    runId: sanitizeText(body?.runId),
    customerId: sanitizeText(body?.customerId || body?.clientId),
    clientId: sanitizeText(body?.clientId || body?.customerId),
    progress: {
      totalPhases: 0,
      completedPhases: 0,
      failedPhases: 0,
      processingPhases: 0,
      percent: 0
    },
    logs: appendScenarioLog([], {
      level: 'info',
      step: 'auto',
      message: 'Auto AI pipeline started.'
    }),
    planner: {
      schemaVersion: 'trackster-driving-planner-v1',
      request: plannerRequest,
      result: null
    },
    signalBehaviorPlan: {
      schemaVersion: 'trackster-signal-delta-plan-v1',
      requestedSignals,
      result: {
        schemaVersion: 'trackster-signal-delta-plan-v1',
        phases: []
      }
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

async function runBehaviorPhase(body, options = {}) {
  const { bucket, key } = resolveScenarioLocation(body);

  if (!bucket) {
    throw new Error('Unable to read scenario.json because no S3 bucket was resolved.');
  }

  if (!key) {
    throw new Error('Unable to read scenario.json because no S3 key or output folder was resolved.');
  }

  let scenarioDocument = await readJsonFromS3(bucket, key);
  const planner = scenarioDocument?.planner?.result;
  const plannerRequest = scenarioDocument?.planner?.request || buildPlannerRequest(body);

  if (!planner || !Array.isArray(planner.phases) || !planner.phases.length) {
    throw new Error('scenario.json does not contain planner.result.phases. Run aiPhase=planner first.');
  }

  const requestedSignals = resolveRequestedSignalsForBehavior(body, scenarioDocument);

  if (!requestedSignals.length) {
    throw new Error('Unable to generate signal behavior plan because no signals were resolved from the request or scenario.json. Send signals, selectedSignals or signalNames in the body.');
  }

  scenarioDocument = ensureScenarioRuntimeFields(scenarioDocument, planner, requestedSignals);

  const phase = resolveBehaviorPhaseToProcess({
    body,
    planner,
    scenarioDocument,
    processNextPending: options.processNextPending
  });

  if (!phase) {
    scenarioDocument = updateScenarioProgress(scenarioDocument, planner);
    scenarioDocument.status = 'behavior_completed';
    scenarioDocument.currentStep = 'All behavior phases completed.';
    scenarioDocument.updatedAt = new Date().toISOString();
    scenarioDocument.logs = appendScenarioLog(scenarioDocument.logs, {
      level: 'info',
      step: 'behavior',
      message: 'All behavior phases were already completed.'
    });

    await putJsonToS3(bucket, key, scenarioDocument);

    return {
      scenarioS3: { bucket, key, fileName: 'scenario.json' },
      status: scenarioDocument.status,
      progress: scenarioDocument.progress,
      currentStep: scenarioDocument.currentStep,
      phaseId: null,
      phaseStatus: 'completed',
      signalBehaviorPhase: null
    };
  }

  scenarioDocument = markBehaviorPhaseProcessing(scenarioDocument, planner, phase.id);
  await putJsonToS3(bucket, key, scenarioDocument);

  try {
    const singlePhasePlanner = {
      ...planner,
      phases: [phase]
    };

    const behaviorGeneration = await generateValidatedSignalBehaviorPlan({
      body: { ...body, signals: requestedSignals },
      plannerRequest,
      planner: singlePhasePlanner
    });

    const behaviorPhase = behaviorGeneration.behaviorPlan.phases[0];
    scenarioDocument = await readJsonFromS3(bucket, key);
    scenarioDocument = ensureScenarioRuntimeFields(scenarioDocument, planner, requestedSignals);
    scenarioDocument = markBehaviorPhaseCompleted({
      scenarioDocument,
      planner,
      phaseId: phase.id,
      behaviorPhase,
      behaviorGeneration
    });

    await putJsonToS3(bucket, key, scenarioDocument);

    return {
      scenarioS3: { bucket, key, fileName: 'scenario.json' },
      status: scenarioDocument.status,
      progress: scenarioDocument.progress,
      currentStep: scenarioDocument.currentStep,
      phaseId: phase.id,
      phaseStatus: 'completed',
      signalBehaviorPhase: behaviorPhase
    };
  } catch (error) {
    scenarioDocument = await safeReadScenarioDocument(bucket, key, scenarioDocument);
    scenarioDocument = ensureScenarioRuntimeFields(scenarioDocument, planner, requestedSignals);
    scenarioDocument = markBehaviorPhaseFailed({ scenarioDocument, planner, phaseId: phase.id, error });
    await putJsonToS3(bucket, key, scenarioDocument);
    throw error;
  }
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

  const scenarioSignals = scenarioDocument?.signalBehaviorPlan?.requestedSignals;
  if (Array.isArray(scenarioSignals)) return uniqueStrings(scenarioSignals.map((signal) => sanitizeSignalName(signal)));

  return [];
}

function resolveBehaviorPhaseToProcess({ body, planner, scenarioDocument, processNextPending }) {
  const explicitPhaseId = toPositiveInteger(body?.phaseId || body?.generation?.phaseId || body?.baseMessage?.phaseId, null);

  if (explicitPhaseId) {
    const phase = planner.phases.find((candidate) => Number(candidate.id) === explicitPhaseId);

    if (!phase) {
      throw new Error(`phaseId ${explicitPhaseId} does not exist in planner.`);
    }

    return phase;
  }

  if (!processNextPending) {
    throw new Error('Behavior phase requires phaseId, or use aiPhase=next_behavior_phase.');
  }

  const completedPhaseIds = new Set(
    (scenarioDocument?.signalBehaviorPlan?.result?.phases || [])
      .filter((phase) => phase?.status === 'completed')
      .map((phase) => Number(phase.phaseId))
  );

  return planner.phases.find((phase) => !completedPhaseIds.has(Number(phase.id))) || null;
}

function ensureScenarioRuntimeFields(scenarioDocument, planner, requestedSignals = []) {
  const now = new Date().toISOString();

  const existingBehaviorPlan = scenarioDocument?.signalBehaviorPlan || {};
  const existingResult = existingBehaviorPlan?.result || {};

  return {
    ...scenarioDocument,
    updatedAt: now,
    status: scenarioDocument?.status || 'planner_saved',
    currentStep: scenarioDocument?.currentStep || 'Planner completed.',
    progress: scenarioDocument?.progress || buildProgress(planner, existingResult.phases || []),
    logs: Array.isArray(scenarioDocument?.logs) ? scenarioDocument.logs : [],
    signalBehaviorPlan: {
      schemaVersion: 'trackster-signal-delta-plan-v1',
      ...existingBehaviorPlan,
      requestedSignals: requestedSignals.length ? requestedSignals : existingBehaviorPlan.requestedSignals || [],
      result: {
        schemaVersion: 'trackster-signal-delta-plan-v1',
        ...existingResult,
        phases: Array.isArray(existingResult.phases) ? existingResult.phases : []
      }
    }
  };
}

function markBehaviorPhaseProcessing(scenarioDocument, planner, phaseId) {
  const totalPhases = planner.phases.length;
  const phases = upsertBehaviorPhaseStatus(scenarioDocument.signalBehaviorPlan.result.phases, {
    phaseId,
    status: 'processing',
    startedAt: new Date().toISOString()
  });

  const updatedDocument = {
    ...scenarioDocument,
    status: 'behavior_processing',
    currentStep: `Generating behavior for phase ${phaseId} of ${totalPhases}.`,
    updatedAt: new Date().toISOString(),
    signalBehaviorPlan: {
      ...scenarioDocument.signalBehaviorPlan,
      result: {
        ...scenarioDocument.signalBehaviorPlan.result,
        phases
      }
    }
  };

  updatedDocument.progress = buildProgress(planner, phases);
  updatedDocument.logs = appendScenarioLog(updatedDocument.logs, {
    level: 'info',
    step: 'behavior',
    phaseId,
    message: `Phase ${phaseId} behavior processing started.`
  });

  return updatedDocument;
}

function markBehaviorPhaseCompleted({ scenarioDocument, planner, phaseId, behaviorPhase, behaviorGeneration }) {
  const completedPhase = {
    ...behaviorPhase,
    phaseId,
    status: 'completed',
    completedAt: new Date().toISOString(),
    attempts: behaviorGeneration.attempts,
    promptSource: behaviorGeneration.promptSource
  };

  const phases = upsertBehaviorPhaseStatus(scenarioDocument.signalBehaviorPlan.result.phases, completedPhase);
  const completedCount = phases.filter((phase) => phase?.status === 'completed').length;
  const totalPhases = planner.phases.length;
  const allCompleted = completedCount >= totalPhases;

  const updatedDocument = {
    ...scenarioDocument,
    status: allCompleted ? 'behavior_completed' : 'behavior_partial',
    currentStep: allCompleted
      ? 'All behavior phases completed.'
      : `Behavior phase ${phaseId} completed. Waiting for next phase.`,
    updatedAt: new Date().toISOString(),
    signalBehaviorPlan: {
      ...scenarioDocument.signalBehaviorPlan,
      attempts: Number(scenarioDocument.signalBehaviorPlan.attempts || 0) + Number(behaviorGeneration.attempts || 1),
      promptSource: behaviorGeneration.promptSource,
      result: {
        ...scenarioDocument.signalBehaviorPlan.result,
        phases
      }
    }
  };

  updatedDocument.progress = buildProgress(planner, phases);
  updatedDocument.logs = appendScenarioLog(updatedDocument.logs, {
    level: 'info',
    step: 'behavior',
    phaseId,
    message: `Phase ${phaseId} behavior completed.`
  });

  return updatedDocument;
}

function markBehaviorPhaseFailed({ scenarioDocument, planner, phaseId, error }) {
  const rawModelResponse =
    error?.rawResponse ||
    error?.cause?.rawResponse ||
    error?.cause?.cause?.rawResponse ||
    null;

  const failedPhase = {
    phaseId,
    status: 'failed',
    failedAt: new Date().toISOString(),
    error: error?.message || 'Unknown error',
    validationErrors: error?.validationErrors || null,
    rawModelResponse
  };

  const phases = upsertBehaviorPhaseStatus(scenarioDocument.signalBehaviorPlan.result.phases, failedPhase);

  const updatedDocument = {
    ...scenarioDocument,
    status: 'behavior_failed',
    currentStep: `Behavior generation failed while processing phase ${phaseId}.`,
    updatedAt: new Date().toISOString(),
    lastError: {
      at: new Date().toISOString(),
      step: 'behavior',
      phaseId,
      message: error?.message || 'Unknown error',
      validationErrors: error?.validationErrors || null,
      rawModelResponse
    },
    signalBehaviorPlan: {
      ...scenarioDocument.signalBehaviorPlan,
      result: {
        ...scenarioDocument.signalBehaviorPlan.result,
        phases
      }
    }
  };

  updatedDocument.progress = buildProgress(planner, phases);
  updatedDocument.logs = appendScenarioLog(updatedDocument.logs, {
    level: 'error',
    step: 'behavior',
    phaseId,
    message: error?.message || 'Unknown error',
    validationErrors: error?.validationErrors || null,
    rawModelResponse
  });

  return updatedDocument;
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
  return {
    ...scenarioDocument,
    progress: buildProgress(planner, scenarioDocument?.signalBehaviorPlan?.result?.phases || [])
  };
}

async function tryWriteFailureToScenario(body, error) {
  try {
    const { bucket, key } = resolveScenarioLocation(body);
    if (!bucket || !key) return;

    const scenarioDocument = await readJsonFromS3(bucket, key);
    const planner = scenarioDocument?.planner?.result || { phases: [] };
    const updatedDocument = {
      ...scenarioDocument,
      status: scenarioDocument?.status === 'behavior_processing' ? 'behavior_failed' : 'failed',
      currentStep: error?.message || 'Simulator AI assist failed.',
      updatedAt: new Date().toISOString(),
      progress: buildProgress(planner, scenarioDocument?.signalBehaviorPlan?.result?.phases || []),
      lastError: {
        at: new Date().toISOString(),
        message: error?.message || 'Unknown error',
        validationErrors: error?.validationErrors || null,
        rawModelResponse:
          error?.rawResponse ||
          error?.cause?.rawResponse ||
          error?.cause?.cause?.rawResponse ||
          null
      },
      logs: appendScenarioLog(scenarioDocument?.logs, {
        level: 'error',
        step: 'ai-assist',
        message: error?.message || 'Unknown error',
        validationErrors: error?.validationErrors || null,
        rawModelResponse:
          error?.rawResponse ||
          error?.cause?.rawResponse ||
          error?.cause?.cause?.rawResponse ||
          null
      })
    };

    await putJsonToS3(bucket, key, updatedDocument);
  } catch (writeError) {
    console.error('Unable to write failure status to scenario.json:', {
      message: writeError?.message
    });
  }
}

async function generateValidatedDrivingPlanner(body) {
  const plannerRequest = buildPlannerRequest(body);
  const promptTemplateResult = await loadPromptTemplate('PHASE_PLAN_PROMPT_KEY');
  const prompt = buildPlannerPrompt(promptTemplateResult.template, plannerRequest);

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
      const validation = validateDrivingPlanner(planner, plannerRequest);

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

async function generateValidatedSignalBehaviorPlan({ body, plannerRequest, planner }) {
  const requestedSignals = extractRequestedSignals(body);

  if (!requestedSignals.length) {
    throw new Error('Unable to generate signal behavior plan because no signals were resolved from the request. Send signals, selectedSignals or signalNames in the body.');
  }

  const promptTemplateResult = await loadPromptTemplate('PHASE_BEHAVIOR_PROMPT_KEY');
  const prompt = buildSignalBehaviorPrompt({
    template: promptTemplateResult.template,
    request: plannerRequest,
    planner,
    requestedSignals
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

async function savePlannerScenarioJson({ body, plannerResult, processingTimeMs }) {
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

  const requestedSignals = extractRequestedSignals(body);
  const scenarioDocument = {
    schemaVersion: 'trackster-ai-scenario-file-v1',
    generatedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    source: 'trackster-simulator-ai-assist',
    lambdaName: process.env.AWS_LAMBDA_FUNCTION_NAME || 'trackster-simulator-ai-assist',
    modelId,
    status: 'planner_saved',
    currentStep: 'Driving planner completed. Waiting for behavior phase generation.',
    processingTimeMs,
    progress: {
      totalPhases: plannerResult.planner.phases.length,
      completedPhases: 0,
      failedPhases: 0,
      processingPhases: 0,
      percent: 0
    },
    logs: appendScenarioLog([], {
      level: 'info',
      step: 'planner',
      message: 'Planner completed.'
    }),
    planner: {
      schemaVersion: 'trackster-driving-planner-v1',
      attempts: plannerResult.attempts,
      promptSource: plannerResult.promptSource,
      request: plannerResult.plannerRequest,
      result: plannerResult.planner
    },
    signalBehaviorPlan: {
      schemaVersion: 'trackster-signal-delta-plan-v1',
      requestedSignals,
      result: {
        schemaVersion: 'trackster-signal-delta-plan-v1',
        phases: []
      }
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

async function loadPromptTemplate(keyEnvName) {
  const bucket = sanitizeText(process.env.PROMPT_BUCKET);

  if (!bucket) {
    throw new Error('PROMPT_BUCKET environment variable was not defined.');
  }

  const key = normalizeS3Key(process.env[keyEnvName]);

  if (!key) {
    throw new Error(`${keyEnvName} environment variable was not defined.`);
  }

  const response = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));

  if (!response.Body) {
    throw new Error(`Prompt file is empty or unreadable: s3://${bucket}/${key}`);
  }

  const template = await response.Body.transformToString('utf8');

  if (!template.trim()) {
    throw new Error(`Prompt file is empty: s3://${bucket}/${key}`);
  }

  return { template, source: { bucket, key } };
}

function buildPlannerPrompt(template, request) {
  return replaceCommonPromptVariables(template, request).trim();
}

function buildSignalBehaviorPrompt({ template, request, planner, requestedSignals }) {
  return replaceCommonPromptVariables(template, request)
    .replaceAll('{{plannerJson}}', JSON.stringify(planner, null, 2))
    .replaceAll('{{signalsJson}}', JSON.stringify(requestedSignals, null, 2))
    .trim();
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
        signals.push(signalName);
      }
    }
  }

  return uniqueStrings(signals);
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
    const lastBrace = text.lastIndexOf('}');

    if (firstBrace < 0 || lastBrace <= firstBrace) {
      const parseError = new Error('Bedrock response is not valid JSON.');
      parseError.rawResponse = text;
      parseError.cause = error;
      throw parseError;
    }

    try {
      return JSON.parse(text.slice(firstBrace, lastBrace + 1));
    } catch (innerError) {
      const parseError = new Error('Bedrock response JSON extraction failed.');
      parseError.rawResponse = text;
      parseError.cause = innerError;
      throw parseError;
    }
  }
}

function validateDrivingPlanner(planner, request) {
  const errors = [];

  if (!planner || typeof planner !== 'object' || Array.isArray(planner)) {
    errors.push('Planner response must be an object.');
    return { valid: false, errors };
  }

  if (!isNonEmptyString(planner.scenarioName)) errors.push('scenarioName must be a non-empty string.');
  if (!isNonEmptyString(planner.summary)) errors.push('summary must be a non-empty string.');
  if (Number(planner.duration) !== Number(request.durationSeconds)) errors.push(`duration must be exactly ${request.durationSeconds}.`);
  if (Number(planner.sampleInterval) !== Number(request.sampleIntervalSeconds)) errors.push(`sampleInterval must be exactly ${request.sampleIntervalSeconds}.`);

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
    const missingSignals = requestedSignals.filter((signalName) => !Object.prototype.hasOwnProperty.call(behaviorPhase.signals, signalName));
    const extraSignals = signalNames.filter((signalName) => !requestedSignals.includes(signalName));

    if (missingSignals.length) errors.push(`phases[${index}] is missing signals: ${missingSignals.join(', ')}.`);
    if (extraSignals.length) errors.push(`phases[${index}] contains extra signals: ${extraSignals.join(', ')}.`);

    for (const signalName of signalNames) {
      const signalBehavior = behaviorPhase.signals[signalName];

      if (!signalBehavior || typeof signalBehavior !== 'object' || Array.isArray(signalBehavior)) {
        errors.push(`phases[${index}].signals.${signalName} must be an object.`);
        continue;
      }

      validateNumericField(errors, signalBehavior, 'start', `phases[${index}].signals.${signalName}`);
      validateNumericField(errors, signalBehavior, 'end', `phases[${index}].signals.${signalName}`);
      validateNumericField(errors, signalBehavior, 'min', `phases[${index}].signals.${signalName}`);
      validateNumericField(errors, signalBehavior, 'max', `phases[${index}].signals.${signalName}`);

      const min = Number(signalBehavior.min);
      const max = Number(signalBehavior.max);
      const start = Number(signalBehavior.start);
      const end = Number(signalBehavior.end);

      if (Number.isFinite(min) && Number.isFinite(max) && min > max) errors.push(`phases[${index}].signals.${signalName}.min must be less than or equal to max.`);
      if (Number.isFinite(start) && Number.isFinite(min) && start < min) errors.push(`phases[${index}].signals.${signalName}.start must not be lower than min.`);
      if (Number.isFinite(start) && Number.isFinite(max) && start > max) errors.push(`phases[${index}].signals.${signalName}.start must not be greater than max.`);
      if (Number.isFinite(end) && Number.isFinite(min) && end < min) errors.push(`phases[${index}].signals.${signalName}.end must not be lower than min.`);
      if (Number.isFinite(end) && Number.isFinite(max) && end > max) errors.push(`phases[${index}].signals.${signalName}.end must not be greater than max.`);

      if (!Array.isArray(signalBehavior.d) || !signalBehavior.d.length) {
        errors.push(`phases[${index}].signals.${signalName}.d must be a non-empty array.`);
      } else {
        for (let deltaIndex = 0; deltaIndex < signalBehavior.d.length; deltaIndex += 1) {
          if (!Number.isFinite(Number(signalBehavior.d[deltaIndex]))) {
            errors.push(`phases[${index}].signals.${signalName}.d[${deltaIndex}] must be numeric.`);
          }
        }
      }
    }
  }

  for (const plannerPhase of planner.phases) {
    if (!seenPhaseIds.has(Number(plannerPhase.id))) {
      errors.push(`phaseId ${plannerPhase.id} is missing from signal behavior plan.`);
    }
  }

  return { valid: errors.length === 0, errors };
}

function validateNumericField(errors, object, fieldName, path) {
  if (!Number.isFinite(Number(object[fieldName]))) {
    errors.push(`${path}.${fieldName} must be numeric.`);
  }
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
