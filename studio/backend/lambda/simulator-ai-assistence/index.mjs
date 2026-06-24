import { Sha256 } from '@aws-crypto/sha256-js';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
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

const DEFAULT_CONTROLLED_SIGNALS = [
  'VehicleSpeedKph',
  'EngineRpm',
  'CoolantTemperatureC',
  'FuelLevelPercent',
  'AcceleratorPedalPercent',
  'BrakePedalPercent'
];

const PREFERRED_SIGNAL_NAMES = [
  'VehicleSpeedKph',
  'Speed',
  'WheelSpeed',
  'EngineRpm',
  'RPM',
  'CoolantTemperatureC',
  'EngineCoolantTemp',
  'FuelLevelPercent',
  'FuelLevel',
  'AcceleratorPedalPercent',
  'AcceleratorPedal',
  'ThrottlePosition',
  'BrakePedalPercent',
  'BrakePedal',
  'BrakePressure'
];

const MAX_CONTROLLED_SIGNALS = Number.parseInt(process.env.MAX_AI_CONTROLLED_SIGNALS || '8', 10);
const DEFAULT_PHASE_COUNT = Number.parseInt(process.env.AI_PHASE_COUNT || '6', 10);
const DEFAULT_MAX_TOKENS = Number.parseInt(process.env.BEDROCK_MAX_TOKENS || '8192', 10);

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
    const scenarioRequest = buildScenarioRequest(body);
    const prompt = buildSignalDeltaPrompt(scenarioRequest);
    const mantleResponse = await callBedrockMantle(prompt);
    const responseText = extractTextFromMantleResponse(mantleResponse);

    const scenario = parseJsonResponse(responseText);
    const validation = validateSignalDeltaScenario(scenario, scenarioRequest);

    if (!validation.valid) {
      return buildResponse(422, {
        success: false,
        message: 'Bedrock returned an invalid signal delta scenario.',
        validationErrors: validation.errors,
        rawResponse: responseText
      });
    }

    return buildResponse(200, {
      success: true,
      modelId,
      schemaVersion: scenarioRequest.schemaVersion,
      controlledSignals: scenarioRequest.controlledSignals,
      scenario
    });
  } catch (error) {
    console.error('Simulator AI assistance failed:', {
      name: error?.name,
      message: error?.message,
      stack: error?.stack
    });

    return buildResponse(500, {
      success: false,
      message: 'Simulator AI assistance failed.',
      error: error?.message || 'Unknown error'
    });
  }
};

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
  if (!availableSignals.length) {
    return DEFAULT_CONTROLLED_SIGNALS;
  }

  const remaining = new Set(availableSignals);
  const selected = [];

  for (const preferred of PREFERRED_SIGNAL_NAMES) {
    const exact = availableSignals.find((name) => name === preferred);
    const fuzzy = availableSignals.find((name) => {
      const normalizedName = normalizeSignalNameForMatch(name);
      const normalizedPreferred = normalizeSignalNameForMatch(preferred);
      return normalizedName.includes(normalizedPreferred) || normalizedPreferred.includes(normalizedName);
    });

    const match = exact || fuzzy;

    if (match && remaining.has(match) && selected.length < MAX_CONTROLLED_SIGNALS) {
      selected.push(match);
      remaining.delete(match);
    }
  }

  for (const signalName of availableSignals) {
    if (selected.length >= MAX_CONTROLLED_SIGNALS) {
      break;
    }

    if (remaining.has(signalName)) {
      selected.push(signalName);
      remaining.delete(signalName);
    }
  }

  return selected.length ? selected : DEFAULT_CONTROLLED_SIGNALS;
}

function normalizeSignalNameForMatch(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

function buildSignalDeltaPrompt(request) {
  const availableSignalsText = request.controlledSignals.map((signal) => `- ${signal}`).join('\n');
  const targetSpeedText = Number.isFinite(request.targetSpeed)
    ? `${request.targetSpeed} ${request.distanceUnit}/h`
    : 'not specified';

  return `
You are generating executable signal behavior instructions for a CAN telemetry simulator.

Return ONLY valid JSON.
Do not include markdown.
Do not include explanations.
Do not include comments.
Do not include any text outside the JSON.

The worker will execute the result directly.

Worker execution logic:
- Each signal starts at "start".
- At every dt seconds, the worker applies the next delta from "d".
- When the delta array ends, the worker repeats it.
- The worker clamps values between min and max.
- The worker may validate that the calculated final value is close to "end".
- The worker applies deltas until the phase time ends or the value reaches the configured min/max safety boundary.
- Pay attention to deltas and make sure the repeated delta calculation moves the value close to the intended end value without depending on clamp.

Generate a realistic urban driving scenario.

Scenario context:
- Scenario name: ${request.scenarioName}
- Requested context: ${request.requestedContext}
- Driver profile: ${request.driverProfile}
- Target speed: ${targetSpeedText}
- Simulation mode: ${request.simulationMode || 'not specified'}
- Generation type: ${request.generationType || 'not specified'}
- Route region: ${request.routeRegion || 'not specified'}
- Initial date/time: ${request.initialDateTime || 'not specified'}
- Selected CAN frames: ${request.selectedCanFrames}

Available signals:
${availableSignalsText}

Global rules:
- dur must be exactly ${request.durationSeconds}.
- dt must be exactly ${request.sampleIntervalSeconds}.
- Create exactly ${request.phaseCount} phases.
- Phases must be ordered.
- Phases must not overlap.
- First phase starts at 0.
- Final phase ends at ${request.durationSeconds}.
- Every phase must contain every available signal listed above.

Signal rules:
- Every signal must contain: start, end, min, max, d.
- start must be >= min.
- start must be <= max.
- end must be >= min.
- end must be <= max.
- min must be <= max.
- d must contain only numeric values.
- d length must be between 4 and 12.
- min and max are safety limits, not target values.
- Do not rely on clamp to hide unrealistic deltas.
- Deltas must be coherent with the difference between start and end.
- The expected final value after repeated deltas should be close to end.
- The end of each phase should be coherent with the start of the next phase.

Physical realism rules:
- Vehicle speed signals should evolve realistically according to the phase.
- Engine RPM signals must remain coherent with vehicle speed signals when both exist.
- Temperature signals should warm up gradually and then stabilize.
- Fuel level signals must never increase.
- Accelerator or throttle signals and brake signals should not both increase aggressively in the same phase.
- Values should evolve smoothly unless the phase explicitly represents a sudden event.
- For any signal you do not fully understand, use conservative start/end values and small deltas relative to the signal range.

Before returning JSON:
- Verify every start and end are inside min/max.
- Verify every min <= max.
- Verify fuel-like signals never increase.
- Verify phase transitions are coherent.
- Verify deltas are realistic relative to the signal scale.
- Verify repeated deltas approximately move from start to end.
- Fix all violations before returning.

Return exactly this schema and use only the available signal names listed above:

{
  "n": "string",
  "dur": ${request.durationSeconds},
  "dt": ${request.sampleIntervalSeconds},
  "ph": [
    {
      "n": "string",
      "from": 0,
      "to": 0,
      "s": {
        "SignalName": {
          "start": 0,
          "end": 0,
          "min": 0,
          "max": 0,
          "d": [0]
        }
      }
    }
  ]
}
`.trim();
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
