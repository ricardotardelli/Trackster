import OpenAI from 'openai';

const DEFAULT_REGION = 'us-east-1';
const DEFAULT_MODEL = 'google.gemma-4-31b';

export class BedrockProviderError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'BedrockProviderError';
    this.provider = 'bedrock';
    this.status = details.status ?? null;
    this.code = details.code ?? null;
    this.retryable = details.retryable ?? false;
    this.rawResponse = details.rawResponse ?? null;
    this.requestId = details.requestId ?? null;
    this.cause = details.cause;
  }
}

function isRetryableStatus(status) {
  return [408, 409, 429, 500, 502, 503, 504].includes(Number(status));
}

export async function generateLlmResponse({
  prompt,
  maxTokens = 4096,
  temperature = 0.2,
  topP = 0.9,
  timeoutMs = 120000
} = {}) {
  const region =
    process.env.BEDROCK_REGION ||
    process.env.AWS_REGION ||
    DEFAULT_REGION;

  const apiKey = String(
    process.env.BEDROCK_API_KEY ||
    process.env.OPENAI_API_KEY ||
    process.env.AWS_BEARER_TOKEN_BEDROCK ||
    ''
  ).trim();

  const model = String(
    process.env.BEDROCK_MODEL_ID ||
    DEFAULT_MODEL
  ).trim();

  const baseUrl = String(
    process.env.BEDROCK_BASE_URL ||
    process.env.OPENAI_BASE_URL ||
    `https://bedrock-mantle.${region}.api.aws/v1`
  ).replace(/\/+$/, '');

  const projectId =
    process.env.BEDROCK_PROJECT_ID ||
    process.env.OPENAI_PROJECT_ID ||
    'default';

  if (!apiKey) {
    throw new BedrockProviderError(
      'No Bedrock Mantle API key was configured. Set BEDROCK_API_KEY, OPENAI_API_KEY or AWS_BEARER_TOKEN_BEDROCK.',
      { code: 'MISSING_API_KEY' }
    );
  }

  if (typeof prompt !== 'string' || !prompt.trim()) {
    throw new BedrockProviderError(
      'Bedrock provider requires a non-empty prompt.',
      { code: 'INVALID_PROMPT' }
    );
  }

  const startedAt = Date.now();

  const client = new OpenAI({
    baseURL: baseUrl,
    apiKey,
    timeout: Number(timeoutMs),
    maxRetries: 0,
    defaultHeaders: {
      'OpenAI-Project': projectId
    }
  });

  try {
    const response = await client.chat.completions.create({
      model,
      messages: [{ role: 'user', content: prompt }],
      temperature: Number(temperature),
      top_p: Number(topP),
      max_tokens: Number(maxTokens)
    });

    const text = response?.choices?.[0]?.message?.content;

    if (typeof text !== 'string' || !text.trim()) {
      throw new BedrockProviderError(
        'Bedrock Mantle response text is empty.',
        {
          code: 'EMPTY_MODEL_RESPONSE',
          rawResponse: JSON.stringify(response || null)
        }
      );
    }

    const usage = response?.usage || {};

    return {
      text: text.trim(),
      provider: 'bedrock',
      model: response?.model || model,
      inputTokens: usage.prompt_tokens ?? null,
      outputTokens: usage.completion_tokens ?? null,
      totalTokens: usage.total_tokens ?? null,
      requestId:
        response?._request_id ||
        response?.request_id ||
        response?.id ||
        null,
      processingTimeMs: Date.now() - startedAt
    };
  } catch (error) {
    if (error instanceof BedrockProviderError) {
      throw error;
    }

    const status = error?.status || error?.response?.status || null;
    const requestId =
      error?.request_id ||
      error?.requestID ||
      error?.headers?.['x-request-id'] ||
      null;

    const rawResponse = error?.status
      ? JSON.stringify({
          status: error.status,
          code: error?.code || error?.error?.code || null,
          type: error?.type || error?.error?.type || null,
          message: error?.message || null
        })
      : null;

    throw new BedrockProviderError(
      error?.message || 'Unable to call Amazon Bedrock Mantle.',
      {
        status,
        code: error?.code || error?.error?.code || 'BEDROCK_API_ERROR',
        retryable:
          error?.name === 'APIConnectionTimeoutError' ||
          error?.name === 'APIConnectionError' ||
          isRetryableStatus(status),
        rawResponse,
        requestId,
        cause: error
      }
    );
  }
}
