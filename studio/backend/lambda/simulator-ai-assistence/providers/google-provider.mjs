import {
  GoogleGenAI,
  ThinkingLevel
} from '@google/genai';

const DEFAULT_MODEL = 'gemma-4-31b-it';

export class GoogleProviderError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'GoogleProviderError';
    this.provider = 'google';
    this.status = details.status ?? null;
    this.code = details.code ?? null;
    this.retryable = details.retryable ?? false;
    this.rawResponse = details.rawResponse ?? null;
    this.requestId = details.requestId ?? null;
    this.cause = details.cause;
  }
}

function isRetryableStatus(status) {
  return [408, 429, 500, 502, 503, 504].includes(Number(status));
}

function safeJsonStringify(value) {
  try {
    return JSON.stringify(value ?? null);
  } catch {
    return null;
  }
}

function resolveErrorDetails(error) {
  const status = Number(
    error?.status ||
    error?.statusCode ||
    error?.response?.status ||
    error?.cause?.status ||
    0
  ) || null;

  const code =
    error?.code ||
    error?.error?.status ||
    error?.cause?.code ||
    (status ? 'GOOGLE_API_ERROR' : 'NETWORK_ERROR');

  const rawResponse =
    error?.rawResponse ||
    error?.responseBody ||
    safeJsonStringify(error?.error || error?.response?.data || null);

  return {
    status,
    code,
    retryable: status ? isRetryableStatus(status) : true,
    rawResponse,
    requestId:
      error?.requestId ||
      error?.response?.headers?.get?.('x-request-id') ||
      error?.response?.headers?.get?.('x-goog-request-id') ||
      null
  };
}

function resolveChunkText(chunk) {
  return typeof chunk?.text === 'string' ? chunk.text : '';
}

export async function generateLlmResponse({
  prompt,
  responseMimeType = null,
  responseSchema = null
} = {}) {
  const apiKey = String(
    process.env.GOOGLE_AI_API_KEY ||
    process.env.GEMINI_API_KEY ||
    ''
  ).trim();

  const model = String(
    process.env.GOOGLE_AI_MODEL || DEFAULT_MODEL
  ).trim();

  if (!apiKey) {
    throw new GoogleProviderError(
      'GOOGLE_AI_API_KEY environment variable was not defined.',
      { code: 'MISSING_API_KEY' }
    );
  }

  if (typeof prompt !== 'string' || !prompt.trim()) {
    throw new GoogleProviderError(
      'Google provider requires a non-empty prompt.',
      { code: 'INVALID_PROMPT' }
    );
  }

  const startedAt = Date.now();

  try {
    const ai = new GoogleGenAI({
      apiKey
    });

    const config = {
      thinkingConfig: {
        thinkingLevel: ThinkingLevel.HIGH
      },
      tools: [
        {
          googleSearch: {}
        }
      ]
    };

    if (typeof responseMimeType === 'string' && responseMimeType.trim()) {
      config.responseMimeType = responseMimeType.trim();
    }

    if (
      responseSchema &&
      typeof responseSchema === 'object' &&
      !Array.isArray(responseSchema)
    ) {
      config.responseSchema = responseSchema;
    }

    const stream = await ai.models.generateContentStream({
      model,
      contents: [
        {
          role: 'user',
          parts: [
            {
              text: prompt
            }
          ]
        }
      ],
      config
    });

    let text = '';
    let responseId = null;
    let modelVersion = null;
    let usageMetadata = null;
    let promptFeedback = null;
    let chunkCount = 0;
    let firstChunkTimeMs = null;

    for await (const chunk of stream) {
      chunkCount += 1;

      if (firstChunkTimeMs === null) {
        firstChunkTimeMs = Date.now() - startedAt;
      }

      text += resolveChunkText(chunk);

      responseId = chunk?.responseId || responseId;
      modelVersion = chunk?.modelVersion || modelVersion;
      usageMetadata = chunk?.usageMetadata || usageMetadata;
      promptFeedback = chunk?.promptFeedback || promptFeedback;
    }

    const normalizedText = text.trim();

    if (!normalizedText) {
      const blockReason = promptFeedback?.blockReason || null;

      throw new GoogleProviderError(
        'Google returned no textual model response.',
        {
          code: blockReason ? 'PROMPT_BLOCKED' : 'EMPTY_MODEL_RESPONSE',
          rawResponse: safeJsonStringify({
            chunkCount,
            promptFeedback,
            usageMetadata
          }),
          requestId: responseId
        }
      );
    }

    return {
      text: normalizedText,
      provider: 'google',
      model: modelVersion || model,
      inputTokens: usageMetadata?.promptTokenCount ?? null,
      outputTokens: usageMetadata?.candidatesTokenCount ?? null,
      totalTokens: usageMetadata?.totalTokenCount ?? null,
      requestId: responseId,
      processingTimeMs: Date.now() - startedAt,
      firstChunkTimeMs,
      chunkCount,
      serviceTier: usageMetadata?.serviceTier ?? null
    };
  } catch (error) {
    if (error instanceof GoogleProviderError) {
      throw error;
    }

    const details = resolveErrorDetails(error);

    throw new GoogleProviderError(
      error?.message || 'Unable to call Google Gemini API.',
      {
        ...details,
        cause: error
      }
    );
  }
}
