import { generateLlmResponse as generateWithGoogle } from './google-provider.mjs';
import { generateLlmResponse as generateWithBedrock } from './bedrock-provider.mjs';

function normalizeProvider(value) {
  const provider = String(value || 'bedrock').trim().toLowerCase();

  if (provider === 'google' || provider === 'gemini') {
    return 'google';
  }

  if (provider === 'bedrock' || provider === 'mantle' || provider === 'aws') {
    return 'bedrock';
  }

  throw new Error(
    `Unsupported AI_PROVIDER "${provider}". Use "google" or "bedrock".`
  );
}

export function getAiProviderInfo() {
  const provider = normalizeProvider(process.env.AI_PROVIDER);

  if (provider === 'google') {
    return {
      provider,
      model: process.env.GOOGLE_AI_MODEL || 'gemma-4-31b-it'
    };
  }

  return {
    provider,
    model: process.env.BEDROCK_MODEL_ID || 'google.gemma-4-31b'
  };
}

export async function generateLlmResponse(options = {}) {
  const { provider } = getAiProviderInfo();

  if (provider === 'google') {
    return generateWithGoogle(options);
  }

  return generateWithBedrock(options);
}
