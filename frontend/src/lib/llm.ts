import type { LLMBackend } from '../types';

export interface LLMCallParams {
  backend: LLMBackend;
  apiKey: string;
  systemPrompt: string;
  userPrompt: string;
  maxTokens?: number;
}

export interface LLMResponse {
  content: string;
  promptTokens: number;
  completionTokens: number;
  model: string;
}

const DEFAULT_MODELS: Record<LLMBackend, string> = {
  anthropic: 'claude-sonnet-4-6',
  openai: 'gpt-4o',
  deepseek: 'deepseek-chat',
};

export async function callLLM(params: LLMCallParams): Promise<LLMResponse> {
  switch (params.backend) {
    case 'anthropic':
      return callAnthropic(params);
    case 'openai':
      return callOpenAI(params);
    case 'deepseek':
      return callDeepSeek(params);
  }
}

async function callAnthropic(params: LLMCallParams): Promise<LLMResponse> {
  const model = DEFAULT_MODELS.anthropic;
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': params.apiKey,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      model,
      max_tokens: params.maxTokens ?? 1024,
      system: params.systemPrompt,
      messages: [{ role: 'user', content: params.userPrompt }],
    }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(`Anthropic ${res.status}: ${(err as { error?: { message?: string } }).error?.message ?? res.statusText}`);
  }

  const data = await res.json() as {
    content: { text: string }[];
    usage: { input_tokens: number; output_tokens: number };
  };

  return {
    content: data.content[0]?.text ?? '',
    promptTokens: data.usage.input_tokens,
    completionTokens: data.usage.output_tokens,
    model,
  };
}

async function callOpenAI(params: LLMCallParams): Promise<LLMResponse> {
  const model = DEFAULT_MODELS.openai;
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${params.apiKey}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      model,
      temperature: 0,
      messages: [
        { role: 'system', content: params.systemPrompt },
        { role: 'user', content: params.userPrompt },
      ],
    }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(`OpenAI ${res.status}: ${(err as { error?: { message?: string } }).error?.message ?? res.statusText}`);
  }

  const data = await res.json() as {
    choices: { message: { content: string } }[];
    usage: { prompt_tokens: number; completion_tokens: number };
    model: string;
  };

  return {
    content: data.choices[0]?.message.content ?? '',
    promptTokens: data.usage.prompt_tokens,
    completionTokens: data.usage.completion_tokens,
    model: data.model,
  };
}

async function callDeepSeek(params: LLMCallParams): Promise<LLMResponse> {
  const model = DEFAULT_MODELS.deepseek;
  const res = await fetch('https://api.deepseek.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${params.apiKey}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      model,
      temperature: 0,
      messages: [
        { role: 'system', content: params.systemPrompt },
        { role: 'user', content: params.userPrompt },
      ],
    }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(`DeepSeek ${res.status}: ${(err as { error?: { message?: string } }).error?.message ?? res.statusText}`);
  }

  const data = await res.json() as {
    choices: { message: { content: string } }[];
    usage: { prompt_tokens: number; completion_tokens: number };
    model: string;
  };

  return {
    content: data.choices[0]?.message.content ?? '',
    promptTokens: data.usage.prompt_tokens,
    completionTokens: data.usage.completion_tokens,
    model: data.model,
  };
}
