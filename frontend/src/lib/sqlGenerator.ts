import { callLLM } from './llm';
import type { AgentSettings, GenerationResult } from '../types';

const SYSTEM_PROMPT = `Write a single correct DuckDB SQL query to answer the question.
Rules:
- Output ONLY raw SQL. No markdown, no explanation, no preamble.
- Qualify all column names with table names when joining.
- Use double quotes for identifiers with spaces.
- Do not add LIMIT unless the question asks for top-N.
- If the question cannot be answered with the available schema, output exactly: UNANSWERABLE
- DuckDB supports CTEs, window functions, date_diff, strftime, QUALIFY.
- For date extraction use: EXTRACT(month FROM col), strftime(col, '%Y-%m'), date_trunc('month', col)`;

export function extractSQL(raw: string): string {
  return raw
    .replace(/^```sql\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/\s*```$/, '')
    .trim();
}

function buildUserPrompt(
  schemaText: string,
  question: string,
  previousSql?: string,
  errorMessage?: string
): string {
  let prompt = schemaText + '\n\n### QUESTION\n' + question;
  if (previousSql && errorMessage) {
    prompt +=
      '\n\n### PREVIOUS ATTEMPT FAILED\nSQL: ' +
      previousSql +
      '\nError: ' +
      errorMessage +
      '\nWrite a corrected SQL query.';
  }
  prompt += '\n\n### SQL QUERY\n';
  return prompt;
}

export async function generateSQL(
  question: string,
  schemaText: string,
  settings: AgentSettings,
  previousSql?: string,
  errorMessage?: string
): Promise<GenerationResult> {
  const userPrompt = buildUserPrompt(schemaText, question, previousSql, errorMessage);
  const response = await callLLM({
    backend: settings.backend,
    apiKey: settings.apiKey,
    systemPrompt: SYSTEM_PROMPT,
    userPrompt,
    maxTokens: 1024,
  });
  return {
    sql: extractSQL(response.content),
    rawResponse: response.content,
    model: response.model,
    promptTokens: response.promptTokens,
    completionTokens: response.completionTokens,
  };
}

export function isUnanswerable(result: GenerationResult): boolean {
  return result.sql.trim().toUpperCase() === 'UNANSWERABLE';
}
