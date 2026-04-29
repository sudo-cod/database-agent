import { callLLM } from './llm';
import type { AgentSettings, AnalysisResult } from '../types';

const ANALYSIS_SYSTEM = `You are a data analyst. Given a SQL query and its results, respond ONLY with valid JSON:
{
  "summary": "2-4 sentence plain language insight",
  "follow_up_questions": ["...", "..."],
  "chart_recommendation": "bar"|"line"|"pie"|"scatter"|null,
  "chart_x_col": "<exact column name>"|null,
  "chart_y_col": "<exact column name>"|null
}
chart rules: bar=categorical x + numeric y, line=time x + numeric y, pie=categorical proportions (max 8 categories), scatter=two numeric columns, null=other.`;

function buildAnalysisPrompt(
  question: string,
  sql: string,
  rows: Record<string, unknown>[],
  columns: string[]
): string {
  const totalRows = rows.length;
  const dtypes = columns.map((c) => {
    const sample = rows[0]?.[c];
    const t = typeof sample;
    return `${c}: ${t === 'number' ? 'numeric' : t === 'string' ? 'text' : t}`;
  }).join(', ');

  const preview = rows.slice(0, 20);
  const csvHeader = columns.join(',');
  const csvRows = preview.map((r) => columns.map((c) => String(r[c] ?? '')).join(','));
  const csvText = [csvHeader, ...csvRows].join('\n');
  const remaining = totalRows - preview.length;

  return (
    `### QUESTION\n${question}\n\n` +
    `### SQL\n${sql}\n\n` +
    `### RESULT\n${totalRows} rows × ${columns.length} cols\nTypes: ${dtypes}\n\n` +
    `### DATA\n${csvText}` +
    (remaining > 0 ? `\n...${remaining} more rows` : '') +
    '\n\nRespond with JSON now.'
  );
}

function parseAnalysisJSON(raw: string): AnalysisResult {
  const cleaned = raw
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/\s*```$/, '')
    .trim();

  const data = JSON.parse(cleaned) as {
    summary?: string;
    follow_up_questions?: string[];
    chart_recommendation?: string | null;
    chart_x_col?: string | null;
    chart_y_col?: string | null;
  };

  const validCharts = new Set(['bar', 'line', 'pie', 'scatter']);
  const chart = data.chart_recommendation && validCharts.has(data.chart_recommendation)
    ? (data.chart_recommendation as AnalysisResult['chartRecommendation'])
    : null;

  return {
    summary: data.summary ?? '',
    followUpQuestions: data.follow_up_questions ?? [],
    chartRecommendation: chart,
    chartXCol: data.chart_x_col ?? null,
    chartYCol: data.chart_y_col ?? null,
  };
}

export async function analyzeResult(
  question: string,
  sql: string,
  rows: Record<string, unknown>[],
  columns: string[],
  settings: AgentSettings
): Promise<AnalysisResult> {
  const userPrompt = buildAnalysisPrompt(question, sql, rows, columns);
  const response = await callLLM({
    backend: settings.backend,
    apiKey: settings.apiKey,
    systemPrompt: ANALYSIS_SYSTEM,
    userPrompt,
    maxTokens: 1024,
  });
  return parseAnalysisJSON(response.content);
}
