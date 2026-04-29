export interface ColumnMeta {
  name: string;
  dtype: string;
  nullable: boolean;
  sampleValues: string[];
}

export interface TableMeta {
  name: string;
  rowCount: number;
  columns: ColumnMeta[];
  foreignKeyHints: string[];
}

export interface SchemaMeta {
  tables: TableMeta[];
}

export interface GenerationResult {
  sql: string;
  rawResponse: string;
  model: string;
  promptTokens: number;
  completionTokens: number;
}

export interface AttemptLog {
  attemptNumber: number;
  sql: string;
  success: boolean;
  error: string | null;
  rowsReturned: number | null;
  durationMs: number;
  promptTokens: number;
  completionTokens: number;
}

export interface ExecutionResult {
  question: string;
  success: boolean;
  rows: Record<string, unknown>[] | null;
  columns: string[];
  finalSql: string;
  attempts: AttemptLog[];
  unanswerable: boolean;
  totalDurationMs: number;
}

export interface AnalysisResult {
  summary: string;
  followUpQuestions: string[];
  chartRecommendation: 'bar' | 'line' | 'pie' | 'scatter' | null;
  chartXCol: string | null;
  chartYCol: string | null;
}

export interface AgentResponse {
  question: string;
  result: ExecutionResult;
  analysis: AnalysisResult | null;
  wallTimeMs: number;
}

export type LLMBackend = 'anthropic' | 'openai' | 'deepseek';

export interface AgentSettings {
  backend: LLMBackend;
  apiKey: string;
}
