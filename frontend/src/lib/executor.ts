import * as duckdb from '@duckdb/duckdb-wasm';
import { generateSQL, isUnanswerable } from './sqlGenerator';
import { runQuery } from './duckdb';
import type { AgentSettings, AttemptLog, ExecutionResult } from '../types';

export async function runWithRetry(
  question: string,
  schemaText: string,
  settings: AgentSettings,
  db: duckdb.AsyncDuckDB,
  maxRetries = 3,
  onAttempt?: (log: AttemptLog) => void
): Promise<ExecutionResult> {
  const attempts: AttemptLog[] = [];
  let previousSql: string | undefined;
  let previousError: string | undefined;
  const totalStart = performance.now();

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const start = performance.now();
    let genResult;

    try {
      genResult = await generateSQL(question, schemaText, settings, previousSql, previousError);
    } catch (err) {
      const log: AttemptLog = {
        attemptNumber: attempt,
        sql: '',
        success: false,
        error: err instanceof Error ? err.message : String(err),
        rowsReturned: null,
        durationMs: performance.now() - start,
        promptTokens: 0,
        completionTokens: 0,
      };
      attempts.push(log);
      onAttempt?.(log);
      break;
    }

    if (isUnanswerable(genResult)) {
      const log: AttemptLog = {
        attemptNumber: attempt,
        sql: genResult.sql,
        success: false,
        error: 'UNANSWERABLE',
        rowsReturned: null,
        durationMs: performance.now() - start,
        promptTokens: genResult.promptTokens,
        completionTokens: genResult.completionTokens,
      };
      attempts.push(log);
      onAttempt?.(log);
      return {
        question,
        success: false,
        rows: null,
        columns: [],
        finalSql: genResult.sql,
        attempts,
        unanswerable: true,
        totalDurationMs: performance.now() - totalStart,
      };
    }

    try {
      const { rows, columns } = await runQuery(db, genResult.sql);
      const log: AttemptLog = {
        attemptNumber: attempt,
        sql: genResult.sql,
        success: true,
        error: null,
        rowsReturned: rows.length,
        durationMs: performance.now() - start,
        promptTokens: genResult.promptTokens,
        completionTokens: genResult.completionTokens,
      };
      attempts.push(log);
      onAttempt?.(log);
      return {
        question,
        success: true,
        rows,
        columns,
        finalSql: genResult.sql,
        attempts,
        unanswerable: false,
        totalDurationMs: performance.now() - totalStart,
      };
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      const log: AttemptLog = {
        attemptNumber: attempt,
        sql: genResult.sql,
        success: false,
        error: errorMsg,
        rowsReturned: null,
        durationMs: performance.now() - start,
        promptTokens: genResult.promptTokens,
        completionTokens: genResult.completionTokens,
      };
      attempts.push(log);
      onAttempt?.(log);
      previousSql = genResult.sql;
      previousError = errorMsg;
    }
  }

  return {
    question,
    success: false,
    rows: null,
    columns: [],
    finalSql: attempts[attempts.length - 1]?.sql ?? '',
    attempts,
    unanswerable: false,
    totalDurationMs: performance.now() - totalStart,
  };
}
