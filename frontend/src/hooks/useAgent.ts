import { useState, useCallback } from 'react';
import * as duckdb from '@duckdb/duckdb-wasm';
import { runWithRetry } from '../lib/executor';
import { analyzeResult } from '../lib/analyzer';
import { schemaToPromptText } from '../lib/schemaExtractor';
import type { AgentResponse, AgentSettings, AttemptLog, SchemaMeta } from '../types';

type AgentStatus = 'idle' | 'generating' | 'executing' | 'analyzing' | 'done' | 'error';

interface AgentState {
  status: AgentStatus;
  currentAttempt: number;
  response: AgentResponse | null;
  error: string | null;
}

export function useAgent(db: duckdb.AsyncDuckDB | null, schema: SchemaMeta | null) {
  const [state, setState] = useState<AgentState>({
    status: 'idle',
    currentAttempt: 0,
    response: null,
    error: null,
  });

  const ask = useCallback(
    async (question: string, settings: AgentSettings) => {
      if (!db || !schema) return;

      const wallStart = performance.now();
      setState({ status: 'generating', currentAttempt: 1, response: null, error: null });

      const schemaText = schemaToPromptText(schema);

      try {
        const result = await runWithRetry(
          question,
          schemaText,
          settings,
          db,
          3,
          (log: AttemptLog) => {
            if (!log.success && log.error !== 'UNANSWERABLE') {
              setState((prev) => ({
                ...prev,
                status: 'generating',
                currentAttempt: log.attemptNumber + 1,
              }));
            }
          }
        );

        let analysis = null;
        if (result.success && result.rows && result.rows.length > 0) {
          setState((prev) => ({ ...prev, status: 'analyzing' }));
          try {
            analysis = await analyzeResult(
              question,
              result.finalSql,
              result.rows,
              result.columns,
              settings
            );
          } catch {
            // analysis failure is non-fatal
          }
        }

        const response: AgentResponse = {
          question,
          result,
          analysis,
          wallTimeMs: performance.now() - wallStart,
        };

        setState({ status: 'done', currentAttempt: 0, response, error: null });
      } catch (err) {
        setState({
          status: 'error',
          currentAttempt: 0,
          response: null,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    },
    [db, schema]
  );

  const reset = useCallback(() => {
    setState({ status: 'idle', currentAttempt: 0, response: null, error: null });
  }, []);

  return { state, ask, reset };
}
