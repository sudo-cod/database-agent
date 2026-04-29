import { useState, useEffect, useCallback } from 'react';
import * as duckdb from '@duckdb/duckdb-wasm';
import { getDB, loadCSVAsTable } from '../lib/duckdb';
import { extractSchema } from '../lib/schemaExtractor';
import type { SchemaMeta } from '../types';

interface DuckDBState {
  db: duckdb.AsyncDuckDB | null;
  schema: SchemaMeta | null;
  loading: boolean;
  error: string | null;
}

export function useDuckDB() {
  const [state, setState] = useState<DuckDBState>({
    db: null,
    schema: null,
    loading: true,
    error: null,
  });

  useEffect(() => {
    getDB()
      .then((db) => {
        setState({ db, schema: null, loading: false, error: null });
      })
      .catch((err) => {
        setState({ db: null, schema: null, loading: false, error: String(err) });
      });
  }, []);

  const refreshSchema = useCallback(async (db: duckdb.AsyncDuckDB) => {
    const schema = await extractSchema(db);
    setState((prev) => ({ ...prev, schema }));
    return schema;
  }, []);

  const loadCSV = useCallback(
    async (file: File, tableName: string): Promise<number> => {
      if (!state.db) throw new Error('Database not initialized');
      const count = await loadCSVAsTable(state.db, file, tableName);
      await refreshSchema(state.db);
      return count;
    },
    [state.db, refreshSchema]
  );

  return { state, loadCSV, refreshSchema };
}
