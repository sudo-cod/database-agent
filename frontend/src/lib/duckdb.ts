import * as duckdb from '@duckdb/duckdb-wasm';
import duckdbMvpWasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import duckdbMvpWorker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';

let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;

export function getDB(): Promise<duckdb.AsyncDuckDB> {
  if (!dbPromise) {
    dbPromise = (async () => {
      const logger = new duckdb.ConsoleLogger();
      const worker = new Worker(duckdbMvpWorker);
      const db = new duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(duckdbMvpWasm);
      await db.open({ path: ':memory:' });
      return db;
    })();
  }
  return dbPromise;
}

export async function loadCSVAsTable(
  db: duckdb.AsyncDuckDB,
  file: File,
  tableName: string
): Promise<number> {
  const conn = await db.connect();
  try {
    await db.registerFileText(tableName + '.csv', await file.text());
    await conn.query(
      `CREATE OR REPLACE TABLE "${tableName}" AS SELECT * FROM read_csv_auto('${tableName}.csv', header=true)`
    );
    const result = await conn.query(`SELECT COUNT(*) as n FROM "${tableName}"`);
    const rows = result.toArray();
    return Number(rows[0]?.n ?? 0);
  } finally {
    await conn.close();
  }
}

export async function runQuery(
  db: duckdb.AsyncDuckDB,
  sql: string
): Promise<{ rows: Record<string, unknown>[]; columns: string[] }> {
  const conn = await db.connect();
  try {
    const upper = sql.trim().replace(/;+$/, '');
    const upperTest = upper.toUpperCase();
    const needsLimit =
      (upperTest.startsWith('SELECT') || upperTest.startsWith('WITH')) &&
      !/\bLIMIT\s+\d+\s*$/i.test(upper);
    const finalSql = needsLimit ? `SELECT * FROM (${upper}) __q LIMIT 10000` : upper;

    const arrowResult = await conn.query(finalSql);
    const fieldNames = arrowResult.schema.fields.map((f) => f.name);
    const rows = arrowResult.toArray().map((row) => {
      const obj: Record<string, unknown> = {};
      for (const name of fieldNames) {
        const val = (row as Record<string, unknown>)[name];
        // Coerce BigInt for Recharts and JSON compatibility
        obj[name] = typeof val === 'bigint' ? Number(val) : val;
      }
      return obj;
    });
    return { rows, columns: fieldNames };
  } finally {
    await conn.close();
  }
}

export async function listTables(db: duckdb.AsyncDuckDB): Promise<string[]> {
  const conn = await db.connect();
  try {
    const result = await conn.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
    );
    return result.toArray().map((r) => String((r as Record<string, unknown>)['table_name']));
  } finally {
    await conn.close();
  }
}
