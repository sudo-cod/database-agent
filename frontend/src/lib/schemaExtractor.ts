import * as duckdb from '@duckdb/duckdb-wasm';
import type { ColumnMeta, TableMeta, SchemaMeta } from '../types';

export async function extractSchema(db: duckdb.AsyncDuckDB, sampleN = 3): Promise<SchemaMeta> {
  const conn = await db.connect();
  try {
    const tableRes = await conn.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
    );
    const tableNames = tableRes.toArray().map((r) => String((r as Record<string, unknown>)['table_name']));

    const rawTables: TableMeta[] = [];

    for (const tname of tableNames) {
      const countRes = await conn.query(`SELECT COUNT(*) as n FROM "${tname}"`);
      const rowCount = Number((countRes.toArray()[0] as Record<string, unknown>)['n'] ?? 0);

      const colRes = await conn.query(
        `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name='${tname}' ORDER BY ordinal_position`
      );

      const columns: ColumnMeta[] = [];
      for (const row of colRes.toArray()) {
        const r = row as Record<string, unknown>;
        const colName = String(r['column_name']);
        const dtype = String(r['data_type']);
        const nullable = String(r['is_nullable']).toUpperCase() === 'YES';

        let sampleValues: string[] = [];
        try {
          const sampleRes = await conn.query(
            `SELECT DISTINCT "${colName}" FROM "${tname}" WHERE "${colName}" IS NOT NULL LIMIT ${sampleN}`
          );
          sampleValues = sampleRes.toArray().map((sr) => {
            const v = (sr as Record<string, unknown>)[colName];
            return typeof v === 'bigint' ? String(Number(v)) : String(v ?? '');
          });
        } catch {
          // ignore sample errors for complex types
        }

        columns.push({ name: colName, dtype, nullable, sampleValues });
      }

      rawTables.push({ name: tname, rowCount, columns, foreignKeyHints: [] });
    }

    const fkHints = inferForeignKeys(rawTables);
    for (const table of rawTables) {
      table.foreignKeyHints = fkHints.get(table.name) ?? [];
    }

    return { tables: rawTables };
  } finally {
    await conn.close();
  }
}

function inferForeignKeys(tables: TableMeta[]): Map<string, string[]> {
  const colIndex = new Map<string, string[]>();
  for (const table of tables) {
    for (const col of table.columns) {
      const existing = colIndex.get(col.name) ?? [];
      existing.push(table.name);
      colIndex.set(col.name, existing);
    }
  }

  const hints = new Map<string, string[]>(tables.map((t) => [t.name, []]));
  for (const [colName, tableNames] of colIndex.entries()) {
    if (tableNames.length > 1) {
      for (let i = 0; i < tableNames.length; i++) {
        for (let j = i + 1; j < tableNames.length; j++) {
          const t1 = tableNames[i]!;
          const t2 = tableNames[j]!;
          hints.get(t1)!.push(`${colName} -> ${t2}.${colName}`);
          hints.get(t2)!.push(`${colName} -> ${t1}.${colName}`);
        }
      }
    }
  }
  return hints;
}

export function schemaToPromptText(schema: SchemaMeta): string {
  const lines: string[] = ['### DATABASE SCHEMA\n'];
  for (const table of schema.tables) {
    lines.push(`TABLE: ${table.name}  (${table.rowCount.toLocaleString('en-US')} rows)`);
    for (const col of table.columns) {
      const nullableFlag = col.nullable ? 'nullable' : 'not null';
      const samplesStr = col.sampleValues.length > 0 ? col.sampleValues.join(', ') : '—';
      lines.push(`  ${col.name}  [${col.dtype}, ${nullableFlag}]  samples: ${samplesStr}`);
    }
    if (table.foreignKeyHints.length > 0) {
      lines.push('  -- join hints: ' + table.foreignKeyHints.join(' | '));
    }
    lines.push('');
  }
  return lines.join('\n');
}
