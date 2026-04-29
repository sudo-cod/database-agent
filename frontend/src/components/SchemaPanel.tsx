import type { SchemaMeta } from '../types';

interface Props {
  schema: SchemaMeta | null;
}

export function SchemaPanel({ schema }: Props) {
  if (!schema || schema.tables.length === 0) {
    return <div className="schema-empty">No tables loaded. Upload a CSV to get started.</div>;
  }

  return (
    <div className="schema-panel">
      <h3>Loaded Tables ({schema.tables.length})</h3>
      {schema.tables.map((table) => (
        <details key={table.name} className="schema-table">
          <summary>
            <span className="table-name">{table.name}</span>
            <span className="row-count">{table.rowCount.toLocaleString()} rows</span>
          </summary>
          <div className="columns">
            {table.columns.map((col) => (
              <div key={col.name} className="column-row">
                <span className="col-name">{col.name}</span>
                <span className="col-type">{col.dtype}</span>
                {col.sampleValues.length > 0 && (
                  <span className="col-samples">{col.sampleValues.slice(0, 3).join(', ')}</span>
                )}
              </div>
            ))}
            {table.foreignKeyHints.length > 0 && (
              <div className="fk-hints">
                {table.foreignKeyHints.join(' | ')}
              </div>
            )}
          </div>
        </details>
      ))}
    </div>
  );
}
