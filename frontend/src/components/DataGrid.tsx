import { useState } from 'react';

interface Props {
  rows: Record<string, unknown>[];
  columns: string[];
}

export function DataGrid({ rows, columns }: Props) {
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  function handleSort(col: string) {
    if (sortCol === col) {
      setSortAsc((a) => !a);
    } else {
      setSortCol(col);
      setSortAsc(true);
    }
  }

  const sorted = sortCol
    ? [...rows].sort((a, b) => {
        const av = a[sortCol];
        const bv = b[sortCol];
        const cmp = av == null ? -1 : bv == null ? 1 : av < bv ? -1 : av > bv ? 1 : 0;
        return sortAsc ? cmp : -cmp;
      })
    : rows;

  return (
    <div className="data-grid-wrap">
      <div className="grid-meta">
        {rows.length.toLocaleString()} rows × {columns.length} columns
      </div>
      <div className="grid-scroll">
        <table className="data-grid">
          <thead>
            <tr>
              {columns.map((col) => (
                <th
                  key={col}
                  onClick={() => handleSort(col)}
                  className={sortCol === col ? (sortAsc ? 'sort-asc' : 'sort-desc') : ''}
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((row, i) => (
              <tr key={i}>
                {columns.map((col) => (
                  <td key={col}>{row[col] == null ? <span className="null">null</span> : String(row[col])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
