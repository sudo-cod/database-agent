import { useState, useRef } from 'react';

function sanitizeTableName(filename: string): string {
  const stem = filename.replace(/\.[^.]+$/, '');
  let name = stem.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase().replace(/^_+|_+$/g, '');
  if (name && /^\d/.test(name)) name = 't_' + name;
  return name || 'uploaded_table';
}

interface FileEntry {
  file: File;
  tableName: string;
}

interface Props {
  onLoad: (file: File, tableName: string) => Promise<number>;
  disabled?: boolean;
}

export function FileUploader({ onLoad, disabled }: Props) {
  const [entries, setEntries] = useState<FileEntry[]>([]);
  const [results, setResults] = useState<{ name: string; count: number }[]>([]);
  const [errors, setErrors] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  function handleFiles(files: FileList) {
    const newEntries: FileEntry[] = [];
    for (const file of Array.from(files)) {
      if (file.name.endsWith('.csv')) {
        newEntries.push({ file, tableName: sanitizeTableName(file.name) });
      }
    }
    setEntries(newEntries);
    setResults([]);
    setErrors([]);
  }

  async function handleLoad() {
    setLoading(true);
    setErrors([]);
    setResults([]);
    const newResults: { name: string; count: number }[] = [];
    const newErrors: string[] = [];

    for (const entry of entries) {
      if (!entry.tableName.trim()) {
        newErrors.push(`${entry.file.name}: table name cannot be empty`);
        continue;
      }
      try {
        const count = await onLoad(entry.file, entry.tableName.trim());
        newResults.push({ name: entry.tableName.trim(), count });
      } catch (err) {
        newErrors.push(`${entry.file.name}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    setResults(newResults);
    setErrors(newErrors);
    if (newResults.length > 0) setEntries([]);
    setLoading(false);
  }

  return (
    <div className="file-uploader">
      <div
        className="drop-zone"
        onClick={() => inputRef.current?.click()}
        onDragOver={(e) => e.preventDefault()}
        onDrop={(e) => { e.preventDefault(); if (e.dataTransfer.files) handleFiles(e.dataTransfer.files); }}
      >
        <span>Drop CSV files here or click to browse</span>
        <input
          ref={inputRef}
          type="file"
          accept=".csv"
          multiple
          style={{ display: 'none' }}
          onChange={(e) => { if (e.target.files) handleFiles(e.target.files); }}
        />
      </div>

      {entries.length > 0 && (
        <div className="file-entries">
          {entries.map((entry, i) => (
            <div key={i} className="file-entry">
              <span className="filename">{entry.file.name}</span>
              <input
                type="text"
                value={entry.tableName}
                onChange={(e) => {
                  const updated = [...entries];
                  updated[i] = { ...entry, tableName: e.target.value };
                  setEntries(updated);
                }}
                placeholder="table name"
              />
            </div>
          ))}
          <button onClick={handleLoad} disabled={loading || disabled}>
            {loading ? 'Loading…' : 'Load into database'}
          </button>
        </div>
      )}

      {results.map((r, i) => (
        <div key={i} className="msg success">
          <strong>{r.name}</strong> loaded — {r.count.toLocaleString()} rows
        </div>
      ))}
      {errors.map((e, i) => (
        <div key={i} className="msg error">{e}</div>
      ))}
    </div>
  );
}
