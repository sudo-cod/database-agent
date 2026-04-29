import { useState } from 'react';
import { SettingsBar } from './components/SettingsBar';
import { FileUploader } from './components/FileUploader';
import { SchemaPanel } from './components/SchemaPanel';
import { QueryInput } from './components/QueryInput';
import { ResultsPanel } from './components/ResultsPanel';
import { useDuckDB } from './hooks/useDuckDB';
import { useAgent } from './hooks/useAgent';
import type { AgentSettings } from './types';

const DEFAULT_SETTINGS: AgentSettings = { backend: 'anthropic', apiKey: '' };

export default function App() {
  const [settings, setSettings] = useState<AgentSettings>(DEFAULT_SETTINGS);
  const [pendingQuestion, setPendingQuestion] = useState('');
  const { state: dbState, loadCSV } = useDuckDB();
  const { state: agentState, ask } = useAgent(dbState.db, dbState.schema);

  const isRunning = agentState.status === 'generating' || agentState.status === 'analyzing';
  const canAsk = !isRunning && !!settings.apiKey && !!dbState.schema && dbState.schema.tables.length > 0;

  function handleAsk(question: string) {
    setPendingQuestion(question);
    ask(question, settings);
  }

  function handleFollowUp(question: string) {
    setPendingQuestion(question);
    ask(question, settings);
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>SQL Intelligence Agent</h1>
        <p className="subtitle">Upload any CSV files and query them with natural language</p>
      </header>

      <main className="app-main">
        <section className="panel">
          <SettingsBar settings={settings} onChange={setSettings} />
        </section>

        <section className="panel">
          {dbState.loading ? (
            <div className="loading">Initializing DuckDB…</div>
          ) : dbState.error ? (
            <div className="msg error">DuckDB failed to load: {dbState.error}</div>
          ) : (
            <FileUploader onLoad={loadCSV} disabled={isRunning} />
          )}
        </section>

        {dbState.schema && (
          <section className="panel">
            <SchemaPanel schema={dbState.schema} />
          </section>
        )}

        <section className="panel">
          {!settings.apiKey && (
            <div className="msg warn">Enter an API key above to enable queries.</div>
          )}
          {dbState.schema && dbState.schema.tables.length === 0 && (
            <div className="msg warn">Upload at least one CSV to start querying.</div>
          )}
          <QueryInput
            onSubmit={handleAsk}
            disabled={!canAsk}
            status={agentState.status}
            initialQuestion={pendingQuestion}
          />
        </section>

        {agentState.status === 'generating' && (
          <div className="status-bar">
            Generating SQL (attempt {agentState.currentAttempt})…
          </div>
        )}
        {agentState.status === 'analyzing' && (
          <div className="status-bar">Analyzing results…</div>
        )}
        {agentState.error && (
          <div className="msg error panel">{agentState.error}</div>
        )}

        {agentState.response && (
          <section className="panel">
            <ResultsPanel response={agentState.response} onFollowUp={handleFollowUp} />
          </section>
        )}
      </main>
    </div>
  );
}
