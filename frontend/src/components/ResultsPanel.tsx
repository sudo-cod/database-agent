import { DataChart } from './DataChart';
import { DataGrid } from './DataGrid';
import { AttemptLog } from './AttemptLog';
import { FollowUpChips } from './FollowUpChips';
import type { AgentResponse } from '../types';

interface Props {
  response: AgentResponse;
  onFollowUp: (q: string) => void;
}

export function ResultsPanel({ response, onFollowUp }: Props) {
  const { result, analysis } = response;

  if (result.unanswerable) {
    return (
      <div className="results-panel">
        <div className="msg error">This question cannot be answered with the available data.</div>
        <AttemptLog attempts={result.attempts} />
      </div>
    );
  }

  if (!result.success) {
    return (
      <div className="results-panel">
        <div className="msg error">
          Query failed after {result.attempts.length} attempt{result.attempts.length !== 1 ? 's' : ''}.
        </div>
        <AttemptLog attempts={result.attempts} />
      </div>
    );
  }

  const rows = result.rows ?? [];
  const repaired = result.attempts.length > 1;

  return (
    <div className="results-panel">
      <div className="result-banner">
        {rows.length.toLocaleString()} rows · {result.totalDurationMs.toFixed(0)}ms
        {repaired && <span className="repair-badge"> · self-repaired in {result.attempts.length} attempts</span>}
      </div>

      {analysis?.summary && (
        <div className="summary">{analysis.summary}</div>
      )}

      <details open className="result-section">
        <summary>SQL</summary>
        <pre className="sql-code">{result.finalSql}</pre>
      </details>

      {analysis?.chartRecommendation && (
        <details open className="result-section">
          <summary>Chart</summary>
          <DataChart data={rows} analysis={analysis} columns={result.columns} />
        </details>
      )}

      <details open className="result-section">
        <summary>Table</summary>
        <DataGrid rows={rows} columns={result.columns} />
      </details>

      {repaired && <AttemptLog attempts={result.attempts} />}

      {analysis?.followUpQuestions && (
        <FollowUpChips questions={analysis.followUpQuestions} onSelect={onFollowUp} />
      )}
    </div>
  );
}
