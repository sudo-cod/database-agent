import type { AttemptLog as AttemptLogType } from '../types';

interface Props {
  attempts: AttemptLogType[];
}

export function AttemptLog({ attempts }: Props) {
  if (attempts.length === 0) return null;
  return (
    <details className="attempt-log">
      <summary>Attempt Log ({attempts.length} attempt{attempts.length !== 1 ? 's' : ''})</summary>
      <div className="attempts">
        {attempts.map((a) => (
          <div key={a.attemptNumber} className={`attempt ${a.success ? 'ok' : 'fail'}`}>
            <div className="attempt-header">
              Attempt {a.attemptNumber} — {a.success ? '✓ success' : '✗ failed'}
              <span className="duration">{a.durationMs.toFixed(0)}ms</span>
            </div>
            <pre className="attempt-sql">{a.sql}</pre>
            {a.error && a.error !== 'UNANSWERABLE' && (
              <div className="attempt-error">{a.error}</div>
            )}
          </div>
        ))}
      </div>
    </details>
  );
}
