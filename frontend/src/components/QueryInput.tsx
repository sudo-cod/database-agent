import { useState } from 'react';

const EXAMPLE_QUESTIONS = [
  'How many rows are in each table?',
  'What are the top 10 values by count?',
  'Show me a sample of 5 rows from the first table.',
  'What is the distribution of values in the most common column?',
];

interface Props {
  onSubmit: (question: string) => void;
  disabled?: boolean;
  status?: string;
  initialQuestion?: string;
}

export function QueryInput({ onSubmit, disabled, status, initialQuestion }: Props) {
  const [question, setQuestion] = useState(initialQuestion ?? '');

  function handleSubmit() {
    if (question.trim()) onSubmit(question.trim());
  }

  function handleKey(e: React.KeyboardEvent) {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) handleSubmit();
  }

  return (
    <div className="query-input">
      <div className="examples">
        {EXAMPLE_QUESTIONS.map((q, i) => (
          <button
            key={i}
            className="example-chip"
            onClick={() => setQuestion(q)}
            disabled={disabled}
          >
            {q}
          </button>
        ))}
      </div>
      <div className="input-row">
        <textarea
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          onKeyDown={handleKey}
          placeholder="Ask a question about your data… (Ctrl+Enter to submit)"
          rows={3}
          disabled={disabled}
        />
        <button onClick={handleSubmit} disabled={disabled || !question.trim()}>
          {status === 'generating' ? `Generating (attempt ${status})…` :
           status === 'analyzing' ? 'Analyzing…' :
           disabled ? 'Waiting…' : 'Ask →'}
        </button>
      </div>
    </div>
  );
}
