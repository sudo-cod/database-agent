interface Props {
  questions: string[];
  onSelect: (q: string) => void;
}

export function FollowUpChips({ questions, onSelect }: Props) {
  if (questions.length === 0) return null;
  return (
    <div className="follow-up">
      <span className="follow-up-label">Follow-up questions:</span>
      <div className="chips">
        {questions.map((q, i) => (
          <button key={i} className="chip" onClick={() => onSelect(q)}>
            {q}
          </button>
        ))}
      </div>
    </div>
  );
}
