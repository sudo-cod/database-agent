import type { AgentSettings, LLMBackend } from '../types';

const BACKEND_LABELS: Record<LLMBackend, string> = {
  anthropic: 'Anthropic (claude-sonnet-4-6)',
  openai: 'OpenAI (gpt-4o)',
  deepseek: 'DeepSeek (deepseek-chat)',
};

interface Props {
  settings: AgentSettings;
  onChange: (s: AgentSettings) => void;
}

export function SettingsBar({ settings, onChange }: Props) {
  return (
    <div className="settings-bar">
      <select
        value={settings.backend}
        onChange={(e) => onChange({ ...settings, backend: e.target.value as LLMBackend })}
      >
        {(Object.keys(BACKEND_LABELS) as LLMBackend[]).map((b) => (
          <option key={b} value={b}>{BACKEND_LABELS[b]}</option>
        ))}
      </select>
      <input
        type="password"
        placeholder="API Key"
        value={settings.apiKey}
        onChange={(e) => onChange({ ...settings, apiKey: e.target.value })}
        autoComplete="off"
      />
    </div>
  );
}
