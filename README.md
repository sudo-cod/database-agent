# SQL Intelligence Agent

A multi-table SQL intelligence agent that works with **any CSV files**. Upload your data, ask questions in plain English, and get SQL queries, charts, and analysis — all running in your browser with no backend server.

**Live site:** https://sudo-cod.github.io/database-agent/

## How it works

```
Browser
├── Upload CSV files → DuckDB WASM (in-browser SQL engine)
├── Type a question → LLM generates SQL → DuckDB executes
├── Self-repair: if SQL fails, the error is fed back to the LLM (up to 3 retries)
└── Results: table, chart, plain-language summary, follow-up questions
```

Everything runs client-side. Your data never leaves your browser.

## Using the site

1. Open https://sudo-cod.github.io/database-agent/
2. Select a backend and paste your API key (Anthropic, OpenAI, or DeepSeek)
3. Upload one or more CSV files — they become queryable tables instantly
4. Ask any question about your data in plain English

## LLM Backends

| Backend   | Model             | Get a key |
|-----------|-------------------|-----------|
| Anthropic | claude-sonnet-4-6 | console.anthropic.com |
| OpenAI    | gpt-4o            | platform.openai.com |
| DeepSeek  | deepseek-chat     | platform.deepseek.com |

API keys are held in memory only — never stored or sent anywhere except the LLM provider.

## Project Structure

```
frontend/
│   src/lib/
│   ├── duckdb.ts           ← DuckDB WASM init, CSV loader, query runner
│   ├── schemaExtractor.ts  ← table/column introspection + FK inference
│   ├── llm.ts              ← fetch-based LLM calls (Anthropic / OpenAI / DeepSeek)
│   ├── sqlGenerator.ts     ← prompt builder + SQL extraction
│   ├── executor.ts         ← self-repair retry loop
│   └── analyzer.ts         ← result analysis + chart recommendations
```

## Local Development

```bash
cd frontend
npm install
npm run dev       # starts at http://localhost:5173/database-agent/
npm run build     # production build → frontend/dist/
```

## Deployment

Pushing any change under `frontend/` to `main` triggers the GitHub Actions workflow which builds and deploys to GitHub Pages automatically.

