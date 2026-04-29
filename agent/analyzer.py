import os
import json
import re

import pandas as pd
from dataclasses import dataclass


@dataclass
class AnalysisResult:
    summary: str
    follow_up_questions: list
    chart_recommendation: str | None
    chart_x_col: str | None
    chart_y_col: str | None


ANALYSIS_SYSTEM = """\
You are a data analyst. Given a SQL query and its results, respond ONLY with valid JSON:
{
  "summary": "2-4 sentence plain language insight",
  "follow_up_questions": ["...", "..."],
  "chart_recommendation": "bar"|"line"|"pie"|"scatter"|null,
  "chart_x_col": "<exact column name>"|null,
  "chart_y_col": "<exact column name>"|null
}
chart rules: bar=categorical x + numeric y, line=time x + numeric y,
pie=categorical proportions (max 8 categories), scatter=two numeric columns, null=other.
"""


def _df_context(df: pd.DataFrame, max_rows: int = 20) -> str:
    preview = df.head(max_rows)
    lines = [",".join(str(c) for c in preview.columns)]
    for _, row in preview.iterrows():
        lines.append(",".join(str(v) for v in row))
    result = "\n".join(lines)
    if len(df) > max_rows:
        result += f"\n...({len(df) - max_rows} more rows)"
    return result


def _parse_json_response(raw: str) -> dict:
    cleaned = re.sub(r"```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    cleaned = cleaned.replace("```", "").strip()
    return json.loads(cleaned)


class Analyzer:
    def __init__(self, backend: str = "deepseek"):
        self.backend = backend

    def _call(self, prompt: str) -> str:
        if self.backend == "openai":
            from openai import OpenAI
            client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            r = client.chat.completions.create(
                model="gpt-4o-mini", temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": ANALYSIS_SYSTEM},
                    {"role": "user", "content": prompt},
                ]
            )
            return r.choices[0].message.content or "{}"

        elif self.backend == "anthropic":
            import anthropic
            client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
            r = client.messages.create(
                model="claude-haiku-4-5-20251001", max_tokens=1024,
                system=ANALYSIS_SYSTEM,
                messages=[{"role": "user", "content": prompt}]
            )
            return r.content[0].text if r.content else "{}"

        elif self.backend == "deepseek":
            from openai import OpenAI
            client = OpenAI(
                api_key=os.environ["DEEPSEEK_API_KEY"],
                base_url="https://api.deepseek.com"
            )
            r = client.chat.completions.create(
                model="deepseek-chat", temperature=0,
                messages=[
                    {"role": "system", "content": ANALYSIS_SYSTEM},
                    {"role": "user", "content": prompt},
                ]
            )
            return r.choices[0].message.content or "{}"

        raise ValueError(f"Unknown backend: {self.backend}")

    def analyze(self, question: str, sql: str, df: pd.DataFrame) -> AnalysisResult:
        if df.empty:
            return AnalysisResult("Query returned no results.", [], None, None, None)

        prompt = (
            f"### QUESTION\n{question}\n\n"
            f"### SQL\n{sql}\n\n"
            f"### RESULT\n{len(df)} rows × {len(df.columns)} cols\n"
            f"Types: { {c: str(df[c].dtype) for c in df.columns} }\n\n"
            f"### DATA\n{_df_context(df)}\n\nRespond with JSON now."
        )
        try:
            raw = self._call(prompt)
            data = _parse_json_response(raw)
            return AnalysisResult(
                data.get("summary", ""),
                data.get("follow_up_questions", []),
                data.get("chart_recommendation"),
                data.get("chart_x_col"),
                data.get("chart_y_col"),
            )
        except Exception as e:
            return AnalysisResult(f"Analysis unavailable: {e}", [], None, None, None)
