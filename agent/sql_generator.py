import os
import re
from dataclasses import dataclass
from schema_extractor import schema_to_prompt_text


@dataclass
class GenerationResult:
    sql: str
    raw_response: str
    model: str
    prompt_tokens: int
    completion_tokens: int


SYSTEM_PROMPT = """\
Write a single correct DuckDB SQL query to answer the question.
Rules:
- Output ONLY raw SQL. No markdown, no explanation, no preamble.
- Qualify all column names with table names when joining.
- Use double quotes for identifiers with spaces.
- Do not add LIMIT unless the question asks for top-N.
- If unanswerable with the schema, output exactly: UNANSWERABLE
- DuckDB supports CTEs, window functions, date_diff, strftime, QUALIFY.
- For date extraction use: EXTRACT(month FROM col), strftime(col, '%Y-%m'), date_trunc('month', col)
"""


def build_user_prompt(question, schema_text, previous_sql=None, error_message=None):
    parts = [schema_text, f"\n### QUESTION\n{question}"]
    if previous_sql and error_message:
        parts.append(
            f"\n### PREVIOUS ATTEMPT FAILED\nSQL:\n{previous_sql}\nError:\n{error_message}\n"
            f"Write a corrected SQL query."
        )
    parts.append("\n### SQL QUERY")
    return "\n".join(parts)


def extract_sql(raw):
    cleaned = re.sub(r"```(?:sql)?\s*", "", raw, flags=re.IGNORECASE)
    return cleaned.replace("```", "").strip()


def _call_anthropic(system, user, model="claude-sonnet-4-6"):
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    response = client.messages.create(
        model=model, max_tokens=1024, system=system,
        messages=[{"role": "user", "content": user}]
    )
    raw = response.content[0].text if response.content else ""
    return GenerationResult(
        extract_sql(raw), raw, model,
        response.usage.input_tokens, response.usage.output_tokens
    )


def _call_openai(system, user, model="gpt-4o"):
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    response = client.chat.completions.create(
        model=model, temperature=0,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}]
    )
    raw = response.choices[0].message.content or ""
    return GenerationResult(
        extract_sql(raw), raw, model,
        response.usage.prompt_tokens, response.usage.completion_tokens
    )


def _call_deepseek(system, user, model="deepseek-chat"):
    from openai import OpenAI
    client = OpenAI(
        api_key=os.environ["DEEPSEEK_API_KEY"],
        base_url="https://api.deepseek.com"
    )
    response = client.chat.completions.create(
        model=model, temperature=0,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}]
    )
    raw = response.choices[0].message.content or ""
    return GenerationResult(
        extract_sql(raw), raw, model,
        response.usage.prompt_tokens, response.usage.completion_tokens
    )


class SQLGenerator:
    def __init__(self, schema, backend="deepseek", model=None):
        self.schema = schema
        self.schema_text = schema_to_prompt_text(schema)
        self.backend = backend
        self.model = model or {
            "anthropic": "claude-sonnet-4-6",
            "openai": "gpt-4o",
            "deepseek": "deepseek-chat",
        }.get(backend, "deepseek-chat")

    def generate(self, question, previous_sql=None, error_message=None):
        user_prompt = build_user_prompt(question, self.schema_text, previous_sql, error_message)
        if self.backend == "openai":
            return _call_openai(SYSTEM_PROMPT, user_prompt, self.model)
        elif self.backend == "anthropic":
            return _call_anthropic(SYSTEM_PROMPT, user_prompt, self.model)
        elif self.backend == "deepseek":
            return _call_deepseek(SYSTEM_PROMPT, user_prompt, self.model)
        else:
            raise ValueError(f"Unknown backend: {self.backend}")

    def is_unanswerable(self, result):
        return result.sql.strip().upper() == "UNANSWERABLE"
