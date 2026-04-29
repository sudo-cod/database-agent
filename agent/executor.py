import re
import time
from dataclasses import dataclass, field

import duckdb
import pandas as pd

from sql_generator import SQLGenerator, GenerationResult


@dataclass
class AttemptLog:
    attempt_number: int
    sql: str
    success: bool
    error: str | None
    rows_returned: int | None
    duration_ms: float
    prompt_tokens: int
    completion_tokens: int


@dataclass
class ExecutionResult:
    question: str
    success: bool
    df: pd.DataFrame | None
    final_sql: str
    attempts: list = field(default_factory=list)
    unanswerable: bool = False
    total_duration_ms: float = 0.0

    @property
    def total_tokens(self):
        return sum(a.prompt_tokens + a.completion_tokens for a in self.attempts)

    @property
    def attempt_count(self):
        return len(self.attempts)


class SQLExecutor:
    def __init__(self, generator: SQLGenerator, db_path: str, max_retries: int = 3, row_limit: int = 10000):
        self.generator = generator
        self.db_path = db_path
        self.max_retries = max_retries
        self.row_limit = row_limit

    def _inject_limit(self, sql: str) -> str:
        upper = sql.upper().strip().rstrip(";")
        if re.search(r"\bLIMIT\s+\d+\s*$", upper):
            return sql
        if upper.startswith("SELECT") or upper.startswith("WITH"):
            return f"SELECT * FROM ({sql}) __q LIMIT {self.row_limit}"
        return sql

    def _run_sql(self, sql: str):
        con = duckdb.connect(self.db_path, read_only=True)
        try:
            t0 = time.perf_counter()
            df = con.execute(self._inject_limit(sql)).df()
            return df, (time.perf_counter() - t0) * 1000
        finally:
            con.close()

    def run(self, question: str) -> ExecutionResult:
        t_start = time.perf_counter()
        attempts = []
        previous_sql = None
        previous_error = None

        for attempt_num in range(1, self.max_retries + 1):
            gen = self.generator.generate(question, previous_sql, previous_error)

            if self.generator.is_unanswerable(gen):
                attempts.append(AttemptLog(
                    attempt_num, gen.sql, False, "Unanswerable", None, 0,
                    gen.prompt_tokens, gen.completion_tokens
                ))
                return ExecutionResult(
                    question, False, None, gen.sql, attempts,
                    unanswerable=True,
                    total_duration_ms=(time.perf_counter() - t_start) * 1000
                )

            try:
                df, duration_ms = self._run_sql(gen.sql)
                attempts.append(AttemptLog(
                    attempt_num, gen.sql, True, None, len(df),
                    duration_ms, gen.prompt_tokens, gen.completion_tokens
                ))
                return ExecutionResult(
                    question, True, df, gen.sql, attempts,
                    total_duration_ms=(time.perf_counter() - t_start) * 1000
                )
            except Exception as e:
                error_str = str(e)
                attempts.append(AttemptLog(
                    attempt_num, gen.sql, False, error_str, None, 0,
                    gen.prompt_tokens, gen.completion_tokens
                ))
                previous_sql = gen.sql
                previous_error = error_str

        return ExecutionResult(
            question, False, None, previous_sql or "", attempts,
            total_duration_ms=(time.perf_counter() - t_start) * 1000
        )
