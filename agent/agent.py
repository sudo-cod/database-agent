import time
import logging
from dataclasses import dataclass

from schema_extractor import SchemaMeta, extract_schema, load_schema_json, save_schema_json
from sql_generator import SQLGenerator
from executor import SQLExecutor, ExecutionResult
from analyzer import Analyzer, AnalysisResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


@dataclass
class AgentResponse:
    question: str
    result: ExecutionResult
    analysis: AnalysisResult | None
    wall_time_ms: float

    @property
    def success(self):
        return self.result.success

    @property
    def df(self):
        return self.result.df

    @property
    def sql(self):
        return self.result.final_sql

    @property
    def summary(self):
        if self.analysis:
            return self.analysis.summary
        if self.result.unanswerable:
            return "Cannot answer with available data."
        last = self.result.attempts[-1].error if self.result.attempts else "Unknown"
        return f"Failed after {self.result.attempt_count} attempt(s). Last error: {last}"


class SQLAgent:
    def __init__(
        self,
        schema: SchemaMeta,
        db_path: str,
        backend: str = "deepseek",
        max_retries: int = 3,
        sql_model: str = None,
        analyze: bool = True,
    ):
        self.schema = schema
        self.db_path = db_path
        self.analyze_enabled = analyze
        self.generator = SQLGenerator(schema, backend=backend, model=sql_model)
        self.executor = SQLExecutor(self.generator, db_path, max_retries=max_retries)
        self.analyzer = Analyzer(backend=backend)

    @classmethod
    def from_db(cls, db_path: str, backend: str = "deepseek", schema_cache: str = None, **kwargs):
        if schema_cache:
            try:
                schema = load_schema_json(schema_cache)
                log.info(f"Schema loaded from cache: {schema_cache}")
            except FileNotFoundError:
                schema = extract_schema(db_path)
                save_schema_json(schema, schema_cache)
        else:
            schema = extract_schema(db_path)

        log.info(f"{len(schema.tables)} tables: {[t.name for t in schema.tables]}")
        return cls(schema=schema, db_path=db_path, backend=backend, **kwargs)

    def ask(self, question: str, verbose: bool = False) -> AgentResponse:
        t0 = time.perf_counter()
        log.info(f"Q: {question!r}")

        result = self.executor.run(question)
        analysis = None
        if result.success and self.analyze_enabled and result.df is not None:
            analysis = self.analyzer.analyze(question, result.final_sql, result.df)

        wall_ms = (time.perf_counter() - t0) * 1000
        status = f"✓ {len(result.df)} rows" if result.success else f"✗ {result.attempt_count} attempts"
        log.info(f"{status} | {wall_ms:.0f}ms | {result.total_tokens} tokens")

        return AgentResponse(question, result, analysis, wall_ms)

    def ask_batch(self, questions: list) -> list:
        return [self.ask(q) for q in questions]
