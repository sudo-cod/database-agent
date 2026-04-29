import json
import duckdb
from dataclasses import dataclass, asdict


@dataclass
class ColumnMeta:
    name: str
    dtype: str
    nullable: bool
    sample_values: list


@dataclass
class TableMeta:
    name: str
    row_count: int
    columns: list
    foreign_key_hints: list


@dataclass
class SchemaMeta:
    tables: list
    db_path: str


def _infer_foreign_keys(tables):
    col_index = {}
    for table in tables:
        for col in table.columns:
            col_index.setdefault(col.name, []).append(table.name)
    hints = {t.name: [] for t in tables}
    for col_name, table_names in col_index.items():
        if len(table_names) > 1:
            for i, t1 in enumerate(table_names):
                for t2 in table_names[i + 1:]:
                    hints[t1].append(f"{col_name} -> {t2}.{col_name}")
                    hints[t2].append(f"{col_name} -> {t1}.{col_name}")
    return hints


def extract_schema(db_path, sample_n=3):
    con = duckdb.connect(db_path, read_only=True)
    table_names = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()]

    raw_tables = []
    for tname in table_names:
        row_count = con.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
        cols_info = con.execute(
            f"SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            f"WHERE table_name='{tname}' ORDER BY ordinal_position"
        ).fetchall()
        columns = []
        for col_name, dtype, nullable_str in cols_info:
            try:
                samples = con.execute(
                    f'SELECT DISTINCT "{col_name}" FROM "{tname}" WHERE "{col_name}" IS NOT NULL LIMIT {sample_n}'
                ).fetchall()
                sample_values = [str(r[0]) for r in samples]
            except Exception:
                sample_values = []
            columns.append(ColumnMeta(col_name, dtype, nullable_str.upper() == "YES", sample_values))
        raw_tables.append(TableMeta(tname, row_count, columns, []))

    con.close()
    fk_hints = _infer_foreign_keys(raw_tables)
    for table in raw_tables:
        table.foreign_key_hints = fk_hints.get(table.name, [])

    return SchemaMeta(raw_tables, db_path)


def schema_to_prompt_text(schema):
    lines = ["### DATABASE SCHEMA\n"]
    for table in schema.tables:
        lines.append(f"TABLE: {table.name}  ({table.row_count:,} rows)")
        for col in table.columns:
            nullable_flag = "nullable" if col.nullable else "not null"
            samples_str = ", ".join(col.sample_values) if col.sample_values else "—"
            lines.append(f"  {col.name}  [{col.dtype}, {nullable_flag}]  samples: {samples_str}")
        if table.foreign_key_hints:
            lines.append("  -- join hints: " + " | ".join(table.foreign_key_hints))
        lines.append("")
    return "\n".join(lines)


def save_schema_json(schema, output_path):
    with open(output_path, "w") as f:
        json.dump(asdict(schema), f, indent=2)


def load_schema_json(path):
    with open(path) as f:
        data = json.load(f)
    tables = [
        TableMeta(
            t["name"], t["row_count"],
            [ColumnMeta(**c) for c in t["columns"]],
            t["foreign_key_hints"]
        )
        for t in data["tables"]
    ]
    return SchemaMeta(tables, data["db_path"])
