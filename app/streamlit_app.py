import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'agent'))

import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'olist.duckdb')

EXAMPLE_QUESTIONS = [
    "How many orders were placed each month in 2017?",
    "What are the top 10 product categories by total revenue?",
    "Which states have the highest average order value?",
    "What is the average delivery time in days by seller state?",
    "Which payment methods are most popular for orders over R$500?",
    "Which sellers have the best average review scores with at least 50 orders?",
    "What product categories have the highest freight-to-price ratio?",
    "How does order volume vary by day of week?",
    "What is the monthly revenue trend across 2017 and 2018?",
    "Which states have the most late deliveries as a percentage?",
]

BACKEND_ENV_MAP = {
    "deepseek": "DEEPSEEK_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
}

BACKEND_LABEL_MAP = {
    "deepseek": "DeepSeek API Key",
    "anthropic": "Anthropic API Key",
    "openai": "OpenAI API Key",
}

st.set_page_config(page_title="Olist SQL Agent", layout="wide")


def db_exists():
    return os.path.exists(DB_PATH)


@st.cache_resource
def get_agent(db_path: str, backend: str, api_key: str):
    env_var = BACKEND_ENV_MAP[backend]
    os.environ[env_var] = api_key

    from agent import SQLAgent
    schema_cache = os.path.join(os.path.dirname(__file__), '..', 'data', 'schema_cache.json')
    return SQLAgent.from_db(db_path, backend=backend, schema_cache=schema_cache)


@st.cache_data
def get_table_summary(db_path: str):
    con = duckdb.connect(db_path, read_only=True)
    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
    ).fetchall()]
    rows = []
    for t in tables:
        count = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        col_count = con.execute(
            f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name='{t}'"
        ).fetchone()[0]
        rows.append({"Table": t, "Rows": f"{count:,}", "Columns": col_count})
    con.close()
    return pd.DataFrame(rows)


def render_chart(df: pd.DataFrame, chart_type: str, x_col: str, y_col: str):
    if x_col not in df.columns or y_col not in df.columns:
        return
    try:
        if chart_type == "bar":
            fig = px.bar(df, x=x_col, y=y_col)
        elif chart_type == "line":
            fig = px.line(df, x=x_col, y=y_col)
        elif chart_type == "pie":
            fig = px.pie(df, names=x_col, values=y_col)
        elif chart_type == "scatter":
            fig = px.scatter(df, x=x_col, y=y_col)
        else:
            return
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not render chart: {e}")


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Olist SQL Agent")
    st.markdown("---")

    backend = st.radio("Backend", ["deepseek", "anthropic", "openai"], index=0)

    env_key = BACKEND_ENV_MAP[backend]
    default_key = os.environ.get(env_key, "")
    api_key = st.text_input(BACKEND_LABEL_MAP[backend], value=default_key, type="password")

    st.markdown(f"**Database:** `data/olist.duckdb`")

    if st.button("Reload Schema"):
        schema_cache = os.path.join(os.path.dirname(__file__), '..', 'data', 'schema_cache.json')
        if os.path.exists(schema_cache):
            os.remove(schema_cache)
        get_agent.clear()
        st.success("Schema cache cleared — will reload on next query.")

    if db_exists():
        with st.expander("Schema"):
            try:
                summary_df = get_table_summary(DB_PATH)
                for _, row in summary_df.iterrows():
                    st.markdown(f"**{row['Table']}** — {row['Rows']} rows, {row['Columns']} cols")
            except Exception as e:
                st.error(f"Cannot read schema: {e}")

# ── DB missing guard ──────────────────────────────────────────────────────────

if not db_exists():
    st.warning(
        "**Database not found.**\n\n"
        "Run the pipeline first:\n"
        "```bash\n"
        "docker compose exec spark python spark/run_pipeline.py\n"
        "docker compose exec spark python warehouse/load_duckdb.py\n"
        "```"
    )
    st.stop()

# ── Main tabs ─────────────────────────────────────────────────────────────────

tab_ask, tab_status, tab_explorer = st.tabs(["Ask a Question", "Pipeline Status", "Data Explorer"])

# ── Tab 1: Ask ────────────────────────────────────────────────────────────────

with tab_ask:
    st.subheader("Natural Language Query")

    if "question_input" not in st.session_state:
        st.session_state["question_input"] = ""

    question = st.text_area(
        "Your question",
        value=st.session_state["question_input"],
        height=80,
        placeholder="e.g. What are the top 10 product categories by revenue?",
        key="question_text_area",
    )

    st.markdown("**Example questions:**")
    cols = st.columns(2)
    for i, eq in enumerate(EXAMPLE_QUESTIONS):
        if cols[i % 2].button(eq, key=f"eq_{i}"):
            st.session_state["question_input"] = eq
            st.rerun()

    run_disabled = not api_key.strip()
    run_btn = st.button(
        "Run Query",
        disabled=run_disabled,
        help="Enter an API key in the sidebar to enable queries." if run_disabled else None,
    )

    if run_btn and question.strip():
        with st.spinner("Thinking…"):
            try:
                agent = get_agent(DB_PATH, backend, api_key.strip())
                response = agent.ask(question.strip())
            except Exception as e:
                st.error(f"Agent error: {e}")
                st.stop()

        if response.success:
            n_rows = len(response.df)
            ms = int(response.result.total_duration_ms)
            st.success(f"{n_rows} rows returned in {ms}ms")

            if response.result.attempt_count > 1:
                st.warning(
                    f"Self-repair triggered — query failed on attempt "
                    f"{response.result.attempt_count - 1}, succeeded on attempt "
                    f"{response.result.attempt_count}."
                )

            with st.expander("SQL Query"):
                st.code(response.sql, language="sql")

            if response.analysis and response.analysis.chart_recommendation:
                render_chart(
                    response.df,
                    response.analysis.chart_recommendation,
                    response.analysis.chart_x_col,
                    response.analysis.chart_y_col,
                )

            st.dataframe(response.df, use_container_width=True)

            if response.analysis:
                st.info(response.analysis.summary)

                if response.analysis.follow_up_questions:
                    st.markdown("**Follow-up questions:**")
                    fu_cols = st.columns(2)
                    for j, fu in enumerate(response.analysis.follow_up_questions):
                        if fu_cols[j % 2].button(fu, key=f"fu_{j}"):
                            st.session_state["question_input"] = fu
                            st.rerun()

        elif response.result.unanswerable:
            st.error("This question cannot be answered with the available data.")
        else:
            last_error = response.result.attempts[-1].error if response.result.attempts else "Unknown error"
            st.error(f"Query failed after {response.result.attempt_count} attempt(s): {last_error}")

            with st.expander("Attempt Log"):
                for attempt in response.result.attempts:
                    st.markdown(f"**Attempt {attempt.attempt_number}** — {'✓' if attempt.success else '✗'}")
                    st.code(attempt.sql, language="sql")
                    if attempt.error:
                        st.error(attempt.error)

    elif run_btn and not question.strip():
        st.warning("Please enter a question.")

# ── Tab 2: Pipeline Status ────────────────────────────────────────────────────

with tab_status:
    st.subheader("Pipeline Status")

    try:
        status_df = get_table_summary(DB_PATH)
        st.dataframe(status_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Cannot read database: {e}")

    if st.button("Run sanity check"):
        with st.spinner("Running…"):
            try:
                con = duckdb.connect(DB_PATH, read_only=True)
                result = con.execute("""
                    SELECT COUNT(*) as joined_rows
                    FROM orders o
                    JOIN order_items oi ON o.order_id = oi.order_id
                """).fetchone()
                con.close()
                st.success(f"orders JOIN order_items: {result[0]:,} rows")
            except Exception as e:
                st.error(f"Sanity check failed: {e}")

# ── Tab 3: Data Explorer ──────────────────────────────────────────────────────

with tab_explorer:
    st.subheader("Data Explorer")

    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        table_list = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()]
        con.close()
    except Exception as e:
        st.error(f"Cannot list tables: {e}")
        table_list = []

    if table_list:
        selected_table = st.selectbox("Select table", table_list)
        n_preview = st.slider("Rows to preview", min_value=10, max_value=500, value=50, step=10)

        try:
            con = duckdb.connect(DB_PATH, read_only=True)
            preview_df = con.execute(f'SELECT * FROM "{selected_table}" LIMIT {n_preview}').df()
            con.close()

            numeric_cols = preview_df.select_dtypes(include="number").columns.tolist()
            if numeric_cols:
                st.markdown("**Numeric column statistics:**")
                st.dataframe(preview_df[numeric_cols].describe(), use_container_width=True)

            st.markdown(f"**Preview ({n_preview} rows):**")
            st.dataframe(preview_df, use_container_width=True)
        except Exception as e:
            st.error(f"Cannot load table: {e}")
