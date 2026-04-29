FROM jupyter/pyspark-notebook:latest

USER root

RUN pip install --no-cache-dir \
    duckdb>=0.10.0 \
    anthropic>=0.25.0 \
    openai>=1.30.0 \
    streamlit>=1.35.0 \
    plotly>=5.22.0 \
    pyarrow>=14.0.0

USER jovyan
