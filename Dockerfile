FROM quay.io/astronomer/astro-runtime:12.6.0

RUN python -m venv .dbt && source .dbt/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate