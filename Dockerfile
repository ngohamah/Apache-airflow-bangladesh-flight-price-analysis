FROM apache/airflow:2.9.3-python3.11

COPY requirements.txt /requirements.txt

# Pin against Airflow's own constraints file so pip resolves quickly instead of
# backtracking through Airflow's dependency graph for every extra package.
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
