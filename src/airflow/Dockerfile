FROM apache/airflow:2.10.5

# create models directory and set ownership
USER root
RUN mkdir -p /app/models

# Install system dependencies required by LightGBM
RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# copy requirement file
COPY requirements.txt /app/requirements.txt

# install Python packages at root (system-wide)
RUN pip install -r /app/requirements.txt

# switch back to airflow user for safety
USER airflow

