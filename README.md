# Fraud Detection MLOps Pipeline

A complete MLOps solution for real-time fraud detection, integrating data ingestion, model training, and inference with Kafka, Airflow, and MLflow.

## Overview

This project implements an end-to-end MLOps pipeline for fraud detection that features:
- Real-time data streaming with Apache Kafka
- Automated model training with Apache Airflow
- Experiment tracking with MLflow
- Real-time inference with PySpark
- Support for multiple ML frameworks (XGBoost, LightGBM, CATBoost)

## Features

- **Data Pipeline**: Kafka streaming, Airflow orchestration, SMOTE class balancing
- **Machine Learning**: Multi-framework support, hyperparameter tuning, experiment tracking
- **Real-time Inference**: PySpark streaming, automated deployment, result streaming

## Technology Stack

- **Orchestration**: Apache Airflow
- **Streaming**: Confluent Kafka, PySpark  
- **ML Frameworks**: XGBoost, LightGBM, CATBoost
- **Experiment Tracking**: MLflow
- **Containerization**: Docker, Docker Compose
- **Language**: Python 3.10+

## Quick Start

1. **Clone the repository**:
   ```bash
   git clone https://github.com/kanewyp/fraud-detection-mlops.git
   cd fraud-detection-mlops
   ```

2. **Set up environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Start services**:
   ```bash
   cd src
   docker-compose --profile flower up -d --build
   ```

4. **Access services**:
   - Airflow: http://localhost:8080
   - MLflow: http://localhost:5500
   - MinIO http://localhost:9000


## Project Structure

```
fraud-detection-mlops/
├── .git/                        # Git repository
├── .gitignore                   # Git ignore patterns
├── .python-version              # Python version specification
├── .venv/                       # Python virtual environment
├── README.md                    # Project documentation
├── src/                         # Source code directory
│   ├── .env                     # Environment variables
│   ├── config.yaml              # Central configuration
│   ├── docker-compose.yml       # Services orchestration
│   ├── init-multiple-dbs.sh     # Database initialization script
│   ├── wait-for-it.sh          # Service dependency wait script
│   ├── airflow/                 # Airflow service configuration
│   │   ├── Dockerfile           # Airflow container definition
│   │   └── requirements.txt     # Airflow Python dependencies
│   ├── config/                  # Additional configuration files
│   ├── dags/                    # Airflow DAGs for training pipeline
│   │   ├── fraud_detection_training_dag.py  # Main DAG definition
│   │   ├── fraud_detection_training.py      # Training logic
│   │   └── __pycache__/         # Python bytecode cache
│   ├── inference/               # Spark streaming inference service
│   │   ├── Dockerfile           # Inference container definition
│   │   ├── main.py              # Spark inference application
│   │   └── requirements.txt     # Inference Python dependencies
│   ├── logs/                    # Airflow execution logs
│   │   ├── dag_id=fraud_detection_training/  # DAG-specific logs
│   │   ├── dag_processor_manager/            # DAG processor logs
│   │   └── scheduler/                        # Scheduler logs
│   ├── mlflow/                  # MLflow server configuration
│   │   ├── Dockerfile           # MLflow container definition
│   │   └── requirements.txt     # MLflow Python dependencies
│   ├── models/                  # Model artifacts storage
│   │   └── fraud_detection_model.pkl        # Trained XGBoost model
│   ├── plugins/                 # custom plugins (empty)
│   └── producer/                # Kafka producer for test data
│       ├── Dockerfile           # Producer container definition
│       ├── main.py              # Kafka producer application
│       └── requirements.txt     # Producer Python dependencies
└── test.py                      # Test script (if present)
```
