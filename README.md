# Fraud Detection MLOps Pipeline

An enterprise-grade, end-to-end MLOps solution for real-time credit card fraud detection with comprehensive feature engineering, multi-framework support, and production-ready streaming inference.

## 🎯 Overview

This project implements a sophisticated MLOps pipeline for fraud detection featuring:

### 🔥 Core Capabilities
- **Real-time Data Streaming**: Confluent Kafka integration with SASL_SSL security
- **Advanced Feature Engineering**: 30+ engineered features including behavioral, temporal, and fraud pattern detection
- **Multi-Framework ML Support**: XGBoost, LightGBM, and CatBoost with automated hyperparameter tuning
- **Production-Ready Inference**: Apache Spark Structured Streaming with sub-second latency
- **Comprehensive MLOps**: Experiment tracking, model versioning, and automated deployment
- **Enterprise Monitoring**: Airflow orchestration with health checks and alerting

### 🛡️ Fraud Detection Features
The system implements **6 sophisticated fraud detection patterns**:

1. **Account Takeover Detection**: High amounts + high-risk merchants + geographic anomalies
2. **Card Testing Detection**: Micro-transactions with verification failures  
3. **Merchant Collusion**: Large amounts with legitimate-appearing verification
4. **Geographic Anomaly**: High-risk countries with address verification failures
5. **Expired Card Fraud**: Expired cards with manual entry and CVV failures
6. **Verification Bypass**: Multiple verification failures with online transactions

### 📊 Feature Engineering (30+ Features)
- **Temporal Features**: Hour-of-day, weekend/night patterns, transaction timing
- **Behavioral Features**: 24-hour activity windows, 7-day rolling averages, velocity tracking
- **Risk Scoring**: Merchant risk, geographic risk, verification scores
- **Anomaly Detection**: Amount outliers, user behavior deviations, currency risks
- **Pattern Recognition**: Targeted fraud signatures, transaction sequencing

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- 8GB+ RAM recommended
- Confluent Kafka cluster (or update config for local Kafka)

### 1. Clone & Setup
```bash
git clone https://github.com/kanewyp/fraud-detection-mlops.git
cd fraud-detection-mlops

# Create Python environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install dependencies (for each subfolder)
pip install -r requirements.txt
```


### 2. Start Services
```bash
# Start all services (includes Flower UI)
docker-compose --profile flower up -d --build

# Or start core services only
docker-compose up -d --build
```

### 3. Access Applications
| Service | URL | Purpose |
|---------|-----|---------|
| **Airflow** | http://localhost:8080 | Workflow orchestration & monitoring |
| **MLflow** | http://localhost:5500 | Experiment tracking & model registry |
| **MinIO** | http://localhost:9000 | Object storage (s3-compatible) |
| **Flower** | http://localhost:5555 | Celery task monitoring |

**Default Credentials**: `admin/admin` for Airflow, `minioadmin/minioadmin` for MinIO


## 🏗️ Architecture

### System Architecture
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Kafka         │    │   Airflow        │    │   MLflow        │
│   Producer      │───▶│   Training       │───▶│   Tracking      │
│   (Synthetic    │    │   Pipeline       │    │   & Registry    │
│   Transactions) │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                       │
         │                        ▼                       │
         │              ┌──────────────────┐              │
         │              │   PostgreSQL     │              │
         │              │   (Airflow DB)   │              │
         │              └──────────────────┘              │
         │                                                │
         ▼                                                ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Spark         │    │   MinIO S3       │    │   Model         │
│   Streaming     │◀───│   Storage        │───▶│   Artifacts     │
│   Inference     │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │
         ▼
┌─────────────────┐
│   Kafka         │
│   Predictions   │
│   Topic         │
└─────────────────┘
```

### Data Flow
1. **Data Generation**: Kafka producer generates realistic credit card transactions with fraud patterns
2. **Model Training**: Airflow orchestrates daily training with feature engineering and hyperparameter tuning
3. **Experiment Tracking**: MLflow tracks all experiments, parameters, metrics, and model versions
4. **Model Registry**: Best models are automatically registered and versioned
5. **Real-time Inference**: Spark Structured Streaming processes transactions in real-time
6. **Fraud Detection**: Predictions are published to Kafka for downstream consumption

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow | Workflow management and scheduling |
| **ML Frameworks** | XGBoost, LightGBM, CatBoost | Model training with ensemble support |
| **Streaming** | Apache Spark + Kafka | Real-time data processing |
| **Experiment Tracking** | MLflow | Model lifecycle management |
| **Storage** | MinIO (S3-compatible) | Model artifacts and data storage |
| **Database** | PostgreSQL | Airflow metadata and model registry |
| **Message Queue** | Redis + Celery | Distributed task processing |
| **Data Generation** | Faker + Confluent Kafka | Realistic transaction simulation |
| **Containerization** | Docker + Docker Compose | Service orchestration |

## 📁 Project Structure

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
│   └── producer/                # Kafka producer for test data generation
│       ├── Dockerfile           # Producer container definition  
│       ├── main.py              # Kafka producer with fraud simulation
│       └── requirements.txt     # Producer Python dependencies
└── test.py                      # Test script (if present)
```

### Key Components

#### 🎭 **Producer Service** (`src/producer/`)
- **Purpose**: Generates realistic credit card transactions with embedded fraud patterns
- **Features**: 
  - 6 distinct fraud patterns with configurable rates (0.5% default fraud rate)
  - Realistic merchant categories, geographic distributions, payment methods
  - JSON schema validation and Decimal-to-float conversion
  - Confluent Kafka integration with SASL_SSL security

#### 🤖 **Training Pipeline** (`src/dags/`)
- **Purpose**: Automated model training and evaluation with MLflow tracking
- **Features**:
  - Multi-framework support (XGBoost, LightGBM, CatBoost)
  - 30+ engineered features with time-based rolling windows
  - Automated hyperparameter tuning with RandomizedSearchCV
  - SMOTE oversampling for class imbalance
  - Precision-recall optimization with custom F-beta scoring
  - Automatic model registration and versioning

#### ⚡ **Inference Service** (`src/inference/`)
- **Purpose**: Real-time fraud detection using Spark Structured Streaming
- **Features**:
  - Sub-second latency with Spark watermarking
  - Complete feature alignment with training pipeline
  - Pandas UDF for efficient batch prediction
  - Automatic fraud alert publishing to Kafka
  - Model loading with joblib/pickle fallback

#### 📊 **MLflow Server** (`src/mlflow/`)
- **Purpose**: Experiment tracking and model lifecycle management
- **Features**:
  - Automatic experiment logging (parameters, metrics, artifacts)
  - Model registry with version control
  - S3-compatible storage integration (MinIO)
  - Model serving capabilities
  - Comparison dashboards and metric visualization

## 🛠️ Configuration

### Core Configuration (`src/config.yaml`)
```yaml
# MLflow Configuration
mlflow:
  experiment_name: "fraud_detection_v2"
  tracking_uri: "http://mlflow-server:5500"
  s3_endpoint_url: "http://minio:9000"

# Kafka Configuration  
kafka:
  bootstrap_servers: "your-kafka-cluster.aws.confluent.cloud:9092"
  security_protocol: 'SASL_SSL'
  topic: "transactions_v2"
  output_topic: "fraud_predictions_v2"

# Model Configuration
model:
  framework: "lightgbm"  # Options: xgboost, catboost, lightgbm
  test_size: 0.2
  seed: 42

# Spark Configuration
spark:
  app_name: "FraudDetectionInference_v2"
  packages: "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,..."
```

### Environment Variables (`.env`)
```bash
# Kafka Credentials
KAFKA_BOOTSTRAP_SERVERS=your-kafka-cluster.confluent.cloud:9092
KAFKA_USERNAME=your-api-key
KAFKA_PASSWORD=your-api-secret

# AWS/MinIO Credentials  
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin

# Airflow Configuration
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=
```

## 🚦 Usage Guide

### Training a Model

1. **Access Airflow UI**: Navigate to http://localhost:8080
2. **Trigger DAG**: Enable and trigger `fraud_detection_training_v2`
3. **Monitor Progress**: Watch task execution and logs in real-time
4. **Check Results**: View experiment results in MLflow UI

### Running Inference

1. **Start Producer** (generates test data):
```bash
docker-compose exec producer python /app/main.py
```

2. **Start Inference** (processes transactions):
```bash
docker-compose exec inference python /app/main.py
```

3. **Monitor Predictions**: Check Kafka topic `fraud_predictions_v2` for alerts

### Viewing Experiments

1. **Access MLflow**: Navigate to http://localhost:5500
2. **Compare Runs**: View metrics, parameters, and model artifacts
3. **Download Models**: Export trained models for external use
4. **Model Registry**: Manage model versions and deployment stages

## 🎯 Machine Learning Details

### Feature Engineering (30+ Features)

#### **Temporal Features**
- `transaction_hour`: Hour of transaction (0-23)
- `is_night`: Night-time transactions (10 PM - 5 AM)  
- `is_weekend`: Weekend transactions
- `transaction_day`: Day of week
- `time_since_last_transaction`: Seconds since user's last transaction

#### **Behavioral Features**
- `user_activity_24h`: Transaction count in last 24 hours
- `rolling_avg_7d`: 7-day rolling average transaction amount
- `amount_to_avg_ratio`: Current amount vs user's average
- `high_velocity`: >3 transactions in 1 hour

#### **Risk Assessment Features**
- `merchant_risk`: High-risk merchant categories
- `verification_score`: Combined CVV/ZIP/address verification
- `high_risk_country`: Transactions from high-risk countries
- `is_card_expired`: Expired card usage
- `high_risk_entry_mode`: Manual/online entry modes

#### **Fraud Pattern Features**
- `card_testing_pattern`: Micro-transactions with verification failures
- `takeover_pattern`: High amounts + geographic + merchant risk
- `collusion_pattern`: Large amounts with legitimate verification
- `geo_anomaly_pattern`: Geographic inconsistencies
- `expired_card_pattern`: Expired cards with manual entry
- `verification_fail_pattern`: Multiple verification failures

#### **Anomaly Detection Features**
- `amount_outlier`: Statistical outliers based on user history
- `amount_z_score`: Z-score relative to user's spending patterns
- `non_usd_currency`: Non-USD transaction risk

### Model Training Process

1. **Data Ingestion**: Consumes transactions from Kafka with fraud labels
2. **Feature Engineering**: Applies 30+ feature transformations
3. **Data Preprocessing**: 
   - Ordinal encoding for categorical variables
   - SMOTE oversampling for class imbalance
   - Stratified train/test splitting
4. **Hyperparameter Tuning**: RandomizedSearchCV with 10 iterations
5. **Model Selection**: F-beta score optimization (beta=2 for recall focus)
6. **Threshold Optimization**: Precision-recall curve analysis
7. **Model Evaluation**: Comprehensive metrics and visualization
8. **Model Registration**: Automatic MLflow model registration

### Supported Algorithms

#### **XGBoost** (Gradient Boosting)
```yaml
xgboost_params:
  n_estimators: 300
  learning_rate: 0.05
  max_depth: 5
  subsample: 0.8
  colsample_bytree: 0.8
  scale_pos_weight: 1
```

#### **LightGBM** (Gradient Boosting)
```yaml
lightgbm_params:
  num_leaves: 31
  n_estimators: 300
  learning_rate: 0.05
  max_depth: 5
  subsample: 0.8
```

#### **CatBoost** (Gradient Boosting)
```yaml
catboost_params:
  iterations: 300
  learning_rate: 0.05
  depth: 5
  l2_leaf_reg: 3
```

## 🔧 Development & Deployment

### Performance Tuning

#### **Spark Optimization**
```yaml
spark:
  shuffle_partitions: 200  # Adjust based on data volume
  max_result_size: "2g"
  driver_memory: "4g"
  executor_memory: "4g"
```

#### **Kafka Optimization**
```yaml
kafka:
  batch_size: 16384
  linger_ms: 5
  compression_type: "snappy"
  acks: "1"
```

## 📝 API Reference

### Model Training API
```python
from fraud_detection_training import FraudDetectionTraining

# Initialize trainer
trainer = FraudDetectionTraining(config_path='config.yaml')

# Train model
model, metrics = trainer.train_model()

# Metrics returned
{
    'auc_pr': 0.95,
    'precision': 0.89,
    'recall': 0.91,
    'f1': 0.90,
    'threshold': 0.65
}
```

### Inference API
```python
from fraud_detection_inference import FraudDetectionInference

# Initialize inference
inference = FraudDetectionInference(config_path='config.yaml')

# Start streaming
inference.run_inference()
```

### MLflow Integration
```python
import mlflow

# Track experiment
with mlflow.start_run():
    mlflow.log_param("framework", "lightgbm")
    mlflow.log_metric("auc_pr", 0.95)
    mlflow.sklearn.log_model(model, "model")
```

### Development Workflow
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Standards
- Follow PEP 8 style guidelines
- Add type hints for all functions
- Include comprehensive docstrings
- Maintain >90% test coverage
- Update documentation for new features

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Apache Airflow** for workflow orchestration
- **MLflow** for experiment tracking
- **Apache Spark** for streaming analytics
- **Confluent Kafka** for real-time messaging
- **XGBoost/LightGBM/CatBoost** for machine learning
- **Docker** for containerization


⭐ **Star this repository** if you find it helpful!

🔥 **Built with passion for secure financial transactions** 🔥
