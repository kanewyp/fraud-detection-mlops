from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import logging

#
logger = logging.getLogger(__name__)

default_args = {
    'owner':'kanewyp',
    'depends_on_past': False,
    'start_date': datetime(2025,7,25),
    'max_active_runs': 1,
}

def _train_model(**context):
    """Airflow wrapper for training task"""
    from fraud_detection_training import FraudDetectionTraining
    try:
        logger.info("Initializing Fraud Detection Training")
        trainer = FraudDetectionTraining()
        model, precision = trainer.train_model()

        return {'status': 'success', 'precision': precision}
    except Exception as e:
        logger.error(f"Training failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Model Training failed: {str(e)}")

with DAG(
    'fraud_detection_training_v2',
    default_args=default_args,
    description='Fraud Detection Model Training Pipeline',
    schedule_interval='18 0 * * *',  # Daily at 18:00 UTC
    catchup=False,
    tags=['fraud', 'ML'],
) as dag:
    
    validate_envronment = BashOperator(
        task_id='validate_environment',
        bash_command='''
        echo "Validating environment..."
        test -f /app/config.yaml &&
        test -f /app/.env &&
        echo "Environment validation successful."
        '''
    )

    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=_train_model,
        provide_context=True
    )

    cleanup_task = BashOperator(
        task_id='cleanup_resources',
        bash_command='rm -f /app/tmp/*.pkl',
        trigger_rule='all_done'
    )

    validate_envronment >> training_task >> cleanup_task

    # Documentation
    dag.doc_md = """
    # Fraud Detection Model Training Pipeline
    Daily Training of fraud detection models using:
    - Transaction data from Kafka
    - XGBoost classifier with precision optimisation
    - MLFlow for experiment tracking
    """