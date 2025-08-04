import logging
import os
import yaml
import mlflow
import boto3
import pandas as pd
import json
import numpy as np
from kafka import KafkaConsumer
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from xgboost import XGBClassifier
from catboost import CatBoostClassifier
from lightgbm import LGBMClassifier
from imblearn.pipeline import Pipeline as ImbPipeline
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold, train_test_split
from sklearn.metrics import make_scorer, fbeta_score, precision_recall_curve, confusion_matrix
from sklearn.metrics import average_precision_score, precision_score, recall_score
from mlflow.models.signature import infer_signature
import joblib


import matplotlib.pyplot as plt

from dotenv import load_dotenv

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('./fraud_detection_model_v2.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FraudDetectionTraining:

    def __init__(self, config_path='/app/config.yaml'):
        # set environmental variables for GitPython
        os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
        os.environ['GIT_PYTHON_GIT_EXECUTABLE'] = 'usr/bin/git'

        load_dotenv(dotenv_path='/app/.env')

        self.config = self._load_config(config_path)

        os.environ.update({
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'AWS_S3_ENDPOINT_URL': self.config['mlflow']['s3_endpoint_url'],
        })

        self._validate_environment()

        mlflow.set_tracking_uri(self.config['mlflow']['tracking_uri'])
        mlflow.set_experiment(self.config['mlflow']['experiment_name'])


    def _load_config(self, config_path: str) -> dict:
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded successfully")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_path}: {str(e)}")
            raise


    def _validate_environment(self):
        required_vars = ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        self._check_minio_connection()

    
    def _check_minio_connection(self):
        try:
            s3 = boto3.client('s3',
                endpoint_url=self.config['mlflow']['s3_endpoint_url'],
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )

            buckets = s3.list_buckets()
            bucket_names = [b['Name'] for b in buckets.get('Buckets',[])]
            logger.info(f"Connected to MinIO. Available buckets: {', '.join(bucket_names)}")

            mlflow_bucket = self.config['mlflow'].get('bucket','mlflow')
            
            if mlflow_bucket not in bucket_names:
                s3.create_bucket(Bucket=mlflow_bucket)
                logger.info(f"Created missing mlflow bucket: {str(mlflow_bucket)}")

        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {str(e)}")
            raise
    
    
    def _read_data_from_kafka(self) -> pd.DataFrame:
        try:
            topic = self.config['kafka']['topic']
            logger.info(f"Reading data from Kafka topic: {topic}")

            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                security_protocol='SASL_SSL',
                sasl_mechanism='PLAIN',
                sasl_plain_username=self.config['kafka']['username'],
                sasl_plain_password=self.config['kafka']['password'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                consumer_timeout_ms=self.config['kafka'].get('timeout', 10000)
            )

            messages = [msg.value for msg in consumer]
            consumer.close()

            df = pd.DataFrame(messages)
            if df.empty:
                raise ValueError('No message received from Kafka')
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

            if 'is_fraud' not in df.columns:
                raise ValueError('Fraud label (is_fraud) missing from Kafka data')
            
            fraud_rate = df['is_fraud'].mean() * 100
            logger.info(f"Received {len(df)} records from Kafka with fraud rate: {fraud_rate:.2f}%")
            return df

        except Exception as e:
            logger.error(f"Failed to read data from Kafka: {str(e)}", exc_info=True)
            raise
    

    def _create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(['user_id', 'timestamp']).copy()
        
        # ------------------extract temporal features-----------------
        # hour of the day
        df['transaction_hour'] = df['timestamp'].dt.hour
        # flag txns happening at night (between 10pm and 5am)
        df['is_night'] = ((df['transaction_hour'] >= 22) | (df['transaction_hour'] < 5)).astype(int)
        # flag txns happening on weekends
        df['is_weekend'] = (df['timestamp'].dt.dayofweek >= 5).astype(int)
        # txn day
        df['transaction_day'] = df['timestamp'].dt.day

        # ------------------time-based features-----------------
        # time since last transaction for each user (in seconds)
        df['time_since_last_transaction'] = df.groupby('user_id')['timestamp'].diff().dt.total_seconds().fillna(0)

        # ------------------behavioural features-----------------
        # user activity in last 24 hours (rolling count)
        # Create proper rolling calculations without index conflicts
        def calculate_user_activity_24h(group):
            if len(group) == 0:
                return pd.Series([], dtype=int)
            group_sorted = group.sort_values('timestamp')
            group_indexed = group_sorted.set_index('timestamp')
            rolling_count = group_indexed['amount'].rolling('24H', closed='left').count().fillna(0)
            # Reindex to match original group order
            result = rolling_count.reindex(group['timestamp']).fillna(0)
            return result
        
        # Apply the function and flatten the result
        activity_result = df.groupby('user_id', group_keys=False).apply(calculate_user_activity_24h)
        df['user_activity_24h'] = activity_result.values.astype(int)

        # ------------------monetary features--------------------
        # rolling 7-day average amount per user
        def calculate_rolling_avg_7d(group):
            if len(group) == 0:
                return pd.Series([], dtype=float)
            group_sorted = group.sort_values('timestamp')
            group_indexed = group_sorted.set_index('timestamp')
            rolling_avg = group_indexed['amount'].rolling('7D', min_periods=1, closed='left').mean().fillna(100.0)
            # Reindex to match original group order
            result = rolling_avg.reindex(group['timestamp']).fillna(100.0)
            return result
            
        # Apply the function and flatten the result
        avg_result = df.groupby('user_id', group_keys=False).apply(calculate_rolling_avg_7d)
        df['rolling_avg_7d'] = avg_result.values
        
        # ratio of current amount to rolling 7-day average
        df['amount_to_avg_ratio'] = df['amount'] / df['rolling_avg_7d']
        df['amount_to_avg_ratio'] = df['amount_to_avg_ratio'].fillna(1.0)

        # ------------------merchant features--------------------
        high_risk_merchants = self.config.get('high_risk_merchants', ['QuickCash', 'GlobalDigital', 'FastMoneyX', 'CryptoATM', 'OnlineGaming'])
        df['merchant_risk'] = df['merchant'].isin(high_risk_merchants).astype(int)

        # ------------------credit card specific features--------------------
        # Card verification score (0-3 based on cvv, zip, address verification)
        df['verification_score'] = (
            df.get('cvv_verified', False).astype(int) + 
            df.get('zip_verified', False).astype(int) + 
            df.get('address_verified', False).astype(int)
        )
        
        # High-risk merchant categories
        high_risk_categories = [6011, 5967, 7995]  # ATM, Online, Gambling
        df['high_risk_category'] = df.get('merchant_category_code', 0).isin(high_risk_categories).astype(int)
        
        # Entry mode risk
        high_risk_entry_modes = ['MANUAL', 'ONLINE']
        df['high_risk_entry_mode'] = df.get('entry_mode', '').isin(high_risk_entry_modes).astype(int)
        
        # Card expiration check
        current_year = pd.Timestamp.now().year
        current_month = pd.Timestamp.now().month
        df['is_card_expired'] = (
            (df.get('card_exp_year', current_year + 1) < current_year) | 
            ((df.get('card_exp_year', current_year + 1) == current_year) & 
             (df.get('card_exp_month', current_month + 1) < current_month))
        ).astype(int)
        
        # High-risk countries
        high_risk_countries = ['RU', 'CN', 'NG', 'PK', 'IR']
        df['high_risk_country'] = df.get('location', '').isin(high_risk_countries).astype(int)
        
        # Transaction type risk
        high_risk_transaction_types = ['ATM_WITHDRAWAL', 'CASH_ADVANCE']
        df['high_risk_transaction_type'] = df.get('transaction_type', '').isin(high_risk_transaction_types).astype(int)
        
        # Amount category features
        df['is_micro_transaction'] = (df['amount'] < 5.0).astype(int)
        df['is_large_transaction'] = (df['amount'] > 1000.0).astype(int)
        
        # Weekend night transactions
        df['weekend_night'] = (df['is_weekend'] & df['is_night']).astype(int)

        # ------------------TARGETED FRAUD DETECTION FEATURES--------------------
        
        # 1. CARD TESTING DETECTION (Pattern 2: amounts 0.01-4.99, online, verification fails)
        df['is_very_small'] = (df['amount'] < 1.0).astype(int)
        df['card_testing_pattern'] = (
            (df['amount'] < 5.0) &
            (df.get('entry_mode', '') == 'ONLINE') &
            ((df.get('cvv_verified', True) == False) | (df.get('zip_verified', True) == False))
        ).astype(int)
        
        # 2. ACCOUNT TAKEOVER DETECTION (Pattern 1: high amounts, high-risk merchants, online, high-risk countries)
        df['takeover_pattern'] = (
            (df['amount'] >= 500) &
            (df['merchant_risk'] == 1) &
            (df.get('entry_mode', '') == 'ONLINE') &
            (df['high_risk_country'] == 1) &
            (df.get('cvv_verified', True) == False)
        ).astype(int)
        
        # 3. MERCHANT COLLUSION DETECTION (Pattern 3: high amounts, high-risk merchants, physical entry)
        df['collusion_pattern'] = (
            (df['amount'] >= 1000) &
            (df['merchant_risk'] == 1) &
            (df.get('entry_mode', '').isin(['CHIP', 'SWIPE'])) &
            (df.get('cvv_verified', True))  # Appears legitimate
        ).astype(int)
        
        # 4. GEOGRAPHIC ANOMALY DETECTION (Pattern 4: high-risk countries, online, address fails)
        df['geo_anomaly_pattern'] = (
            (df['high_risk_country'] == 1) &
            (df.get('entry_mode', '') == 'ONLINE') &
            (df.get('address_verified', True) == False)
        ).astype(int)
        
        # 5. EXPIRED CARD DETECTION (Pattern 5: expired cards, manual entry, CVV fails)
        df['expired_card_pattern'] = (
            (df['is_card_expired'] == 1) &
            (df.get('entry_mode', '') == 'MANUAL') &
            (df.get('cvv_verified', True) == False)
        ).astype(int)
        
        # 6. VERIFICATION FAILURE DETECTION (Pattern 6: all verifications fail, online)
        df['verification_fail_pattern'] = (
            (df.get('cvv_verified', True) == False) &
            (df.get('zip_verified', True) == False) &
            (df.get('address_verified', True) == False) &
            (df.get('entry_mode', '') == 'ONLINE')
        ).astype(int)
        
        # 7. VELOCITY FEATURES (useful for detecting rapid fraud attempts)
        # Transaction count in last 1 hour using proper rolling window
        def calculate_txn_count_1h(group):
            if len(group) == 0:
                return pd.Series([], dtype=int)
            group_sorted = group.sort_values('timestamp')
            group_indexed = group_sorted.set_index('timestamp')
            rolling_count = group_indexed['amount'].rolling('1H', closed='left').count().fillna(0)
            # Reindex to match original group order
            result = rolling_count.reindex(group['timestamp']).fillna(0)
            return result
            
        # Apply the function and flatten the result
        velocity_result = df.groupby('user_id', group_keys=False).apply(calculate_txn_count_1h)
        df['txn_count_1h'] = velocity_result.values.astype(int)
        df['high_velocity'] = (df['txn_count_1h'] > 3).astype(int)  # >3 txns in 1 hour
        
        # 8. AMOUNT ANOMALY FEATURES
        df['user_amount_std'] = df.groupby('user_id')['amount'].transform('std').fillna(0)
        df['amount_z_score'] = abs((df['amount'] - df['rolling_avg_7d']) / (df['user_amount_std'] + 0.01))
        df['amount_outlier'] = (df['amount_z_score'] > 2.5).astype(int)  # 2.5 standard deviations
        
        # 9. NON-USD CURRENCY (higher fraud risk based on producer currencies)
        df['non_usd_currency'] = (df.get('currency', 'USD') != 'USD').astype(int)

        # feature selection - streamlined to most relevant features
        features_col = [
            # Basic temporal and monetary features
            'amount', 'is_night', 'is_weekend', 'transaction_day', 'transaction_hour', 
            'time_since_last_transaction', 'user_activity_24h', 'rolling_avg_7d', 
            'amount_to_avg_ratio', 'merchant_risk', 'merchant',
            
            # Credit card verification and risk features
            'verification_score', 'high_risk_category', 'high_risk_entry_mode', 
            'is_card_expired', 'high_risk_country', 'high_risk_transaction_type',
            'is_micro_transaction', 'is_large_transaction', 'weekend_night',
            
            # Targeted fraud pattern detection features
            'is_very_small', 'card_testing_pattern', 'takeover_pattern', 
            'collusion_pattern', 'geo_anomaly_pattern', 'expired_card_pattern',
            'verification_fail_pattern', 'high_velocity', 'amount_outlier', 'non_usd_currency'
        ]
        
        if 'is_fraud' not in df.columns:
            raise ValueError('is_fraud label is missing from the data')
        
        return df[features_col + ['is_fraud']]
        

    def _create_model(self):
        """Create model based on framework specified in config"""
        framework = self.config['model']['framework'].lower()
        seed = self.config['model'].get('seed', 42)

        print("The model chosen for training is: ", framework)
        
        if framework == 'xgboost':
            params = self.config['model']['xgboost_params'].copy()
            params['eval_metric'] = 'aucpr'
            params['random_state'] = seed
            params['reg_lambda'] = 1.0
            return XGBClassifier(**params)
            
        elif framework == 'catboost':
            params = self.config['model']['catboost_params'].copy()
            params['random_state'] = seed
            params['verbose'] = False
            params['eval_metric'] = 'AUC'
            return CatBoostClassifier(**params)
            
        elif framework == 'lightgbm':
            params = self.config['model']['lightgbm_params'].copy()
            params['random_state'] = seed
            params['objective'] = 'binary'
            params['metric'] = 'auc'
            params['verbose'] = -1
            return LGBMClassifier(**params)
            
        else:
            raise ValueError(f"Unsupported framework: {framework}. Choose from: xgboost, catboost, lightgbm")


    def _get_param_grid(self):
        """Get hyperparameter grid based on framework"""
        framework = self.config['model']['framework'].lower()
        
        if framework == 'xgboost':
            return {
                'classifier__max_depth': [3, 5, 7],
                'classifier__learning_rate': [0.01, 0.05, 0.1],
                'classifier__subsample': [0.6, 0.8, 1.0],
                'classifier__colsample_bytree': [0.6, 0.8, 1.0],
                'classifier__gamma': [0, 0.1, 0.3],
                'classifier__reg_alpha': [0, 0.1, 0.5]
            }
        elif framework == 'catboost':
            return {
                'classifier__depth': [4, 6, 8],
                'classifier__learning_rate': [0.01, 0.05, 0.1],
                'classifier__l2_leaf_reg': [1, 3, 5],
                'classifier__border_count': [32, 64, 128]
            }
        elif framework == 'lightgbm':
            return {
                'classifier__max_depth': [3, 5, 7],
                'classifier__learning_rate': [0.01, 0.05, 0.1],
                'classifier__num_leaves': [31, 50, 100],
                'classifier__subsample': [0.6, 0.8, 1.0],
                'classifier__colsample_bytree': [0.6, 0.8, 1.0]
            }
        

    def train_model(self):
        # extract data from Kafka,
        # feature engineering
        # split into train/test datasets
        # log model and artifacts in MLFlow
        # Preprocessing pipeline
        try:
            logger.info("Starting model training process...")
            
            # read raw data from Kafka
            df = self._read_data_from_kafka()

            # start feature engineering from the raw data
            data = self._create_features(df)

            # split data into features (X) and target (y)
            X = data.drop(columns=['is_fraud'])
            y = data['is_fraud']

            if(y.sum() == 0):
                raise ValueError("No positive sample found in the data, cannot train model")
            if(y.sum() < 10):
                raise ValueError("Too few positive samples found in the data, cannot train model")
            
            # split into train and test datasets
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=self.config['model'].get('test_size', 0.2), 
                random_state=self.config['model'].get('random_state', 42),
                stratify=y
            )

            with mlflow.start_run():
                mlflow.log_metrics({
                    'train_sample': X_train.shape[0],
                    'positive_samples': int(y_train.sum()),
                    'class_ratio': float(y_train.mean()),
                    'test_sample': X_test.shape[0]
                })

                # column transformer for preprocessing
                categorical_features = ['merchant']
                preprocessor = ColumnTransformer(
                    [
                        ('merchant_encoder', OrdinalEncoder(
                            handle_unknown='use_encoded_value', unknown_value=-1, dtype=np.float32
                        ), categorical_features)
                    ], 
                    remainder='passthrough'
                )

                # Create model based on framework
                model = self._create_model()
                framework = self.config['model']['framework'].lower()
                
                logger.info(f"Training with {framework} framework")

                # ----- pipeline construction -----
                # # preprocessing
                pipeline = ImbPipeline([
                    ('preprocessor', preprocessor),
                    ('smote', SMOTE(random_state=self.config['model'].get('seed', 42))),
                    ('classifier', model)
                ], memory='./cache/')

                # Get framework-specific parameter grid
                param_dist = self._get_param_grid()

                searcher = RandomizedSearchCV(
                    pipeline,
                    param_dist,
                    n_iter=10,
                    scoring=make_scorer(fbeta_score, beta=2, zero_division=0),
                    cv=StratifiedKFold(n_splits=3, shuffle=True),
                    n_jobs=1,
                    refit=True,
                    error_score='raise',
                    random_state=self.config['model'].get('seed', 42)
                )

                logger.info("Starting hyperparameter tuning...")
                searcher.fit(X_train, y_train)
                best_model=searcher.best_estimator_
                best_params=searcher.best_params_
                logger.info(f"Best hyperparameters found: {best_params}")

                train_proba = best_model.predict_proba(X_train)[:, 1]
                precision_arr, recall_arr, thresholds_arr= precision_recall_curve(y_train, train_proba)
                f1_scores = [2* (p * r) / (p + r) if (p + r) > 0 else 0 for p, r in zip(precision_arr, recall_arr)]
                best_threshold = thresholds_arr[np.argmax(f1_scores)]
                logger.info(f"Best threshold for F1 score: {best_threshold}")

                X_test_processed = best_model.named_steps['preprocessor'].transform(X_test)

                test_proba = best_model.named_steps['classifier'].predict_proba(X_test_processed)[:, 1]

                Y_pred = (test_proba >= best_threshold).astype(int)

                metrics = {
                    'auc_pr': float(average_precision_score(y_test, test_proba)),
                    'precision': float(precision_score(y_test, Y_pred, zero_division=0)),
                    'recall': float(recall_score(y_test, Y_pred, zero_division=0)),
                    'f1': float(fbeta_score(y_test, Y_pred, beta=2, zero_division=0)),
                    'threshold': float(best_threshold)
                }

                mlflow.log_metrics(metrics)
                mlflow.log_params(best_params)

                # confusion matrix
                cm = confusion_matrix(y_test, Y_pred)
                plt.figure(figsize=(6,4))
                plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
                plt.title('Confusion Matrix')
                plt.colorbar()
                tick_marks = np.arange(2)
                plt.xticks(tick_marks, ['Not Fraud', 'Fraud'])
                plt.yticks(tick_marks, ['Not Fraud', 'Fraud'])

                for i in range(2):
                    for j in range(2):
                        plt.text(j,i,format(cm[i,j], 'd'), ha='center', va='center', color='red')
                
                plt.tight_layout()
                cm_filename = 'confusion_matrix.png'
                plt.savefig(cm_filename)
                mlflow.log_artifact(cm_filename)
                plt.close()

                # precision and recall curve
                plt.figure(figsize=(10,6))
                plt.plot(recall_arr, precision_arr, marker='.', label='Precision-Recall Curve')
                plt.xlabel('Recall')
                plt.ylabel('Precision')
                plt.title('Precision-Recall Curve')
                plt.legend()
                pr_filename = 'precision_recall_curve.png'
                plt.savefig(pr_filename)
                mlflow.log_artifact(pr_filename)
                plt.close()

                signature = infer_signature(X_train, Y_pred)
                
                # Include framework in model name
                model_name = f"fraud_detection_{framework}_model_v2"
                
                mlflow.sklearn.log_model(
                    sk_model=best_model,
                    artifact_path='model',
                    signature=signature,
                    registered_model_name=model_name
                )

                # Log framework info
                mlflow.log_param('framework', framework)

                os.makedirs('model', exist_ok=True)
                joblib.dump(best_model, '/app/models/fraud_detection_model_v2.pkl')

                logger.info(f'Training successfully completed with metrics {metrics}')

                return best_model, metrics

        except Exception as e:
            logger.error(f"Training failed: {str(e)}", exc_info=True)
            raise


