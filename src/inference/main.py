import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import yaml
import joblib
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType
from pyspark.sql.functions import from_json, col, hour, dayofweek, when, lit, pandas_udf, coalesce, PandasUDFType, year, month, current_date
import pandas as pd

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class FraudDetectionInference:
    bootstrap_servers = None
    topic = None
    security_protocol = None
    sasl_mechanism = None
    username = None
    password = None
    sasl_jaa_config = None

    def __init__(self, config_path='app/config.yaml'):
        load_dotenv(dotenv_path='/app/.env')
        self.config=self._load_config(config_path)
        self.spark=self._init_spark_session()
        self.model=self._load_model(self.config['model']['path'])
        self.broadcast_model = self.spark.sparkContext.broadcast(self.model)
        logger.debug(f'Environment variables loaded successfully {dict(os.environ)}')

    def _load_model(self, model_path):
        try:
            # Try loading with joblib first (recommended for sklearn models)
            try:
                model = joblib.load(model_path)
                logger.info(f"Model loaded successfully with joblib from {model_path}")
                return model
            except Exception as joblib_error:
                logger.warning(f"Failed to load with joblib: {joblib_error}")
                
                # Fallback to pickle with protocol handling
                import pickle
                with open(model_path, 'rb') as f:
                    model = pickle.load(f)
                    logger.info(f"Model loaded successfully with pickle from {model_path}")
                    return model
                    
        except Exception as e:
            logger.error(f"Error loading model from {model_path}: {e}")
            raise

    @staticmethod
    def _load_config(config_path):
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading config file {config_path}: {e}")
            raise

    def _init_spark_session(self):
        try:
            packages = self.config.get('spark', {}).get('packages', '')
            builder = SparkSession.builder.appName(self.config.get('spark', {}).get('app_name', 'FraudDetectionInference'))
            if packages:
                builder = builder.config('spark.jars.packages', packages)
            spark = builder.getOrCreate()
            logger.info("Spark session initialized successfully.")
            return spark
        except Exception as e:
            logger.error(f"Error initialising spark session: {str(e)}")
            raise
    
    def read_data_from_kafka(self):
        logger.info(f"Reading data from Kafka topic {self.config['kafka']['topic']}")
        kafka_bootstrap_servers = self.config['kafka']['bootstrap_servers']
        kafka_topic = self.config['kafka']['topic']
        kafka_security_protocol = self.config['kafka'].get('security_protocol', 'SASL_SSL')
        kafka_sasl_mechanism = self.config['kafka'].get('sasl_mechanism', 'PLAIN')
        kafka_username = self.config['kafka'].get('username')
        kafka_password = self.config['kafka'].get('password')
        kafka_sasl_jaas_config = (
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_username}" password="{kafka_password}";'
        )

        self.bootstrap_servers = kafka_bootstrap_servers
        self.topic = kafka_topic
        self.security_protocol = kafka_security_protocol
        self.sasl_mechanism = kafka_sasl_mechanism
        self.username = kafka_username
        self.password = kafka_password
        self.sasl_jaas_config = kafka_sasl_jaas_config

        df = (self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("subscribe", kafka_topic)
                .option('startingOffsets', 'latest')
                .option("kafka.security.protocol", kafka_security_protocol)
                .option("kafka.sasl.mechanism", kafka_sasl_mechanism)
                .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config)
                .load()
              )
        
        json_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("card_id", StringType(), True),
            StructField("amount", FloatType(), True),
            StructField("currency", StringType(), True),
            StructField("merchant", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("merchant_category_code", IntegerType(), True),
            StructField("terminal_id", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("entry_mode", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("lat", FloatType(), True),
            StructField("long", FloatType(), True),
            StructField("device_id", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("card_brand", StringType(), True),
            StructField("card_exp_month", IntegerType(), True),
            StructField("card_exp_year", IntegerType(), True),
            StructField("cvv_verified", BooleanType(), True),
            StructField("zip_verified", BooleanType(), True),
            StructField("address_verified", BooleanType(), True),
        ])

        parsed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", json_schema).alias("data")) \
            .select("data.*")
        return parsed_df
    

    def add_features(self, df):
        # temporal features
        df = df.withColumn('transaction_hour', hour(col('timestamp')))
        df = df.withColumn('is_weekend', when((dayofweek(col('timestamp')) == 1) | (dayofweek(col('timestamp')) == 7), 1).otherwise(0))
        df = df.withColumn('is_night', when((col('transaction_hour') < 5) | (col('transaction_hour') >= 22), 1).otherwise(0))
        df = df.withColumn('transaction_day', dayofweek(col('timestamp')))
        
        # time-based features 
        # Calculate time since last transaction using lag over user_id partition
        from pyspark.sql.window import Window
        from pyspark.sql.functions import lag, unix_timestamp
        
        user_window = Window.partitionBy("user_id").orderBy("timestamp")
        df = df.withColumn("prev_timestamp", lag("timestamp").over(user_window))
        df = df.withColumn("time_since_last_transaction", 
                          when(col("prev_timestamp").isNull(), 0.0)
                          .otherwise(unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")))
        df = df.drop("prev_timestamp")
        
        # behavioral features - REQUIRES STATEFUL STREAMING
        # user_activity_24h needs: sliding 24h window + state management across batches
        # Implementation would require: Spark state stores with TTL or external cache (Redis)
        df = df.withColumn("user_activity_24h", lit(1))  # simplified: assume minimal activity
        
        # monetary features - REQUIRES STATEFUL STREAMING  
        # rolling_avg_7d needs: 7-day transaction history + persistent state across micro-batches
        # Implementation would require: external state store (Cassandra/Redis) or Spark state management
        df = df.withColumn("rolling_avg_7d", lit(100.0))  # simplified: use global average
        df = df.withColumn("amount_to_avg_ratio", col("amount") / col("rolling_avg_7d"))
        df = df.withColumn("amount_to_avg_ratio", coalesce(col("amount_to_avg_ratio"), lit(1.0)))
        
        # merchant features
        high_risk_merchants = self.config.get('high_risk_merchants', ['QuickCash', 'GlobalDigital', 'FastMoneyX', 'CryptoATM', 'OnlineGaming'])
        df = df.withColumn("merchant_risk", col("merchant").isin(high_risk_merchants).cast("int"))
        
        # Credit card specific features
        # Card verification features
        df = df.withColumn("verification_score", 
                          (col("cvv_verified").cast("int") + 
                           col("zip_verified").cast("int") + 
                           col("address_verified").cast("int")))
        
        # High-risk merchant categories (ATM, Online, etc.)
        high_risk_categories = [6011, 5967, 7995]  # ATM, Online, Gambling
        df = df.withColumn("high_risk_category", col("merchant_category_code").isin(high_risk_categories).cast("int"))
        
        # Entry mode risk (manual/online higher risk)
        high_risk_entry_modes = ['MANUAL', 'ONLINE']
        df = df.withColumn("high_risk_entry_mode", col("entry_mode").isin(high_risk_entry_modes).cast("int"))
        
        # Card expiration check (expired cards are high risk)
        from pyspark.sql.functions import year, month, current_date
        df = df.withColumn("is_card_expired", 
                          when((col("card_exp_year") < year(current_date())) | 
                               ((col("card_exp_year") == year(current_date())) & 
                                (col("card_exp_month") < month(current_date()))), 1).otherwise(0))
        
        # High-risk countries
        high_risk_countries = ['RU', 'CN', 'NG', 'PK', 'IR']
        df = df.withColumn("high_risk_country", col("location").isin(high_risk_countries).cast("int"))
        
        # Transaction type risk
        high_risk_transaction_types = ['ATM_WITHDRAWAL', 'CASH_ADVANCE']
        df = df.withColumn("high_risk_transaction_type", col("transaction_type").isin(high_risk_transaction_types).cast("int"))
        
        # Amount category features
        df = df.withColumn("is_micro_transaction", when(col("amount") < 5.0, 1).otherwise(0))
        df = df.withColumn("is_large_transaction", when(col("amount") > 1000.0, 1).otherwise(0))
        
        # Weekend night transactions (higher risk)
        df = df.withColumn("weekend_night", (col("is_weekend") & col("is_night")).cast("int"))

        df.printSchema()
        return df


    def run_inference(self):
        import pandas as pd
        df=self.read_data_from_kafka()
        df=df.withWatermark("timestamp", "24 hours")
        df=self.add_features(df)
        
        broadcast_model = self.broadcast_model

        @pandas_udf('int')
        def predict_udf(
            amount: pd.Series,
            is_night: pd.Series,
            is_weekend: pd.Series,
            transaction_day: pd.Series,
            transaction_hour: pd.Series,
            time_since_last_transaction: pd.Series,
            user_activity_24h: pd.Series,
            rolling_avg_7d: pd.Series,
            amount_to_avg_ratio: pd.Series,
            merchant_risk: pd.Series,
            merchant: pd.Series,
            verification_score: pd.Series,
            high_risk_category: pd.Series,
            high_risk_entry_mode: pd.Series,
            is_card_expired: pd.Series,
            high_risk_country: pd.Series,
            high_risk_transaction_type: pd.Series,
            is_micro_transaction: pd.Series,
            is_large_transaction: pd.Series,
            weekend_night: pd.Series
        ) -> pd.Series:
            input_df = pd.DataFrame({
                'amount': amount,
                'is_night': is_night,
                'is_weekend': is_weekend,
                'transaction_day': transaction_day,
                'transaction_hour': transaction_hour,
                'time_since_last_transaction': time_since_last_transaction,
                'user_activity_24h': user_activity_24h,
                'rolling_avg_7d': rolling_avg_7d,
                'amount_to_avg_ratio': amount_to_avg_ratio,
                'merchant_risk': merchant_risk,
                'merchant': merchant,
                'verification_score': verification_score,
                'high_risk_category': high_risk_category,
                'high_risk_entry_mode': high_risk_entry_mode,
                'is_card_expired': is_card_expired,
                'high_risk_country': high_risk_country,
                'high_risk_transaction_type': high_risk_transaction_type,
                'is_micro_transaction': is_micro_transaction,
                'is_large_transaction': is_large_transaction,
                'weekend_night': weekend_night
            })

            # get probabilities of the fruad cases
            prob = broadcast_model.value.predict_proba(input_df)[:, 1]
            threshold = 0.70
            predictions = (prob >= threshold).astype(int)
            return pd.Series(predictions)

        prediction_df = df.withColumn('prediction', predict_udf(
            *[col(f) for f in [
                "amount", "is_night", "is_weekend", "transaction_day",
                "transaction_hour", "time_since_last_transaction", 
                "user_activity_24h", "rolling_avg_7d", "amount_to_avg_ratio",
                "merchant_risk", "merchant", "verification_score",
                "high_risk_category", "high_risk_entry_mode", "is_card_expired",
                "high_risk_country", "high_risk_transaction_type",
                "is_micro_transaction", "is_large_transaction", "weekend_night"
            ]]
        ))

        fraud_predictions = prediction_df.filter(col('prediction') == 1)

        (fraud_predictions.selectExpr("CAST(transaction_id AS STRING) AS key",
                                     "to_json(struct(*)) AS value"
                                    )
                                    .writeStream
                                    .format("kafka")
                                    .option("kafka.bootstrap.servers", self.bootstrap_servers)
                                    .option("topic",'fraud_predictions')
                                    .option("kafka.security.protocol", self.security_protocol)
                                    .option("kafka.sasl.mechanism", self.sasl_mechanism)
                                    .option("kafka.sasl.jaas.config", self.sasl_jaas_config)
                                    .option("checkpointLocation", "checkpoints/checkpoint")
                                    .outputMode("update")
                                    .start()
                                    .awaitTermination()
        )

if __name__ == "__main__":
    inference= FraudDetectionInference("/app/config.yaml")
    inference.run_inference()
            
