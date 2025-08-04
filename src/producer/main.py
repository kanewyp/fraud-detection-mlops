import os
from confluent_kafka import Producer
import logging
from dotenv import load_dotenv
from faker import Faker
import random
import signal
import time
import json
from typing import Dict, Any, Optional
from jsonschema import validate, ValidationError, FormatChecker
from datetime import datetime, timedelta, timezone


logging.basicConfig(format="%(asctime)s - %(levelname)s - %(module)s - %(message)s", 
                    level=logging.INFO
                    )

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path="/app/.env")

fake = Faker()

TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "integer", "minimum": 1000, "maximum": 9999},
        "amount": {"type": "number", "minimum": 0.01, "maximum": 100000.0},
        "currency": {"type": "string", "pattern": r"^[A-Z]{3}$"},
        "merchant": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "location": {"type": "string", "pattern": r"^[A-Z]{2}$"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1}
    },
    "required": ["transaction_id", "user_id", "amount", "currency", "timestamp", "is_fraud"]
}


class TransactionProducer():
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.kafka_username= os.getenv("KAFKA_USERNAME")
        self.kafka_password = os.getenv("KAFKA_PASSWORD")
        self.topic = os.getenv("KAFKA_TOPIC", "transactions")
        self.running = False # Flag to control the production loop
        
        # Confluent Kafka configuration
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'transaction_producer',
            'compression.type': 'gzip',
            'linger.ms': '5',
            'batch.size': 16384,
        }

        if self.kafka_username and self.kafka_password:
            self.producer_config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': self.kafka_username,
                'sasl.password': self.kafka_password
            })
        else:
            self.producer_config['security.protocol'] = 'PLAINTEXT'

        try:
            self.producer = Producer(self.producer_config)
            logger.info('Confluent Kafka Producer initialized successfully.')
        except Exception as e:
            logger.error(f"Failed to initialize Confluent Kafka Producer: {str(e)}")
            raise e
        
        # risky users
        self.compromised_users = set(random.sample(range(1000, 9999), 50)) #0.5% of users
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX']
        self.fraud_pattern_weights = {
            'account_takeover': 0.4,  # 40% of fraudulent transactions
            'card_testing': 0.3,  # 30% of fraudulent transactions
            'merchant_collusion': 0.2,  # 20%
            'geo_anomaly': 0.1  # 10%
        }

        # Configure graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    
    def delivery_report(self, err, msg):
        """Callback to handle delivery reports from Kafka"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


    def validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        try:
            validate(
                instance=transaction,
                schema=TRANSACTION_SCHEMA,
                format_checker=FormatChecker()
            )
            return True

        except ValidationError as e:
            logger.error(f"Invalid Transcation: {str(e)}")
            return False
        

    def generate_transaction(self) -> Optional[Dict[str, Any]]:
        """Generate a random transaction with potential fraud patterns"""
        transaction = {
            'transaction_id': fake.uuid4(),
            'user_id': random.randint(1000, 9999),
            'amount': round(fake.pyfloat(min_value=0.01, max_value=100000.0), 2),
            'currency': 'USD',
            'merchant': fake.company(),
            'timestamp': (datetime.now(timezone.utc) + timedelta(seconds=random.randint(-300, 3000))).isoformat(),
            'location': fake.country_code(),
            'is_fraud': 0 # non-fraudulent by default
        }

        is_fraud = 0
        amount = transaction['amount']
        user_id = transaction['user_id']
        merchant = transaction['merchant']

        # account takeover
        if user_id in self.compromised_users and amount > 500:
            if random.random() < self.fraud_pattern_weights['account_takeover']:
                is_fraud = 1
                transaction['amount'] = random.uniform(500, 5000)  # High value transaction
                transaction['merchant'] = random.choice(self.high_risk_merchants)

        # card testing
        if not is_fraud and amount < 2.0:
            # simulate rapid small txns
            if user_id % 1000 == 0 and random.random() < self.fraud_pattern_weights['card_testing']:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(0.01, 2.0), 2)
                transaction['location'] = 'US'  # Card testing often happens in the US

        # merchant collusion
        if not is_fraud and merchant in self.high_risk_merchants:
            if amount > 3000 and random.random() < self.fraud_pattern_weights['merchant_collusion']:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(300.0, 1500.0), 2)

        # geo anomaly
        if not is_fraud:
            if user_id % 500 == 0 and random.random() < self.fraud_pattern_weights['geo_anomaly']:
                is_fraud = 1
                transaction['location'] = random.choice(['RU', 'CN', 'GB'])  # High-risk countries
        
        # baseline random fraud (0.5% of all transactions)
        if not is_fraud and random.random() < 0.005:
            is_fraud = 1
            transaction['amount'] = random.uniform(100, 2000)

        # ensure that final fraud rate is between 1-2%
        transaction['is_fraud'] = is_fraud if random.random() < 0.985 else 0

        # validate modified transaction
        if self.validate_transaction(transaction):
            return transaction


    def send_transaction(self) -> bool:
        try:
            transaction = self.generate_transaction()
            if not transaction:
                return False
            
            self.producer.produce( # Produce the transaction into Kafka
                self.topic,
                key=transaction['transaction_id'],
                value=json.dumps(transaction),
                callback=self.delivery_report
            )

            self.producer.poll(0)  # trigger callbacks
            return True
        except Exception as e:
            logger.error(f"Failed to send transaction: {str(e)}")
            return False



    def run_continuous_production(self, interval: float=0.0):
        """Run continuous message production with graceful shutdown """
        self.running = True
        logger.info("Starting producer for topic %s...", self.topic)

        try:
            while self.running:
                if self.send_transaction():
                    time.sleep(interval)
        finally:
            self.shutdown()


    def shutdown(self, signum=None, frame=None):
        if self.running:
            logger.info("Initiating shutdown...")
            self.running = False
            if self.producer:
                self.producer.flush(timeout=30)  # Wait for up to 30 seconds for messages to be sent
                self.producer.close()
            logger.info("Producer shutdown complete.")


if __name__=="__main__":
    producer = TransactionProducer() # Initialize the producer
    # producer.run_single_production() # produce a single transaction
    producer.run_continuous_production() # produce transactions continuously


    
