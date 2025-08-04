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
        "card_id": {"type": "string"},
        "amount": {"type": "number", "minimum": 0.01, "maximum": 100000.0},
        "currency": {"type": "string", "pattern": r"^[A-Z]{3}$"},
        "merchant": {"type": "string"},
        "merchant_category": {"type": "string"},
        "merchant_category_code": {"type": "integer", "minimum": 1000, "maximum": 9999},
        "terminal_id": {"type": "string"},
        "transaction_type": {"type": "string"},
        "entry_mode": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "location": {"type": "string", "pattern": r"^[A-Z]{2}$"},
        "lat": {"type": "number", "minimum": -90, "maximum": 90},
        "long": {"type": "number", "minimum": -180, "maximum": 180},
        "device_id": {"type": "string"},
        "ip_address": {"type": "string"},
        "user_agent": {"type": "string"},
        "card_type": {"type": "string"},
        "card_brand": {"type": "string"},
        "card_exp_month": {"type": "integer", "minimum": 1, "maximum": 12},
        "card_exp_year": {"type": "integer", "minimum": 2024, "maximum": 2035},
        "cvv_verified": {"type": "boolean"},
        "zip_verified": {"type": "boolean"},
        "address_verified": {"type": "boolean"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1}
    },
    "required": ["transaction_id", "user_id", "card_id", "amount", "currency", "timestamp", "is_fraud"]
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
        
        # risky users and card data
        self.compromised_users = set(random.sample(range(1000, 9999), 50)) #0.5% of users
        self.compromised_cards = set([fake.credit_card_number() for _ in range(100)])
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX', 'CryptoATM', 'OnlineGaming']
        self.merchant_categories = {
            'gas_station': 5541,
            'grocery': 5411,
            'restaurant': 5812,
            'retail': 5311,
            'online': 5967,
            'atm': 6011,
            'hotel': 7011,
            'airline': 4511,
            'pharmacy': 5912,
            'electronics': 5732
        }
        self.card_brands = ['VISA', 'MASTERCARD', 'AMEX', 'DISCOVER']
        self.card_types = ['CREDIT', 'DEBIT', 'PREPAID']
        self.transaction_types = ['PURCHASE', 'ATM_WITHDRAWAL', 'CASH_ADVANCE', 'REFUND', 'TRANSFER']
        self.entry_modes = ['CHIP', 'SWIPE', 'CONTACTLESS', 'ONLINE', 'MANUAL']
        
        # user-card mappings (users can have multiple cards)
        self.user_cards = {}
        for user_id in range(1000, 9999):
            num_cards = random.choices([1, 2, 3, 4], weights=[0.6, 0.25, 0.1, 0.05])[0]
            self.user_cards[user_id] = [fake.credit_card_number() for _ in range(num_cards)]
        self.fraud_pattern_weights = {
            'account_takeover': 0.25,  # 25% of fraudulent transactions
            'card_testing': 0.20,  # 20% of fraudulent transactions
            'merchant_collusion': 0.15,  # 15%
            'geo_anomaly': 0.15,  # 15%
            'expired_card': 0.10,  # 10%
            'verification_failure': 0.15  # 15%
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
        """Generate a random transaction with credit card fraud patterns"""
        user_id = random.randint(1000, 9999)
        card_id = random.choice(self.user_cards.get(user_id, [fake.credit_card_number()]))
        merchant_cat = random.choice(list(self.merchant_categories.keys()))
        
        # Base transaction with credit card features
        transaction = {
            'transaction_id': fake.uuid4(),
            'user_id': user_id,
            'card_id': card_id,
            'amount': round(fake.pyfloat(min_value=0.01, max_value=5000.0), 2),
            'currency': random.choice(['USD', 'EUR', 'GBP', 'CAD']),
            'merchant': fake.company(),
            'merchant_category': merchant_cat,
            'merchant_category_code': self.merchant_categories[merchant_cat],
            'terminal_id': fake.uuid4()[:8],
            'transaction_type': random.choice(self.transaction_types),
            'entry_mode': random.choice(self.entry_modes),
            'timestamp': (datetime.now(timezone.utc) + timedelta(seconds=random.randint(-300, 300))).isoformat(),
            'location': fake.country_code(),
            'lat': round(fake.latitude(), 6),
            'long': round(fake.longitude(), 6),
            'device_id': fake.uuid4(),
            'ip_address': fake.ipv4(),
            'user_agent': fake.user_agent(),
            'card_type': random.choice(self.card_types),
            'card_brand': random.choice(self.card_brands),
            'card_exp_month': random.randint(1, 12),
            'card_exp_year': random.randint(2024, 2030),
            'cvv_verified': random.choice([True, False]),
            'zip_verified': random.choice([True, False]), 
            'address_verified': random.choice([True, False]),
            'is_fraud': 0  # non-fraudulent by default
        }

        is_fraud = 0
        amount = transaction['amount']

        # FRAUD PATTERN 1: Account/Card takeover
        if (user_id in self.compromised_users or card_id in self.compromised_cards) and amount > 300:
            if random.random() < self.fraud_pattern_weights['account_takeover']:
                is_fraud = 1
                transaction['amount'] = random.uniform(500, 3000)  # High value transaction
                transaction['merchant'] = random.choice(self.high_risk_merchants)
                transaction['entry_mode'] = 'ONLINE'  # Likely online fraud
                transaction['cvv_verified'] = False  # CVV not verified
                transaction['location'] = random.choice(['RU', 'CN', 'NG'])  # High-risk countries

        # FRAUD PATTERN 2: Card testing (small amounts)
        if not is_fraud and amount < 5.0:
            if user_id % 1000 == 0 and random.random() < self.fraud_pattern_weights['card_testing']:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(0.01, 4.99), 2)
                transaction['merchant_category'] = 'online'
                transaction['merchant_category_code'] = 5967
                transaction['entry_mode'] = 'ONLINE'
                transaction['cvv_verified'] = False
                transaction['zip_verified'] = False

        # FRAUD PATTERN 3: Merchant collusion
        if not is_fraud and transaction['merchant'] in self.high_risk_merchants:
            if amount > 1000 and random.random() < self.fraud_pattern_weights['merchant_collusion']:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(1000, 2500), 2)
                transaction['entry_mode'] = random.choice(['CHIP', 'SWIPE'])
                transaction['cvv_verified'] = True  # Appears legitimate

        # FRAUD PATTERN 4: Geographic anomaly 
        if not is_fraud and user_id % 500 == 0:
            if random.random() < self.fraud_pattern_weights['geo_anomaly']:
                is_fraud = 1
                transaction['location'] = random.choice(['RU', 'CN', 'NG', 'PK'])
                transaction['entry_mode'] = 'ONLINE'
                transaction['address_verified'] = False
                transaction['amount'] = random.uniform(200, 1500)

        # FRAUD PATTERN 5: Expired card usage
        if not is_fraud and transaction['card_exp_year'] <= 2024:
            if random.random() < 0.8:  # 80% chance expired cards are fraud
                is_fraud = 1
                transaction['cvv_verified'] = False
                transaction['entry_mode'] = 'MANUAL'

        # FRAUD PATTERN 6: Multiple verification failures
        if not is_fraud and not any([transaction['cvv_verified'], transaction['zip_verified'], transaction['address_verified']]):
            if random.random() < 0.3:  # 30% chance when all verifications fail
                is_fraud = 1
                transaction['entry_mode'] = 'ONLINE'
                transaction['amount'] = random.uniform(100, 800)

        # Baseline random fraud (0.3% of all transactions)
        if not is_fraud and random.random() < 0.003:
            is_fraud = 1
            transaction['amount'] = random.uniform(50, 1000)

        # Final fraud rate control (target 1.5-2.5%)
        transaction['is_fraud'] = is_fraud if random.random() < 0.975 else 0

        # Validate enhanced transaction
        if self.validate_transaction(transaction):
            return transaction
        else:
            logger.warning("Generated transaction failed validation, skipping...")
            return None


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


    
