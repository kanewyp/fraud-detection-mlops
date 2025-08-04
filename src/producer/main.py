import os
from confluent_kafka import Producer
import logging
from dotenv import load_dotenv
from faker import Faker
import random
import signal
import time
import json
import decimal
from typing import Dict, Any, Optional
from jsonschema import validate, ValidationError, FormatChecker
from datetime import datetime, timedelta, timezone


logging.basicConfig(format="%(asctime)s - %(levelname)s - %(module)s - %(message)s", 
                    level=logging.INFO
                    )

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path="/app/.env")

fake = Faker()

def convert_decimals_to_float(obj):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, dict):
        return {k: convert_decimals_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals_to_float(i) for i in obj]
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    else:
        return obj

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
        "card_exp_year": {"type": "integer", "minimum": 2022, "maximum": 2035},
        "cvv_verified": {"type": "boolean"},
        "zip_verified": {"type": "boolean"},
        "address_verified": {"type": "boolean"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1}
    },
    "required": ["transaction_id", "user_id", "card_id", "amount", "currency", "timestamp", "is_fraud"]
}


class TransactionProducer():
    """
    Enhanced Credit Card Transaction Producer with Realistic Fraud Patterns
    
    Implements 6 distinct fraud patterns based on real-world credit card fraud scenarios:
    
    1. ACCOUNT/CARD TAKEOVER (25% of fraud):
       - Uses compromised user accounts or stolen card numbers
       - High-value transactions ($500-$3000)
       - Often from high-risk merchants
       - Online entry mode, CVV verification fails
       - Originates from high-risk countries (RU, CN, NG)
    
    2. CARD TESTING (20% of fraud):
       - Small-value transactions to test stolen card validity
       - Amounts under $5 (typically $0.01-$4.99)
       - Online merchants, verification failures
       - Precursor to larger fraudulent transactions
    
    3. MERCHANT COLLUSION (15% of fraud):
       - Fraudulent merchants processing fake transactions
       - High-value amounts ($1000-$2500)
       - Uses legitimate-appearing verification (CVV passes)
       - Physical entry modes (CHIP/SWIPE) to appear normal
    
    4. GEOGRAPHIC ANOMALY (15% of fraud):
       - Transactions from unusual/high-risk locations
       - Countries: RU, CN, NG, PK (high fraud rates)
       - Online transactions, address verification fails
       - Medium amounts ($200-$1500)
    
    5. EXPIRED CARD USAGE (10% of fraud):
       - Using cards past expiration date
       - Manual entry mode (bypassing chip validation)
       - CVV verification typically fails
       - Various transaction amounts
    
    6. VERIFICATION FAILURES (15% of fraud):
       - All verification checks fail (CVV, ZIP, Address)
       - Online transactions (easier to bypass checks)
       - Medium amounts ($100-$800)
       - Often automated fraud attempts
    
    Target fraud rate: 0.5% ± 0.05% with dynamic adjustment
    """
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.kafka_username= os.getenv("KAFKA_USERNAME")
        self.kafka_password = os.getenv("KAFKA_PASSWORD")
        self.topic = os.getenv("KAFKA_TOPIC", "transactions_v2")
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
        
        # Fraud rate monitoring
        self.total_transactions = 0
        self.fraud_transactions = 0
        self.target_fraud_rate = 0.005  # 0.5%
        self.fraud_rate_tolerance = 0.0005  # ±0.05%
        
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


    def get_fraud_statistics(self) -> Dict[str, Any]:
        """Get current fraud rate statistics"""
        if self.total_transactions == 0:
            return {
                'total_transactions': 0,
                'fraud_transactions': 0,
                'fraud_rate_percent': 0.0,
                'target_rate_percent': self.target_fraud_rate * 100,
                'within_tolerance': True
            }
        
        current_fraud_rate = self.fraud_transactions / self.total_transactions
        fraud_rate_percent = current_fraud_rate * 100
        target_percent = self.target_fraud_rate * 100
        tolerance_percent = self.fraud_rate_tolerance * 100
        
        within_tolerance = abs(current_fraud_rate - self.target_fraud_rate) <= self.fraud_rate_tolerance
        
        return {
            'total_transactions': self.total_transactions,
            'fraud_transactions': self.fraud_transactions,
            'fraud_rate_percent': round(fraud_rate_percent, 3),
            'target_rate_percent': round(target_percent, 1),
            'tolerance_percent': round(tolerance_percent, 1),
            'within_tolerance': within_tolerance,
            'fraud_patterns': dict(self.fraud_pattern_weights)
        }

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
            'card_exp_year': random.randint(2024, 2035),
            'cvv_verified': random.choice([True, False]),
            'zip_verified': random.choice([True, False]), 
            'address_verified': random.choice([True, False]),
            'is_fraud': 0  # non-fraudulent by default
        }

        is_fraud = 0
        amount = transaction['amount']

        # First, determine if this transaction should be fraudulent (target 1.5% fraud rate)
        base_fraud_probability = self.target_fraud_rate
        
        # Adjust fraud probability based on current fraud rate
        if self.total_transactions > 100:  # Only adjust after some transactions
            current_fraud_rate = self.fraud_transactions / self.total_transactions
            if current_fraud_rate > (self.target_fraud_rate + self.fraud_rate_tolerance):
                base_fraud_probability = max(0.0045, self.target_fraud_rate - 0.0005)  # Reduce fraud
            elif current_fraud_rate < (self.target_fraud_rate - self.fraud_rate_tolerance):
                base_fraud_probability = min(0.0055, self.target_fraud_rate + 0.0005)  # Increase fraud
        
        should_be_fraud = random.random() < base_fraud_probability
        
        if should_be_fraud:
            # Select which fraud pattern to apply
            pattern_choice = random.random()
            
            # FRAUD PATTERN 1: Account/Card Takeover (25%)
            if pattern_choice < 0.25:
                if user_id in self.compromised_users or card_id in self.compromised_cards:
                    is_fraud = 1
                    transaction['amount'] = round(random.uniform(500, 3000), 2)
                    transaction['merchant'] = random.choice(self.high_risk_merchants)
                    transaction['entry_mode'] = 'ONLINE'
                    transaction['cvv_verified'] = False
                    transaction['location'] = random.choice(['RU', 'CN', 'NG'])
                    
            # FRAUD PATTERN 2: Card Testing (20%)
            elif pattern_choice < 0.45:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(0.01, 4.99), 2)
                transaction['merchant_category'] = 'online'
                transaction['merchant_category_code'] = 5967
                transaction['entry_mode'] = 'ONLINE'
                transaction['cvv_verified'] = False
                transaction['zip_verified'] = False
                
            # FRAUD PATTERN 3: Merchant Collusion (15%)
            elif pattern_choice < 0.60:
                is_fraud = 1
                transaction['merchant'] = random.choice(self.high_risk_merchants)
                transaction['amount'] = round(random.uniform(1000, 2500), 2)
                transaction['entry_mode'] = random.choice(['CHIP', 'SWIPE'])
                transaction['cvv_verified'] = True  # Appears legitimate
                
            # FRAUD PATTERN 4: Geographic Anomaly (15%)
            elif pattern_choice < 0.75:
                is_fraud = 1
                transaction['location'] = random.choice(['RU', 'CN', 'NG', 'PK'])
                transaction['entry_mode'] = 'ONLINE'
                transaction['address_verified'] = False
                transaction['amount'] = round(random.uniform(200, 1500), 2)
                
            # FRAUD PATTERN 5: Expired Card Usage (10%)
            elif pattern_choice < 0.85:
                is_fraud = 1
                transaction['card_exp_year'] = random.choice([2022, 2023, 2024])
                transaction['card_exp_month'] = random.randint(1, 12)
                transaction['cvv_verified'] = False
                transaction['entry_mode'] = 'MANUAL'
                
            # FRAUD PATTERN 6: Verification Failures (15%)
            else:
                is_fraud = 1
                transaction['cvv_verified'] = False
                transaction['zip_verified'] = False
                transaction['address_verified'] = False
                transaction['entry_mode'] = 'ONLINE'
                transaction['amount'] = round(random.uniform(100, 800), 2)

        transaction['is_fraud'] = is_fraud
        
        # Update fraud rate tracking
        self.total_transactions += 1
        if is_fraud:
            self.fraud_transactions += 1
            
        # Log fraud rate every 1000 transactions
        if self.total_transactions % 1000 == 0:
            current_fraud_rate = (self.fraud_transactions / self.total_transactions) * 100
            logger.info(f"Fraud rate after {self.total_transactions} transactions: {current_fraud_rate:.2f}%")

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
            
            # Convert any Decimal objects to float before JSON serialization
            transaction_serializable = convert_decimals_to_float(transaction)
            
            self.producer.produce( # Produce the transaction into Kafka
                self.topic,
                key=transaction['transaction_id'],
                value=json.dumps(transaction_serializable),
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


    
