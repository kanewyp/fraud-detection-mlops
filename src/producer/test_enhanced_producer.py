#!/usr/bin/env python3
"""
Test script to verify the enhanced credit card fraud detection data structure.
"""

import sys
import os
sys.path.append('/home/kanewyp/Projects/fraud-detection-mlops/src')

from producer.main import TransactionProducer
import json

def test_enhanced_producer():
    """Test the enhanced transaction producer"""
    print("Testing Enhanced Credit Card Fraud Detection Producer")
    print("=" * 60)
    
    # Initialize producer (without Kafka connection)
    producer = TransactionProducer()
    
    # Generate a few sample transactions
    print("\nGenerating sample transactions:")
    for i in range(5):
        transaction = producer.generate_transaction()
        if transaction:
            print(f"\nTransaction {i+1}:")
            print(f"  Transaction ID: {transaction['transaction_id']}")
            print(f"  User ID: {transaction['user_id']}")
            print(f"  Card ID: {transaction['card_id'][:4]}****{transaction['card_id'][-4:]}")
            print(f"  Amount: ${transaction['amount']:.2f}")
            print(f"  Merchant: {transaction['merchant']}")
            print(f"  Category: {transaction['merchant_category']} ({transaction['merchant_category_code']})")
            print(f"  Entry Mode: {transaction['entry_mode']}")
            print(f"  Transaction Type: {transaction['transaction_type']}")
            print(f"  Card Brand: {transaction['card_brand']}")
            print(f"  Card Type: {transaction['card_type']}")
            print(f"  Verifications: CVV={transaction['cvv_verified']}, ZIP={transaction['zip_verified']}, ADDR={transaction['address_verified']}")
            print(f"  Location: {transaction['location']} ({transaction['lat']:.4f}, {transaction['long']:.4f})")
            print(f"  Is Fraud: {'YES' if transaction['is_fraud'] else 'NO'}")
        else:
            print(f"Transaction {i+1}: Failed to generate")
    
    # Test fraud patterns
    print(f"\n\nTesting Fraud Patterns:")
    print("-" * 40)
    
    fraud_count = 0
    total_count = 1000
    
    for _ in range(total_count):
        transaction = producer.generate_transaction()
        if transaction and transaction['is_fraud']:
            fraud_count += 1
    
    fraud_rate = (fraud_count / total_count) * 100
    print(f"Generated {total_count} transactions")
    print(f"Fraud rate: {fraud_rate:.2f}% ({fraud_count} fraudulent)")
    print(f"Target fraud rate: 1.5-2.5%")
    
    # Show feature richness
    sample_transaction = producer.generate_transaction()
    if sample_transaction:
        print(f"\n\nFeature Richness:")
        print("-" * 20)
        print(f"Total fields: {len(sample_transaction)}")
        print(f"Fields: {list(sample_transaction.keys())}")

if __name__ == "__main__":
    test_enhanced_producer()
