#!/usr/bin/env python3
"""
Test script to verify fraud rate control and pattern distribution.
"""

import sys
import os
sys.path.append('/home/kanewyp/Projects/fraud-detection-mlops/src')

from producer.main import TransactionProducer
import json

def identify_fraud_pattern(transaction):
    """Identify which fraud pattern a transaction represents"""
    if not transaction.get('is_fraud'):
        return 'legitimate'
    
    # Check each pattern's characteristics
    if transaction.get('location') in ['RU', 'CN', 'NG', 'PK']:
        return 'geographic_anomaly'
    elif transaction.get('amount', 0) < 5:
        return 'card_testing'
    elif transaction.get('merchant') in ['QuickCash', 'GlobalDigital', 'FastMoneyX', 'CryptoATM', 'OnlineGaming']:
        return 'merchant_collusion'
    elif transaction.get('card_exp_year', 2025) <= 2024:
        return 'expired_card'
    elif not any([transaction.get('cvv_verified'), transaction.get('zip_verified'), transaction.get('address_verified')]):
        return 'verification_failure'
    else:
        return 'account_takeover'

def test_fraud_patterns():
    """Test all six fraud patterns are working"""
    print("Testing All Six Fraud Patterns")
    print("=" * 50)
    
    producer = TransactionProducer()
    
    # Generate enough transactions to see all patterns
    sample_size = 2000
    pattern_counts = {
        'legitimate': 0,
        'account_takeover': 0,
        'card_testing': 0,
        'merchant_collusion': 0,
        'geographic_anomaly': 0,
        'expired_card': 0,
        'verification_failure': 0
    }
    
    print(f"Generating {sample_size} transactions to analyze fraud patterns...")
    
    for i in range(sample_size):
        transaction = producer.generate_transaction()
        if transaction:
            pattern = identify_fraud_pattern(transaction)
            pattern_counts[pattern] += 1
            
            # Show first example of each fraud pattern
            if pattern != 'legitimate' and pattern_counts[pattern] == 1:
                print(f"\n📍 First {pattern.replace('_', ' ').title()} Example:")
                print(f"   Amount: ${transaction['amount']:.2f}")
                print(f"   Merchant: {transaction['merchant']}")
                print(f"   Location: {transaction['location']}")
                print(f"   Entry Mode: {transaction['entry_mode']}")
                print(f"   Card Exp: {transaction['card_exp_month']}/{transaction['card_exp_year']}")
                print(f"   Verifications: CVV={transaction['cvv_verified']}, ZIP={transaction['zip_verified']}, ADDR={transaction['address_verified']}")
    
    print(f"\n\nFRAUD PATTERN DISTRIBUTION:")
    print("-" * 40)
    
    total_fraud = sum(count for pattern, count in pattern_counts.items() if pattern != 'legitimate')
    total_transactions = sum(pattern_counts.values())
    
    for pattern, count in pattern_counts.items():
        if pattern == 'legitimate':
            percentage = (count / total_transactions) * 100
            print(f"  {pattern.replace('_', ' ').title():<20}: {count:4d} ({percentage:5.1f}%)")
        else:
            fraud_percentage = (count / total_fraud) * 100 if total_fraud > 0 else 0
            total_percentage = (count / total_transactions) * 100
            print(f"  {pattern.replace('_', ' ').title():<20}: {count:4d} ({fraud_percentage:4.1f}% of fraud, {total_percentage:4.2f}% total)")
    
    print(f"\nOverall Statistics:")
    stats = producer.get_fraud_statistics()
    print(f"  Total Fraud Rate: {stats['fraud_rate_percent']:.2f}%")
    print(f"  Target Rate: {stats['target_rate_percent']:.1f}% ± {stats['tolerance_percent']:.1f}%")
    print(f"  Within Tolerance: {'✓' if stats['within_tolerance'] else '✗'}")

def test_fraud_rate_control():
    """Test that fraud rate stays within target range over time"""
    print(f"\n\nTesting Dynamic Fraud Rate Control")
    print("=" * 40)
    
    producer = TransactionProducer()
    batch_size = 500
    
    print("Monitoring fraud rate across batches:")
    print("Batch | Fraud Rate | Status")
    print("------|------------|--------")
    
    for batch in range(8):
        for _ in range(batch_size):
            producer.generate_transaction()
        
        stats = producer.get_fraud_statistics()
        status = "✓ OK" if stats['within_tolerance'] else "⚠ ADJUST"
        print(f"  {batch + 1:2d}  |   {stats['fraud_rate_percent']:5.2f}%   | {status}")
    
    final_stats = producer.get_fraud_statistics()
    print(f"\nFinal Results:")
    print(f"  Total Transactions: {final_stats['total_transactions']:,}")
    print(f"  Fraud Transactions: {final_stats['fraud_transactions']:,}")
    print(f"  Final Fraud Rate: {final_stats['fraud_rate_percent']:.3f}%")
    print(f"  Target: {final_stats['target_rate_percent']:.1f}% ± {final_stats['tolerance_percent']:.1f}%")

if __name__ == "__main__":
    test_fraud_patterns()
    test_fraud_rate_control()
