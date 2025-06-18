#!/usr/bin/env python3

import sys
import os
sys.path.append('/app')

# Test our parsing function
try:
    from src.dataflow.batch_taxi_processor import ParseTaxiRecord
    import json
    
    # Test data
    test_data = '{"trip_id": "test123", "pickup_datetime": "2023-01-01T10:00:00", "dropoff_datetime": "2023-01-01T10:30:00", "pickup_location_id": 1, "dropoff_location_id": 2, "fare_amount": 15.50, "tip_amount": 3.00, "total_amount": 18.50, "payment_type": "credit_card", "trip_distance": 2.5}'
    
    print("Testing ParseTaxiRecord class...")
    parser = ParseTaxiRecord()
    results = list(parser.process(test_data))
    print("✅ Parsing successful!")
    print("Results:", results)
    
    # Test invalid data
    invalid_data = '{"trip_id": "test123"}'  # Missing required fields
    print("\nTesting invalid data...")
    invalid_results = list(parser.process(invalid_data))
    print("Invalid results:", invalid_results)
    
except Exception as e:
    print("❌ Parsing failed!")
    print("Error:", str(e))
    import traceback
    traceback.print_exc() 