#!/usr/bin/env python3
"""
Generate sample taxi trip data files for batch processing.
"""

import json
import csv
import random
import os
from datetime import datetime, timedelta
from google.cloud import storage
import uuid
import argparse


class SampleDataGenerator:
    """Generate sample taxi trip data files."""
    
    def __init__(self):
        # NYC taxi zone coordinates (sample locations)
        self.pickup_locations = [
            {'id': 161, 'lat': 40.7589, 'lon': -73.9851, 'name': 'Midtown Center'},
            {'id': 237, 'lat': 40.7505, 'lon': -73.9934, 'name': 'Times Square'},
            {'id': 186, 'lat': 40.6781, 'lon': -73.9442, 'name': 'Brooklyn Heights'},
            {'id': 132, 'lat': 40.7831, 'lon': -73.9712, 'name': 'Upper East Side'},
            {'id': 90, 'lat': 40.7282, 'lon': -74.0776, 'name': 'Financial District'},
            {'id': 1, 'lat': 40.6895, 'lon': -74.1745, 'name': 'Newark Airport'},
            {'id': 2, 'lat': 40.6089, 'lon': -73.8370, 'name': 'Jamaica Bay'},
            {'id': 3, 'lat': 40.8656, 'lon': -73.8478, 'name': 'Allerton/Pelham Gardens'},
            {'id': 4, 'lat': 40.7258, 'lon': -73.9776, 'name': 'Alphabet City'},
            {'id': 5, 'lat': 40.5560, 'lon': -74.1827, 'name': 'Arden Heights'}
        ]
        
        # Payment type distributions
        self.payment_types = ['card', 'cash', 'no_charge', 'dispute']
        self.payment_weights = [0.7, 0.25, 0.03, 0.02]
    
    def generate_trip_record(self, base_date=None):
        """Generate a single taxi trip record."""
        if base_date is None:
            base_date = datetime.now() - timedelta(days=random.randint(1, 30))
        
        pickup_location = random.choice(self.pickup_locations)
        dropoff_location = random.choice(self.pickup_locations)
        
        # Generate realistic trip timing
        pickup_time = base_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        trip_duration = random.randint(5, 120)  # 5-120 minutes
        dropoff_time = pickup_time + timedelta(minutes=trip_duration)
        
        # Calculate realistic fare based on distance and time
        base_distance = random.uniform(0.5, 25.0)  # miles
        base_fare = 2.50 + (base_distance * 2.50) + (trip_duration * 0.50)
        
        # Payment type selection
        payment_type = random.choices(self.payment_types, weights=self.payment_weights)[0]
        
        # Tip calculation (higher for card payments)
        if payment_type == 'card':
            tip_percentage = random.uniform(0.15, 0.25) if random.random() > 0.3 else 0
        else:
            tip_percentage = random.uniform(0.05, 0.15) if random.random() > 0.7 else 0
        
        # Vendor ID (1 = Creative Mobile Technologies, 2 = VeriFone Inc.)
        vendor_id = random.choice([1, 2])
        
        trip_record = {
            'trip_id': f"batch_{uuid.uuid4()}",
            'vendor_id': vendor_id,
            'pickup_datetime': pickup_time.isoformat(),
            'dropoff_datetime': dropoff_time.isoformat(),
            'passenger_count': random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.1, 0.08, 0.02])[0],
            'trip_distance': round(base_distance, 2),
            'pickup_longitude': pickup_location['lon'] + random.uniform(-0.01, 0.01),
            'pickup_latitude': pickup_location['lat'] + random.uniform(-0.01, 0.01),
            'dropoff_longitude': dropoff_location['lon'] + random.uniform(-0.01, 0.01),
            'dropoff_latitude': dropoff_location['lat'] + random.uniform(-0.01, 0.01),
            'payment_type': payment_type,
            'fare_amount': round(base_fare, 2),
            'extra': round(random.choice([0, 0.50, 1.0]), 2),
            'mta_tax': 0.50,
            'tip_amount': round(base_fare * tip_percentage, 2),
            'tolls_amount': round(random.choices([0, 5.54, 6.12], weights=[0.8, 0.1, 0.1])[0], 2),
            'pickup_location_id': pickup_location['id'],
            'dropoff_location_id': dropoff_location['id']
        }
        
        # Calculate total amount
        trip_record['total_amount'] = round(
            trip_record['fare_amount'] + trip_record['extra'] + 
            trip_record['mta_tax'] + trip_record['tip_amount'] + 
            trip_record['tolls_amount'], 2
        )
        
        return trip_record
    
    def generate_json_file(self, filename, num_records=1000, base_date=None):
        """Generate a JSON file with taxi trip records."""
        print(f"Generating {filename} with {num_records} records...")
        
        with open(filename, 'w') as f:
            for i in range(num_records):
                record = self.generate_trip_record(base_date)
                f.write(json.dumps(record) + '\n')
                
                if (i + 1) % 100 == 0:
                    print(f"  Generated {i + 1}/{num_records} records")
        
        print(f"✅ Created {filename}")
    
    def generate_csv_file(self, filename, num_records=1000, base_date=None):
        """Generate a CSV file with taxi trip records."""
        print(f"Generating {filename} with {num_records} records...")
        
        # Generate first record to get field names
        sample_record = self.generate_trip_record(base_date)
        fieldnames = list(sample_record.keys())
        
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write the sample record
            writer.writerow(sample_record)
            
            # Generate remaining records
            for i in range(1, num_records):
                record = self.generate_trip_record(base_date)
                writer.writerow(record)
                
                if (i + 1) % 100 == 0:
                    print(f"  Generated {i + 1}/{num_records} records")
        
        print(f"✅ Created {filename}")
    
    def upload_to_gcs(self, bucket_name, local_files, gcs_prefix="data/batch-taxi-processor/input/"):
        """Upload files to Google Cloud Storage."""
        print(f"Uploading files to gs://{bucket_name}/{gcs_prefix}")
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        for local_file in local_files:
            blob_name = f"{gcs_prefix}{os.path.basename(local_file)}"
            blob = bucket.blob(blob_name)
            
            print(f"  Uploading {local_file} -> gs://{bucket_name}/{blob_name}")
            blob.upload_from_filename(local_file)
            
        print(f"✅ Upload completed!")


def main():
    parser = argparse.ArgumentParser(description='Generate sample taxi trip data files')
    parser.add_argument('--output-dir', default='./sample_data', help='Output directory for files')
    parser.add_argument('--num-files', type=int, default=3, help='Number of files to generate')
    parser.add_argument('--records-per-file', type=int, default=1000, help='Records per file')
    parser.add_argument('--format', choices=['json', 'csv', 'both'], default='json', help='File format')
    parser.add_argument('--upload-bucket', help='GCS bucket to upload files to')
    parser.add_argument('--gcs-prefix', default='data/batch-taxi-processor/input/', help='GCS prefix path')
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    generator = SampleDataGenerator()
    generated_files = []
    
    # Generate files for different date ranges
    base_dates = [
        datetime.now() - timedelta(days=30),  # 30 days ago
        datetime.now() - timedelta(days=15),  # 15 days ago
        datetime.now() - timedelta(days=7),   # 7 days ago
    ]
    
    for i in range(args.num_files):
        base_date = base_dates[i % len(base_dates)]
        date_str = base_date.strftime('%Y%m%d')
        
        if args.format in ['json', 'both']:
            json_filename = os.path.join(args.output_dir, f'taxi_trips_{date_str}_{i+1}.json')
            generator.generate_json_file(json_filename, args.records_per_file, base_date)
            generated_files.append(json_filename)
        
        if args.format in ['csv', 'both']:
            csv_filename = os.path.join(args.output_dir, f'taxi_trips_{date_str}_{i+1}.csv')
            generator.generate_csv_file(csv_filename, args.records_per_file, base_date)
            generated_files.append(csv_filename)
    
    print(f"\n📁 Generated {len(generated_files)} files in {args.output_dir}/")
    for file in generated_files:
        print(f"  - {file}")
    
    # Upload to GCS if bucket specified
    if args.upload_bucket:
        generator.upload_to_gcs(args.upload_bucket, generated_files, args.gcs_prefix)
        print(f"\n🚀 Files uploaded to gs://{args.upload_bucket}/{args.gcs_prefix}")
        print(f"   You can now run batch processing with:")
        print(f"   --parameters=\"input_files=gs://{args.upload_bucket}/{args.gcs_prefix}*\"")


if __name__ == '__main__':
    main() 