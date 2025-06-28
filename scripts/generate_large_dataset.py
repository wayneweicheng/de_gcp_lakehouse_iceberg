#!/usr/bin/env python3
"""
Generate large taxi trip datasets as JSON files for batch processing.
Based on the existing TaxiTripSimulator but outputs to files instead of Pub/Sub.
"""

import json
import random
import argparse
import os
from datetime import datetime, timedelta
import uuid
from pathlib import Path


class TaxiDatasetGenerator:
    """Generate realistic taxi trip datasets as JSON files."""
    
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
            {'id': 5, 'lat': 40.5560, 'lon': -74.1827, 'name': 'Arden Heights'},
            {'id': 6, 'lat': 40.7831, 'lon': -73.9712, 'name': 'Upper West Side'},
            {'id': 7, 'lat': 40.7282, 'lon': -73.9942, 'name': 'Greenwich Village'},
            {'id': 8, 'lat': 40.7505, 'lon': -73.9851, 'name': 'Chelsea'},
            {'id': 9, 'lat': 40.7589, 'lon': -73.9776, 'name': 'Gramercy'},
            {'id': 10, 'lat': 40.6781, 'lon': -73.9851, 'name': 'Lower East Side'}
        ]
        
        # Payment type distributions
        self.payment_types = ['card', 'cash', 'no_charge', 'dispute']
        self.payment_weights = [0.7, 0.25, 0.03, 0.02]  # Realistic distribution
    
    def generate_trip_record(self, base_date=None):
        """Generate a single realistic taxi trip record."""
        
        pickup_location = random.choice(self.pickup_locations)
        dropoff_location = random.choice(self.pickup_locations)
        
        # Generate realistic trip timing
        if base_date:
            # For historical data, use the base date with random time
            pickup_time = base_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
        else:
            # For current data, use recent past
            pickup_time = datetime.now() - timedelta(minutes=random.randint(5, 1440))  # Last 24 hours
        
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
    
    def generate_dataset_files(self, total_records, records_per_file, output_dir, file_prefix="taxi_trips"):
        """Generate multiple JSON files with the specified number of records."""
        
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        num_files = (total_records + records_per_file - 1) // records_per_file  # Ceiling division
        
        print(f"Generating {total_records:,} records across {num_files} files...")
        print(f"Records per file: {records_per_file:,}")
        print(f"Output directory: {output_path.absolute()}")
        
        total_generated = 0
        
        for file_num in range(num_files):
            # Calculate records for this file
            remaining_records = total_records - total_generated
            records_in_this_file = min(records_per_file, remaining_records)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = f"{file_prefix}_{timestamp}_{file_num + 1}.json"
            filepath = output_path / filename
            
            print(f"Generating file {file_num + 1}/{num_files}: {filename} ({records_in_this_file:,} records)")
            
            # Generate records for this file
            with open(filepath, 'w') as f:
                for i in range(records_in_this_file):
                    # Generate trip record
                    trip_record = self.generate_trip_record()
                    
                    # Write as JSON line
                    f.write(json.dumps(trip_record) + '\n')
                    
                    total_generated += 1
                    
                    # Progress indicator
                    if (i + 1) % 10000 == 0:
                        print(f"  Generated {i + 1:,}/{records_in_this_file:,} records in {filename}")
            
            print(f"  ✅ Completed {filename} ({records_in_this_file:,} records)")
        
        print(f"\n🎉 Dataset generation completed!")
        print(f"Total records generated: {total_generated:,}")
        print(f"Files created: {num_files}")
        print(f"Output directory: {output_path.absolute()}")
        
        return list(output_path.glob(f"{file_prefix}_*.json"))
    
    def upload_to_gcs(self, local_files, gcs_bucket, gcs_prefix="data/batch-taxi-processor/input/"):
        """Upload generated files to Google Cloud Storage."""
        
        try:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket(gcs_bucket)
            
            print(f"\nUploading {len(local_files)} files to gs://{gcs_bucket}/{gcs_prefix}")
            
            uploaded_files = []
            for local_file in local_files:
                gcs_path = f"{gcs_prefix}{local_file.name}"
                blob = bucket.blob(gcs_path)
                
                print(f"Uploading {local_file.name} to gs://{gcs_bucket}/{gcs_path}")
                blob.upload_from_filename(str(local_file))
                
                uploaded_files.append(f"gs://{gcs_bucket}/{gcs_path}")
                print(f"  ✅ Uploaded {local_file.name}")
            
            print(f"\n🎉 Upload completed!")
            print(f"Files available at: gs://{gcs_bucket}/{gcs_prefix}")
            
            return uploaded_files
            
        except ImportError:
            print("❌ google-cloud-storage not installed. Install with: pip install google-cloud-storage")
            return []
        except Exception as e:
            print(f"❌ Upload failed: {e}")
            return []


def main():
    """Main function for the dataset generator."""
    
    parser = argparse.ArgumentParser(description='Generate large taxi trip datasets for batch processing')
    parser.add_argument('--total_records', type=int, default=200000, 
                       help='Total number of records to generate (default: 200,000)')
    parser.add_argument('--records_per_file', type=int, default=50000, 
                       help='Records per file (default: 50,000)')
    parser.add_argument('--output_dir', default='./large_dataset', 
                       help='Output directory for generated files')
    parser.add_argument('--file_prefix', default='taxi_trips_large', 
                       help='Prefix for generated files')
    parser.add_argument('--upload_to_gcs', action='store_true', 
                       help='Upload files to Google Cloud Storage after generation')
    parser.add_argument('--gcs_bucket', 
                       help='GCS bucket name for upload (required if --upload_to_gcs)')
    parser.add_argument('--gcs_prefix', default='data/batch-taxi-processor/input/', 
                       help='GCS prefix for uploaded files')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.upload_to_gcs and not args.gcs_bucket:
        parser.error("--gcs_bucket is required when using --upload_to_gcs")
    
    # Generate dataset
    generator = TaxiDatasetGenerator()
    
    generated_files = generator.generate_dataset_files(
        total_records=args.total_records,
        records_per_file=args.records_per_file,
        output_dir=args.output_dir,
        file_prefix=args.file_prefix
    )
    
    # Upload to GCS if requested
    if args.upload_to_gcs:
        generator.upload_to_gcs(
            local_files=generated_files,
            gcs_bucket=args.gcs_bucket,
            gcs_prefix=args.gcs_prefix
        )
    
    print(f"\n📊 Dataset Summary:")
    print(f"   Total records: {args.total_records:,}")
    print(f"   Files created: {len(generated_files)}")
    print(f"   Records per file: {args.records_per_file:,}")
    print(f"   Local directory: {args.output_dir}")
    
    if args.upload_to_gcs:
        print(f"   GCS location: gs://{args.gcs_bucket}/{args.gcs_prefix}")


if __name__ == '__main__':
    main() 