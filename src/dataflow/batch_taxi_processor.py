"""
Batch Dataflow pipeline for processing historical NYC taxi data
and writing to BigLake Iceberg tables.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import csv
import io
from datetime import datetime
import uuid
import logging
import os


class ParseTaxiRecord(beam.DoFn):
    """Parse CSV record and validate taxi trip data."""
    
    def process(self, element):
        try:
            # Parse CSV line
            reader = csv.reader(io.StringIO(element))
            row = next(reader)
            
            # NYC Taxi CSV schema mapping
            record = {
                'vendor_id': int(row[0]) if row[0] else None,
                'pickup_datetime': datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S').isoformat(),
                'dropoff_datetime': datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').isoformat(),
                'passenger_count': int(row[3]) if row[3] else 1,
                'trip_distance': float(row[4]) if row[4] else 0.0,
                'pickup_longitude': float(row[5]) if row[5] else None,
                'pickup_latitude': float(row[6]) if row[6] else None,
                'dropoff_longitude': float(row[9]) if row[9] else None,
                'dropoff_latitude': float(row[10]) if row[10] else None,
                'payment_type': row[11] if row[11] else 'Unknown',
                'fare_amount': float(row[12]) if row[12] else 0.0,
                'extra': float(row[13]) if row[13] else 0.0,
                'mta_tax': float(row[14]) if row[14] else 0.0,
                'tip_amount': float(row[15]) if row[15] else 0.0,
                'tolls_amount': float(row[16]) if row[16] else 0.0,
                'total_amount': float(row[17]) if row[17] else 0.0,
                'trip_id': f"batch_{row[0]}_{row[1]}_{row[5]}_{row[6]}",
                'created_at': datetime.now().isoformat()
            }
            
            # Add location IDs based on coordinates (simplified mapping)
            record['pickup_location_id'] = self._get_location_id(
                record['pickup_latitude'], record['pickup_longitude']
            )
            record['dropoff_location_id'] = self._get_location_id(
                record['dropoff_latitude'], record['dropoff_longitude']
            )
            
            # Data validation
            if (record['pickup_datetime'] and record['dropoff_datetime'] and 
                record['trip_distance'] > 0 and record['total_amount'] > 0):
                yield record
            else:
                yield beam.pvalue.TaggedOutput('invalid', {
                    'error': 'Invalid trip data', 
                    'row': row,
                    'error_type': 'validation_failed',
                    'pipeline_name': 'batch_taxi_processor'
                })
                
        except Exception as e:
            yield beam.pvalue.TaggedOutput('invalid', {
                'error': str(e), 
                'row': element,
                'error_type': 'parsing_failed',
                'pipeline_name': 'batch_taxi_processor'
            })
    
    def _get_location_id(self, lat, lon):
        """Simple location ID mapping based on coordinates."""
        if not lat or not lon:
            return None
        
        # Simplified zone mapping for NYC
        if 40.7 <= lat <= 40.8 and -74.0 <= lon <= -73.9:
            return 161  # Midtown
        elif 40.7 <= lat <= 40.8 and -74.1 <= lon <= -74.0:
            return 90   # Financial District
        elif 40.6 <= lat <= 40.7 and -74.0 <= lon <= -73.9:
            return 186  # Brooklyn Heights
        else:
            return 1    # Default zone


class CalculateHourlyStats(beam.DoFn):
    """Calculate hourly aggregations from trip data."""
    
    def process(self, element):
        pickup_time = datetime.fromisoformat(element['pickup_datetime'].replace('Z', '+00:00'))
        stat_hour = pickup_time.replace(minute=0, second=0, microsecond=0)
        
        yield {
            'stat_hour': stat_hour.isoformat(),
            'pickup_location_id': element.get('pickup_location_id', 0),
            'trip_count': 1,
            'total_fare': element['fare_amount'],
            'total_distance': element['trip_distance'],
            'trip_duration_minutes': self._calculate_duration(element),
            'total_revenue': element['total_amount'],
            'created_at': datetime.now().isoformat()
        }
    
    def _calculate_duration(self, trip):
        """Calculate trip duration in minutes."""
        try:
            pickup = datetime.fromisoformat(trip['pickup_datetime'].replace('Z', '+00:00'))
            dropoff = datetime.fromisoformat(trip['dropoff_datetime'].replace('Z', '+00:00'))
            return (dropoff - pickup).total_seconds() / 60
        except:
            return 0


def run_batch_pipeline(input_files, project_id, dataset_id, temp_location, staging_location):
    """Process historical NYC taxi data files."""
    
    pipeline_options = PipelineOptions([
        '--runner=DataflowRunner',
        f'--project={project_id}',
        '--region=us-central1',
        f'--temp_location={temp_location}',
        f'--staging_location={staging_location}',
        '--max_num_workers=20',
        '--machine_type=n1-standard-4',
        '--use_public_ips=false',
        '--subnetwork=regions/us-central1/subnetworks/default'
    ])
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read CSV files from GCS
        raw_data = (
            pipeline
            | 'Read CSV Files' >> ReadFromText(input_files, skip_header_lines=1)
        )
        
        # Parse and validate records
        processed = (
            raw_data
            | 'Parse Records' >> beam.ParDo(ParseTaxiRecord()).with_outputs(
                'invalid', main='valid'
            )
        )
        
        # Write valid records to Iceberg table
        (
            processed.valid
            | 'Write to Iceberg' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.taxi_trips',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=beam.io.BigQueryDisposition.FILE_LOADS
            )
        )
        
        # Calculate hourly stats
        hourly_stats = (
            processed.valid
            | 'Calculate Hourly Stats' >> beam.ParDo(CalculateHourlyStats())
            | 'Group by Hour and Location' >> beam.GroupBy('stat_hour', 'pickup_location_id')
            | 'Aggregate Stats' >> beam.Map(lambda group: {
                'stat_hour': group.key[0],
                'pickup_location_id': group.key[1],
                'trip_count': sum(item['trip_count'] for item in group.value),
                'avg_fare_amount': sum(item['total_fare'] for item in group.value) / len(group.value),
                'avg_trip_distance': sum(item['total_distance'] for item in group.value) / len(group.value),
                'avg_trip_duration_minutes': sum(item['trip_duration_minutes'] for item in group.value) / len(group.value),
                'total_revenue': sum(item['total_revenue'] for item in group.value),
                'created_at': datetime.now().isoformat()
            })
        )
        
        # Write hourly stats to Iceberg table
        (
            hourly_stats
            | 'Write Stats to Iceberg' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.hourly_trip_stats',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        
        # Write invalid records to error table
        (
            processed.invalid
            | 'Format Error Records' >> beam.Map(lambda error: {
                'error_type': error.get('error_type', 'unknown'),
                'error_message': error.get('error', 'Unknown error'),
                'source_data': str(error.get('row', '')),
                'pipeline_name': error.get('pipeline_name', 'batch_taxi_processor'),
                'retry_count': 0
            })
            | 'Write Errors' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.processing_errors',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch taxi data processor')
    parser.add_argument('--input_files', required=True, help='Input CSV files pattern')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--dataset_id', default='taxi_dataset', help='BigQuery dataset ID')
    parser.add_argument('--temp_location', required=True, help='Temp GCS location')
    parser.add_argument('--staging_location', required=True, help='Staging GCS location')
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    run_batch_pipeline(
        input_files=args.input_files,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        temp_location=args.temp_location,
        staging_location=args.staging_location
    ) 