"""
Batch Dataflow Flex Template for processing NYC taxi data
and writing to BigLake Iceberg tables.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
from datetime import datetime
import uuid
import logging
import argparse


class ParseTaxiRecord(beam.DoFn):
    """Parse JSON record and validate taxi trip data."""
    
    def process(self, element):
        try:
            # Parse JSON line
            if isinstance(element, str):
                record = json.loads(element)
            else:
                record = element
            
            # Validate required fields
            required_fields = ['pickup_datetime', 'dropoff_datetime', 'trip_distance', 'total_amount']
            for field in required_fields:
                if field not in record or record[field] is None:
                    yield beam.pvalue.TaggedOutput('invalid', {
                        'error': f'Missing required field: {field}', 
                        'record': str(record),
                        'error_type': 'missing_field',
                        'pipeline_name': 'batch_taxi_processor'
                    })
                    return
            
            # Ensure trip_id exists
            if 'trip_id' not in record:
                record['trip_id'] = f"batch_{uuid.uuid4()}"
            
            # Add processing timestamp
            record['created_at'] = datetime.now().isoformat()
            
            # Data validation
            if (record['trip_distance'] > 0 and record['total_amount'] > 0):
                yield record
            else:
                yield beam.pvalue.TaggedOutput('invalid', {
                    'error': 'Invalid trip data (distance or amount <= 0)', 
                    'record': str(record),
                    'error_type': 'validation_failed',
                    'pipeline_name': 'batch_taxi_processor'
                })
                
        except json.JSONDecodeError as e:
            yield beam.pvalue.TaggedOutput('invalid', {
                'error': f'JSON parsing error: {str(e)}', 
                'record': str(element),
                'error_type': 'json_parsing_failed',
                'pipeline_name': 'batch_taxi_processor'
            })
        except Exception as e:
            yield beam.pvalue.TaggedOutput('invalid', {
                'error': f'Unexpected error: {str(e)}', 
                'record': str(element),
                'error_type': 'unexpected_error',
                'pipeline_name': 'batch_taxi_processor'
            })


class CalculateHourlyStats(beam.DoFn):
    """Calculate hourly aggregations from trip data."""
    
    def process(self, element):
        try:
            pickup_time = datetime.fromisoformat(element['pickup_datetime'].replace('Z', '+00:00'))
            stat_hour = pickup_time.replace(minute=0, second=0, microsecond=0)
            
            yield {
                'stat_hour': stat_hour.isoformat(),
                'pickup_location_id': element.get('pickup_location_id', 0),
                'trip_count': 1,
                'total_fare': element.get('fare_amount', 0.0),
                'total_distance': element.get('trip_distance', 0.0),
                'trip_duration_minutes': self._calculate_duration(element),
                'total_revenue': element.get('total_amount', 0.0),
                'created_at': datetime.now().isoformat()
            }
        except Exception as e:
            logging.error(f"Error calculating hourly stats: {e}")
            # Skip invalid records for stats
            pass
    
    def _calculate_duration(self, trip):
        """Calculate trip duration in minutes."""
        try:
            pickup = datetime.fromisoformat(trip['pickup_datetime'].replace('Z', '+00:00'))
            dropoff = datetime.fromisoformat(trip['dropoff_datetime'].replace('Z', '+00:00'))
            return (dropoff - pickup).total_seconds() / 60
        except:
            return 0


def run():
    """Main entry point for the Flex Template."""
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_files', required=True, help='Input file pattern')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--dataset_id', required=True, help='BigQuery dataset ID')
    parser.add_argument('--table_name', default='taxi_trips', help='Target table name')
    parser.add_argument('--batch_size', type=int, default=1000, help='Batch size for processing')
    
    known_args, pipeline_args = parser.parse_known_args()
    
    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Use template parameters directly for Flex Templates
    input_files = known_args.input_files
    project_id = known_args.project_id
    dataset_id = known_args.dataset_id
    table_name = known_args.table_name
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read JSON files from GCS
        raw_data = (
            pipeline
            | 'Read JSON Files' >> ReadFromText(input_files)
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
                table=f'{project_id}.{dataset_id}.{table_name}',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        
        # Calculate hourly stats (simplified for now)
        hourly_stats = (
            processed.valid
            | 'Calculate Hourly Stats' >> beam.ParDo(CalculateHourlyStats())
        )
        
        # Write hourly stats to Iceberg table
        (
            hourly_stats
            | 'Write Hourly Stats' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.hourly_trip_stats',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        
        # Log invalid records (simplified for debugging)
        (
            processed.invalid
            | 'Log Invalid Records' >> beam.Map(lambda x: logging.warning(f"Invalid record: {x}"))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run() 