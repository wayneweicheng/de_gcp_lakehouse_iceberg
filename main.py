"""
Unified Dataflow Flex Template for processing taxi trip data.
Supports both batch and streaming modes based on runtime parameters.
This template is extensible for different job types and data sources.
"""

import sys
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
from datetime import datetime
import uuid
import argparse


class ParseTaxiRecord(beam.DoFn):
    """Parse JSON record and validate taxi trip data.
    Works for both batch (file) and streaming (Pub/Sub) sources.
    """
    
    def __init__(self, pipeline_mode='batch'):
        self.pipeline_mode = pipeline_mode
    
    def process(self, element):
        # Import modules inside process method for Dataflow worker compatibility
        import apache_beam as beam
        from datetime import datetime
        import json
        import uuid
        
        try:
            # Handle different input formats based on mode
            if self.pipeline_mode == 'streaming':
                # Pub/Sub messages might be bytes
                if isinstance(element, bytes):
                    message_data = element.decode('utf-8')
                else:
                    message_data = element
                record = json.loads(message_data)
            else:
                # Batch: JSON lines from files
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
                        'pipeline_name': f'{self.pipeline_mode}_taxi_processor'
                    })
                    return
            
            # Ensure trip_id exists
            if 'trip_id' not in record:
                prefix = 'stream' if self.pipeline_mode == 'streaming' else 'batch'
                record['trip_id'] = f"{prefix}_{uuid.uuid4()}"
            
            # Parse and convert timestamps to BigQuery format
            try:
                pickup_dt = datetime.fromisoformat(record['pickup_datetime'].replace('Z', '+00:00'))
                dropoff_dt = datetime.fromisoformat(record['dropoff_datetime'].replace('Z', '+00:00'))
                
                # Convert to BigQuery TIMESTAMP canonical format (YYYY-MM-DD HH:MM:SS UTC)
                record['pickup_datetime'] = pickup_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                record['dropoff_datetime'] = dropoff_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                
                # Ensure pickup is before dropoff
                if pickup_dt >= dropoff_dt:
                    yield beam.pvalue.TaggedOutput('invalid', {
                        'error': 'Pickup time must be before dropoff time',
                        'record': str(record),
                        'error_type': 'invalid_timestamp_order',
                        'pipeline_name': f'{self.pipeline_mode}_taxi_processor'
                    })
                    return
                    
            except (ValueError, KeyError) as e:
                yield beam.pvalue.TaggedOutput('invalid', {
                    'error': f'Invalid timestamp format: {str(e)}',
                    'record': str(record),
                    'error_type': 'invalid_timestamp_format',
                    'pipeline_name': f'{self.pipeline_mode}_taxi_processor'
                })
                return
            
            # Convert numeric fields to appropriate precision for BigQuery NUMERIC type
            # BigQuery NUMERIC supports up to 9 decimal places
            numeric_fields = [
                'trip_distance', 'pickup_longitude', 'pickup_latitude', 
                'dropoff_longitude', 'dropoff_latitude', 'fare_amount', 
                'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount'
            ]
            
            for field in numeric_fields:
                if field in record and record[field] is not None:
                    # Round to 9 decimal places for BigQuery NUMERIC compatibility
                    if field in ['pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude']:
                        # Coordinates: round to 9 decimal places (sufficient for GPS precision)
                        record[field] = round(float(record[field]), 9)
                    else:
                        # Money amounts: round to 2 decimal places
                        record[field] = round(float(record[field]), 2)
            
            # Remove event_timestamp field if present (streaming data includes this but BigQuery table doesn't)
            if 'event_timestamp' in record:
                del record['event_timestamp']
            
            # Add processing timestamp in BigQuery format
            record['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            
            # Data validation
            if (record['trip_distance'] > 0 and record['total_amount'] > 0):
                yield record
            else:
                yield beam.pvalue.TaggedOutput('invalid', {
                    'error': 'Invalid trip data (distance or amount <= 0)', 
                    'record': str(record),
                    'error_type': 'validation_failed',
                    'pipeline_name': f'{self.pipeline_mode}_taxi_processor'
                })
                
        except json.JSONDecodeError as e:
            yield beam.pvalue.TaggedOutput('invalid', {
                'error': f'JSON parsing error: {str(e)}', 
                'record': str(element),
                'error_type': 'json_parsing_failed',
                'pipeline_name': f'{self.pipeline_mode}_taxi_processor'
            })
        except Exception as e:
            yield beam.pvalue.TaggedOutput('invalid', {
                'error': f'Unexpected error: {str(e)}', 
                'record': str(element),
                'error_type': 'unexpected_error',
                'pipeline_name': f'{self.pipeline_mode}_taxi_processor'
            })


class CalculateHourlyStats(beam.DoFn):
    """Calculate hourly aggregations from trip data.
    Works for both batch and streaming modes.
    """
    
    def __init__(self, pipeline_mode='batch'):
        self.pipeline_mode = pipeline_mode
    
    def process(self, element, window=beam.DoFn.WindowParam):
        # Import modules inside process method for Dataflow worker compatibility
        import apache_beam as beam
        from datetime import datetime
        import logging
        
        try:
            # Parse BigQuery timestamp format (YYYY-MM-DD HH:MM:SS UTC)
            pickup_time_str = element['pickup_datetime'].replace(' UTC', '+00:00')
            pickup_time = datetime.fromisoformat(pickup_time_str)
            
            if self.pipeline_mode == 'streaming':
                # For streaming, use window information
                stat_hour = window.start.to_utc_datetime().replace(minute=0, second=0, microsecond=0)
                window_info = {
                    'window_start': window.start.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'window_end': window.end.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S UTC'),
                }
            else:
                # For batch, use pickup time
                stat_hour = pickup_time.replace(minute=0, second=0, microsecond=0)
                window_info = {}
            
            # Create a key for grouping by hour and location
            key = (stat_hour.isoformat(), element.get('pickup_location_id', 0))
            
            # Emit individual trip data that will be aggregated later
            stats_record = {
                'key': key,
                'stat_hour': stat_hour.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'pickup_location_id': element.get('pickup_location_id', 0),
                'trip_count': 1,
                'fare_amount': float(element.get('fare_amount', 0.0)),
                'trip_distance': float(element.get('trip_distance', 0.0)),
                'trip_duration_minutes': self._calculate_duration(element),
                'total_revenue': float(element.get('total_amount', 0.0)),
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            }
            
            # Add window info for streaming
            stats_record.update(window_info)
            
            yield stats_record
            
        except Exception as e:
            logging.error(f"Error calculating hourly stats: {e}")
            # Skip invalid records for stats
            pass
    
    def _calculate_duration(self, trip):
        """Calculate trip duration in minutes."""
        from datetime import datetime
        
        try:
            # Parse BigQuery timestamp format (YYYY-MM-DD HH:MM:SS UTC)
            pickup_str = trip['pickup_datetime'].replace(' UTC', '+00:00')
            dropoff_str = trip['dropoff_datetime'].replace(' UTC', '+00:00')
            pickup = datetime.fromisoformat(pickup_str)
            dropoff = datetime.fromisoformat(dropoff_str)
            return float((dropoff - pickup).total_seconds() / 60)
        except:
            return 0.0


class AggregateHourlyStats(beam.DoFn):
    """Aggregate hourly stats by location and hour.
    Works for both batch and streaming modes.
    """
    
    def process(self, element):
        from datetime import datetime
        
        key, stats_iterable = element
        
        # Convert iterable to list
        stats_list = list(stats_iterable)
        
        if not stats_list:
            return
            
        # Calculate aggregated values
        trip_count = len(stats_list)
        total_fare = sum(s['fare_amount'] for s in stats_list)
        total_distance = sum(s['trip_distance'] for s in stats_list)
        total_duration = sum(s['trip_duration_minutes'] for s in stats_list)
        total_revenue = sum(s['total_revenue'] for s in stats_list)
        
        # Get common fields from first record
        first_stat = stats_list[0]
        
        aggregated_record = {
            'stat_hour': first_stat['stat_hour'],
            'pickup_location_id': first_stat['pickup_location_id'],
            'trip_count': trip_count,
            'avg_fare_amount': round(total_fare / trip_count, 2),
            'avg_trip_distance': round(total_distance / trip_count, 2),
            'avg_trip_duration_minutes': round(total_duration / trip_count, 2),
            'total_revenue': round(total_revenue, 2),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        }
        
        # Add window info if available (streaming mode)
        if 'window_start' in first_stat:
            aggregated_record.update({
                'window_start': first_stat['window_start'],
                'window_end': first_stat['window_end']
            })
        
        yield aggregated_record


def run_batch_processor(known_args):
    """Run batch processing pipeline."""
    
    # Set up pipeline options
    pipeline_options = PipelineOptions()
    
    # Get parameters (support both direct args and RuntimeValueProvider)
    input_files = getattr(known_args, 'input_files', None)
    project_id = getattr(known_args, 'project_id', None)
    dataset_id = getattr(known_args, 'dataset_id', None)
    table_name = getattr(known_args, 'table_name', 'taxi_trips')
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read JSON files from GCS
        raw_data = (
            pipeline
            | 'Read from GCS' >> ReadFromText(input_files)
        )
        
        # Parse and validate records
        processed = (
            raw_data
            | 'Parse Records' >> beam.ParDo(ParseTaxiRecord(pipeline_mode='batch')).with_outputs(
                'invalid', main='valid'
            )
        )
        
        # Write valid records to main table
        (
            processed.valid
            | 'Write to Main Table' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.{table_name}',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        
        # Calculate and write hourly aggregations
        hourly_stats = (
            processed.valid
            | 'Calculate Hourly Stats' >> beam.ParDo(CalculateHourlyStats(pipeline_mode='batch'))
            | 'Key by Hour and Location' >> beam.Map(lambda x: (x['key'], x))
            | 'Group by Key' >> beam.GroupByKey()
            | 'Aggregate Stats' >> beam.ParDo(AggregateHourlyStats())
        )
        
        (
            hourly_stats
            | 'Write Hourly Stats' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.hourly_trip_stats',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        
        # Log invalid records for monitoring
        (
            processed.invalid
            | 'Log Invalid Records' >> beam.Map(
                lambda x: logging.error(f"Invalid record: {x}")
            )
        )


def run_streaming_processor(known_args):
    """Run streaming processing pipeline."""
    
    # Set up pipeline options for streaming
    pipeline_options = PipelineOptions(streaming=True)
    
    # Get parameters
    subscription_name = getattr(known_args, 'subscription_name', None)
    project_id = getattr(known_args, 'project_id', None)
    dataset_id = getattr(known_args, 'dataset_id', None)
    table_name = getattr(known_args, 'table_name', 'taxi_trips')
    window_size = getattr(known_args, 'window_size', 60)
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read from Pub/Sub subscription
        messages = (
            pipeline
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=subscription_name)
        )
        
        # Parse and validate messages
        processed = (
            messages
            | 'Parse Messages' >> beam.ParDo(ParseTaxiRecord(pipeline_mode='streaming')).with_outputs(
                'invalid', main='valid'
            )
        )
        
        # Write valid records to main table
        (
            processed.valid
            | 'Write to Main Table' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.{table_name}',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        
        # Calculate windowed aggregations
        windowed_stats = (
            processed.valid
            | 'Apply Fixed Windows' >> beam.WindowInto(beam.window.FixedWindows(window_size))
            | 'Calculate Windowed Stats' >> beam.ParDo(CalculateHourlyStats(pipeline_mode='streaming'))
            | 'Key by Window and Location' >> beam.Map(lambda x: (x['key'], x))
            | 'Group by Key' >> beam.GroupByKey()
            | 'Aggregate Windowed Stats' >> beam.ParDo(AggregateHourlyStats())
        )
        
        # Write windowed stats to table
        (
            windowed_stats
            | 'Write Windowed Stats' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.windowed_trip_stats',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        # Log invalid records for monitoring
        (
            processed.invalid
            | 'Log Invalid Records' >> beam.Map(
                lambda x: logging.error(f"Invalid record: {x}")
            )
        )


def run(argv=None):
    """Main entry point for the unified Flex Template."""
    
    parser = argparse.ArgumentParser(description='Unified Taxi Data Processing Template')
    
    # Common parameters
    parser.add_argument('--mode', choices=['batch', 'streaming'], required=True,
                       help='Processing mode: batch or streaming')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--dataset_id', required=True, help='BigQuery dataset ID')
    parser.add_argument('--table_name', default='taxi_trips', help='Target table name')
    
    # Batch-specific parameters
    parser.add_argument('--input_files', help='GCS path pattern for input files (batch mode)')
    parser.add_argument('--batch_size', type=int, default=1000, help='Batch size (batch mode)')
    
    # Streaming-specific parameters  
    parser.add_argument('--subscription_name', help='Pub/Sub subscription path (streaming mode)')
    parser.add_argument('--window_size', type=int, default=60, help='Window size in seconds (streaming mode)')
    
    # Future extensibility parameters
    parser.add_argument('--data_source', default='taxi', help='Data source type (taxi, uber, lyft, etc.)')
    parser.add_argument('--customer_id', help='Customer identifier for multi-tenant processing')
    parser.add_argument('--job_config', help='JSON configuration for custom job parameters')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Validate mode-specific parameters
    if known_args.mode == 'batch':
        if not known_args.input_files:
            raise ValueError("--input_files is required for batch mode")
        logging.info(f"Running in BATCH mode with input: {known_args.input_files}")
        run_batch_processor(known_args)
        
    elif known_args.mode == 'streaming':
        if not known_args.subscription_name:
            raise ValueError("--subscription_name is required for streaming mode")
        logging.info(f"Running in STREAMING mode with subscription: {known_args.subscription_name}")
        run_streaming_processor(known_args)
    
    else:
        raise ValueError(f"Unsupported mode: {known_args.mode}")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()