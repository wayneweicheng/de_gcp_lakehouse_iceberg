"""
Streaming Dataflow Flex Template for processing real-time NYC taxi data
from Pub/Sub and writing to BigLake Iceberg tables.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
from datetime import datetime
import uuid
import logging
import argparse


class ParseTaxiMessage(beam.DoFn):
    """Parse Pub/Sub message and validate taxi trip data."""
    
    def process(self, element):
        try:
            # Parse JSON message
            if isinstance(element, bytes):
                message_data = element.decode('utf-8')
            else:
                message_data = element
            
            record = json.loads(message_data)
            
            # Validate required fields
            required_fields = ['pickup_datetime', 'dropoff_datetime', 'trip_distance', 'total_amount']
            for field in required_fields:
                if field not in record or record[field] is None:
                    yield beam.pvalue.TaggedOutput('invalid', {
                        'error': f'Missing required field: {field}', 
                        'record': record,
                        'error_type': 'missing_field',
                        'pipeline_name': 'streaming_taxi_processor'
                    })
                    return
            
            # Ensure trip_id exists
            if 'trip_id' not in record:
                record['trip_id'] = f"stream_{uuid.uuid4()}"
            
            # Add processing timestamp
            record['created_at'] = datetime.now().isoformat()
            
            # Data validation
            if (record['trip_distance'] > 0 and record['total_amount'] > 0):
                yield record
            else:
                yield beam.pvalue.TaggedOutput('invalid', {
                    'error': 'Invalid trip data (distance or amount <= 0)', 
                    'record': record,
                    'error_type': 'validation_failed',
                    'pipeline_name': 'streaming_taxi_processor'
                })
                
        except json.JSONDecodeError as e:
            yield beam.pvalue.TaggedOutput('invalid', {
                'error': f'JSON parsing error: {str(e)}', 
                'record': element,
                'error_type': 'json_parsing_failed',
                'pipeline_name': 'streaming_taxi_processor'
            })
        except Exception as e:
            yield beam.pvalue.TaggedOutput('invalid', {
                'error': f'Unexpected error: {str(e)}', 
                'record': element,
                'error_type': 'unexpected_error',
                'pipeline_name': 'streaming_taxi_processor'
            })


class CalculateWindowedStats(beam.DoFn):
    """Calculate windowed aggregations from trip data."""
    
    def process(self, element, window=beam.DoFn.WindowParam):
        try:
            pickup_time = datetime.fromisoformat(element['pickup_datetime'].replace('Z', '+00:00'))
            
            yield {
                'window_start': window.start.to_utc_datetime().isoformat(),
                'window_end': window.end.to_utc_datetime().isoformat(),
                'pickup_location_id': element.get('pickup_location_id', 0),
                'trip_count': 1,
                'total_fare': element.get('fare_amount', 0.0),
                'total_distance': element.get('trip_distance', 0.0),
                'trip_duration_minutes': self._calculate_duration(element),
                'total_revenue': element.get('total_amount', 0.0),
                'created_at': datetime.now().isoformat()
            }
        except Exception as e:
            logging.error(f"Error calculating windowed stats: {e}")
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
    parser.add_argument('--subscription_name', required=True, help='Pub/Sub subscription path')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--dataset_id', required=True, help='BigQuery dataset ID')
    parser.add_argument('--table_name', default='taxi_trips', help='Target table name')
    parser.add_argument('--window_size', type=int, default=60, help='Window size in seconds')
    
    known_args, pipeline_args = parser.parse_known_args()
    
    # Set up pipeline options for streaming
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Use RuntimeValueProvider for template parameters
    subscription_name = (RuntimeValueProvider.get_value('subscription_name', str, known_args.subscription_name)
                        if hasattr(RuntimeValueProvider, 'get_value')
                        else known_args.subscription_name)
    
    project_id = (RuntimeValueProvider.get_value('project_id', str, known_args.project_id)
                  if hasattr(RuntimeValueProvider, 'get_value')
                  else known_args.project_id)
    
    dataset_id = (RuntimeValueProvider.get_value('dataset_id', str, known_args.dataset_id)
                  if hasattr(RuntimeValueProvider, 'get_value')
                  else known_args.dataset_id)
    
    table_name = (RuntimeValueProvider.get_value('table_name', str, known_args.table_name)
                  if hasattr(RuntimeValueProvider, 'get_value')
                  else known_args.table_name)
    
    window_size = (RuntimeValueProvider.get_value('window_size', int, known_args.window_size)
                   if hasattr(RuntimeValueProvider, 'get_value')
                   else known_args.window_size)
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read from Pub/Sub subscription
        messages = (
            pipeline
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=subscription_name)
        )
        
        # Parse and validate messages
        processed = (
            messages
            | 'Parse Messages' >> beam.ParDo(ParseTaxiMessage()).with_outputs(
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
        
        # Calculate windowed stats
        windowed_stats = (
            processed.valid
            | 'Apply Fixed Windows' >> beam.WindowInto(beam.window.FixedWindows(window_size))
            | 'Calculate Windowed Stats' >> beam.ParDo(CalculateWindowedStats())
            | 'Group by Window and Location' >> beam.GroupBy('window_start', 'pickup_location_id')
            | 'Aggregate Windowed Stats' >> beam.Map(lambda group: {
                'window_start': group.key[0],
                'pickup_location_id': group.key[1],
                'trip_count': sum(item['trip_count'] for item in group.value),
                'avg_fare_amount': sum(item['total_fare'] for item in group.value) / len(group.value) if group.value else 0,
                'avg_trip_distance': sum(item['total_distance'] for item in group.value) / len(group.value) if group.value else 0,
                'avg_trip_duration_minutes': sum(item['trip_duration_minutes'] for item in group.value) / len(group.value) if group.value else 0,
                'total_revenue': sum(item['total_revenue'] for item in group.value),
                'created_at': datetime.now().isoformat()
            })
        )
        
        # Write windowed stats to Iceberg table
        (
            windowed_stats
            | 'Write Windowed Stats' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.windowed_trip_stats',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        # Write invalid records to error table for monitoring
        (
            processed.invalid
            | 'Write Errors' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.processing_errors',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                schema='error:STRING,record:STRING,error_type:STRING,pipeline_name:STRING,created_at:TIMESTAMP'
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run() 