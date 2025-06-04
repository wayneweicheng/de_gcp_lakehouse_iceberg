"""
Streaming Dataflow pipeline for processing real-time NYC taxi data
from Pub/Sub and writing to BigLake Iceberg tables.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
from datetime import datetime
import uuid
import logging


class ProcessLiveTaxiTrip(beam.DoFn):
    """Process real-time taxi trip completion events."""
    
    def process(self, element):
        try:
            # Parse JSON message from Pub/Sub
            trip_data = json.loads(element.decode('utf-8'))
            
            # Enrich with processing metadata
            record = {
                'trip_id': trip_data.get('trip_id', str(uuid.uuid4())),
                'vendor_id': trip_data.get('vendor_id', 1),
                'pickup_datetime': trip_data['pickup_datetime'],
                'dropoff_datetime': trip_data['dropoff_datetime'],
                'passenger_count': trip_data.get('passenger_count', 1),
                'trip_distance': float(trip_data.get('trip_distance', 0)),
                'pickup_longitude': float(trip_data.get('pickup_longitude', 0)),
                'pickup_latitude': float(trip_data.get('pickup_latitude', 0)),
                'dropoff_longitude': float(trip_data.get('dropoff_longitude', 0)),
                'dropoff_latitude': float(trip_data.get('dropoff_latitude', 0)),
                'payment_type': trip_data.get('payment_type', 'card'),
                'fare_amount': float(trip_data.get('fare_amount', 0)),
                'extra': float(trip_data.get('extra', 0)),
                'mta_tax': float(trip_data.get('mta_tax', 0.5)),
                'tip_amount': float(trip_data.get('tip_amount', 0)),
                'tolls_amount': float(trip_data.get('tolls_amount', 0)),
                'total_amount': float(trip_data.get('total_amount', 0)),
                'pickup_location_id': trip_data.get('pickup_location_id'),
                'dropoff_location_id': trip_data.get('dropoff_location_id'),
                'created_at': datetime.now().isoformat()
            }
            
            # Validate essential fields
            if (record['pickup_datetime'] and record['dropoff_datetime'] and
                record['total_amount'] > 0):
                yield record
            else:
                yield beam.pvalue.TaggedOutput('dead_letter', {
                    'error': 'Invalid trip data',
                    'error_type': 'validation_failed',
                    'pipeline_name': 'streaming_taxi_processor',
                    'source_data': trip_data
                })
                
        except Exception as e:
            yield beam.pvalue.TaggedOutput('dead_letter', {
                'error': str(e), 
                'error_type': 'parsing_failed',
                'pipeline_name': 'streaming_taxi_processor',
                'message': element.decode('utf-8', errors='ignore')
            })


class CalculateHourlyStats(beam.DoFn):
    """Calculate hourly aggregations for real-time monitoring."""
    
    def process(self, element, window=beam.DoFn.WindowParam):
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


class AggregateHourlyStats(beam.DoFn):
    """Aggregate hourly statistics from individual trip events."""
    
    def process(self, element):
        key, values = element
        stat_hour, pickup_location_id = key
        
        values_list = list(values)
        trip_count = len(values_list)
        
        if trip_count > 0:
            yield {
                'stat_hour': stat_hour,
                'pickup_location_id': pickup_location_id,
                'trip_count': trip_count,
                'avg_fare_amount': sum(item['total_fare'] for item in values_list) / trip_count,
                'avg_trip_distance': sum(item['total_distance'] for item in values_list) / trip_count,
                'avg_trip_duration_minutes': sum(item['trip_duration_minutes'] for item in values_list) / trip_count,
                'total_revenue': sum(item['total_revenue'] for item in values_list),
                'created_at': datetime.now().isoformat()
            }


def run_streaming_pipeline(project_id, subscription_name, dataset_id, temp_location, staging_location):
    """Process real-time taxi trip events."""
    
    pipeline_options = PipelineOptions([
        '--runner=DataflowRunner',
        f'--project={project_id}',
        '--region=us-central1',
        '--streaming=true',
        f'--temp_location={temp_location}',
        f'--staging_location={staging_location}',
        '--max_num_workers=10',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--enable_streaming_engine=true',
        '--use_public_ips=false'
    ])
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read from Pub/Sub subscription
        messages = (
            pipeline
            | 'Read from Pub/Sub' >> ReadFromPubSub(
                subscription=f'projects/{project_id}/subscriptions/{subscription_name}'
            )
        )
        
        # Process trip completion events
        processed_trips = (
            messages
            | 'Process Trip Events' >> beam.ParDo(ProcessLiveTaxiTrip()).with_outputs(
                'dead_letter', main='processed'
            )
        )
        
        # Write individual trips to Iceberg table
        (
            processed_trips.processed
            | 'Write Trips to Iceberg' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.taxi_trips',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        
        # Calculate and write hourly stats
        hourly_stats = (
            processed_trips.processed
            | 'Add Timestamp' >> beam.Map(lambda x: beam.window.TimestampedValue(
                x, 
                datetime.fromisoformat(x['pickup_datetime'].replace('Z', '+00:00')).timestamp()
            ))
            | 'Window into Hours' >> beam.WindowInto(beam.window.FixedWindows(3600))  # 1 hour
            | 'Calculate Stats' >> beam.ParDo(CalculateHourlyStats())
            | 'Key by Location and Hour' >> beam.Map(lambda x: (
                (x['stat_hour'], x['pickup_location_id']), x
            ))
            | 'Group by Key' >> beam.GroupByKey()
            | 'Aggregate Stats' >> beam.ParDo(AggregateHourlyStats())
        )
        
        (
            hourly_stats
            | 'Write Stats to Iceberg' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.hourly_trip_stats',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        
        # Handle dead letter queue
        (
            processed_trips.dead_letter
            | 'Format Dead Letter Records' >> beam.Map(lambda error: {
                'error_type': error.get('error_type', 'unknown'),
                'error_message': error.get('error', 'Unknown error'),
                'source_data': str(error.get('source_data', error.get('message', ''))),
                'pipeline_name': error.get('pipeline_name', 'streaming_taxi_processor'),
                'retry_count': 0
            })
            | 'Write Dead Letters' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.processing_errors',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Streaming taxi data processor')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--subscription_name', required=True, help='Pub/Sub subscription name')
    parser.add_argument('--dataset_id', default='taxi_dataset', help='BigQuery dataset ID')
    parser.add_argument('--temp_location', required=True, help='Temp GCS location')
    parser.add_argument('--staging_location', required=True, help='Staging GCS location')
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    run_streaming_pipeline(
        project_id=args.project_id,
        subscription_name=args.subscription_name,
        dataset_id=args.dataset_id,
        temp_location=args.temp_location,
        staging_location=args.staging_location
    ) 