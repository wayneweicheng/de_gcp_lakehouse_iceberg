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
from apache_beam.transforms.window import SlidingWindows, FixedWindows
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


class CalculateLocationFeatures(beam.DoFn):
    """Calculate location-based features using sliding windows for demand forecasting.
    Groups by pickup_location_id (NYC Taxi Zone) and calculates velocity and patterns.
    """
    
    def __init__(self, pipeline_mode='streaming'):
        self.pipeline_mode = pipeline_mode
    
    def process(self, element, window=beam.DoFn.WindowParam):
        import apache_beam as beam
        from datetime import datetime
        import logging
        
        try:
            # Extract pickup_location_id (NYC Taxi Zone)
            pickup_location_id = element.get('pickup_location_id', 0)
            
            # Parse timestamp
            pickup_time_str = element['pickup_datetime'].replace(' UTC', '+00:00')
            pickup_time = datetime.fromisoformat(pickup_time_str)
            
            # Get window information for sliding window features
            window_start = window.start.to_utc_datetime()
            window_end = window.end.to_utc_datetime()
            
            # Calculate location features for this trip
            location_features = {
                'pickup_location_id': pickup_location_id,
                'trip_id': element.get('trip_id'),
                'window_start': window_start.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'window_end': window_end.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'transaction_timestamp': element['pickup_datetime'],
                
                # Basic trip features
                'trip_distance': float(element.get('trip_distance', 0.0)),
                'fare_amount': float(element.get('fare_amount', 0.0)),
                'total_amount': float(element.get('total_amount', 0.0)),
                'tip_amount': float(element.get('tip_amount', 0.0)),
                
                # Location features
                'pickup_longitude': element.get('pickup_longitude'),
                'pickup_latitude': element.get('pickup_latitude'),
                'dropoff_longitude': element.get('dropoff_longitude'),
                'dropoff_latitude': element.get('dropoff_latitude'),
                'dropoff_location_id': element.get('dropoff_location_id', 0),
                
                # Temporal features
                'hour_of_day': pickup_time.hour,
                'day_of_week': pickup_time.weekday(),
                'is_weekend': pickup_time.weekday() >= 5,
                'is_rush_hour': pickup_time.hour in [7, 8, 9, 17, 18, 19],  # Morning & evening rush
                
                # Trip duration calculation
                'trip_duration_minutes': self._calculate_duration(element),
                
                # Features that will be aggregated across the sliding window
                'trip_count': 1,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            }
            
            yield location_features
            
        except Exception as e:
            logging.error(f"Error calculating location features: {e}")
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


class AggregateLocationFeatures(beam.DoFn):
    """Aggregate location features across sliding windows for demand patterns.
    Creates velocity features, pricing patterns, destination diversity, etc.
    """
    
    def process(self, element):
        from datetime import datetime
        import statistics
        import math
        
        pickup_location_id, trips_iterable = element
        trips = list(trips_iterable)
        
        if not trips:
            return
            
        # Sort trips by timestamp for time-based features
        trips.sort(key=lambda x: x['transaction_timestamp'])
        
        # Basic aggregations
        trip_count = len(trips)
        total_amount_sum = sum(t['total_amount'] for t in trips)
        total_distance_sum = sum(t['trip_distance'] for t in trips)
        total_tip_sum = sum(t['tip_amount'] for t in trips)
        
        # Calculate demand velocity and patterns
        amounts = [t['total_amount'] for t in trips]
        distances = [t['trip_distance'] for t in trips]
        durations = [t['trip_duration_minutes'] for t in trips if t['trip_duration_minutes'] > 0]
        tip_amounts = [t['tip_amount'] for t in trips]
        
        # Time-based features
        first_trip = trips[0]
        last_trip = trips[-1]
        
        window_start = first_trip['window_start']
        window_end = first_trip['window_end']
        
        # Destination diversity features
        unique_dropoff_locations = len(set(t['dropoff_location_id'] for t in trips))
        
        # Temporal patterns
        hours_of_day = [t['hour_of_day'] for t in trips]
        unique_hours = len(set(hours_of_day))
        weekend_trip_count = sum(1 for t in trips if t['is_weekend'])
        rush_hour_trip_count = sum(1 for t in trips if t['is_rush_hour'])
        
        # Statistical features for revenue
        avg_amount = statistics.mean(amounts) if amounts else 0.0
        std_amount = statistics.stdev(amounts) if len(amounts) > 1 else 0.0
        max_amount = max(amounts) if amounts else 0.0
        min_amount = min(amounts) if amounts else 0.0
        
        # Statistical features for distance
        avg_distance = statistics.mean(distances) if distances else 0.0
        std_distance = statistics.stdev(distances) if len(distances) > 1 else 0.0
        max_distance = max(distances) if distances else 0.0
        
        # Duration statistics
        avg_duration = statistics.mean(durations) if durations else 0.0
        std_duration = statistics.stdev(durations) if len(durations) > 1 else 0.0
        
        # Tip statistics  
        avg_tip = statistics.mean(tip_amounts) if tip_amounts else 0.0
        tip_percentage = (total_tip_sum / total_amount_sum * 100) if total_amount_sum > 0 else 0.0
        
        # Calculate demand intensity (trips per minute in this window)
        window_duration_minutes = 30  # 30-minute sliding window
        demand_intensity = trip_count / window_duration_minutes
        
        # Calculate average inter-arrival time between trips
        if len(trips) > 1:
            # Parse timestamps to calculate time differences
            timestamps = []
            for trip in trips:
                ts_str = trip['transaction_timestamp'].replace(' UTC', '+00:00')
                timestamps.append(datetime.fromisoformat(ts_str))
            
            time_diffs = [(timestamps[i+1] - timestamps[i]).total_seconds() / 60 
                         for i in range(len(timestamps)-1)]
            avg_inter_arrival_minutes = statistics.mean(time_diffs) if time_diffs else 0.0
        else:
            avg_inter_arrival_minutes = 0.0
        
        # Create aggregated feature record
        aggregated_features = {
            'pickup_location_id': pickup_location_id,
            'window_start': window_start,
            'window_end': window_end,
            
            # Volume and velocity features
            'trip_count': trip_count,
            'demand_intensity_per_minute': round(demand_intensity, 4),
            'avg_inter_arrival_minutes': round(avg_inter_arrival_minutes, 2),
            
            # Revenue features
            'total_revenue': round(total_amount_sum, 2),
            'avg_trip_amount': round(avg_amount, 2),
            'std_trip_amount': round(std_amount, 2),
            'max_trip_amount': round(max_amount, 2),
            'min_trip_amount': round(min_amount, 2),
            'revenue_coefficient_variation': round(std_amount / avg_amount, 4) if avg_amount > 0 else 0.0,
            
            # Distance features
            'total_distance': round(total_distance_sum, 2),
            'avg_trip_distance': round(avg_distance, 2),
            'std_trip_distance': round(std_distance, 2),
            'max_trip_distance': round(max_distance, 2),
            
            # Duration features
            'avg_trip_duration_minutes': round(avg_duration, 2),
            'std_trip_duration_minutes': round(std_duration, 2),
            
            # Tip and service features
            'total_tips': round(total_tip_sum, 2),
            'avg_tip_amount': round(avg_tip, 2),
            'tip_percentage': round(tip_percentage, 2),
            
            # Destination diversity
            'unique_dropoff_locations': unique_dropoff_locations,
            'destination_diversity_ratio': round(unique_dropoff_locations / trip_count, 4),
            
            # Temporal features
            'unique_hours_of_day': unique_hours,
            'weekend_trip_count': weekend_trip_count,
            'weekend_trip_ratio': round(weekend_trip_count / trip_count, 4),
            'rush_hour_trip_count': rush_hour_trip_count,
            'rush_hour_trip_ratio': round(rush_hour_trip_count / trip_count, 4),
            
            # Performance indicators
            'high_demand_flag': trip_count > 10,  # More than 10 trips in 30 minutes
            'high_revenue_flag': total_amount_sum > avg_amount * trip_count + 2 * std_amount if std_amount > 0 else False,
            'long_distance_flag': max_distance > 20.0,  # Trips longer than 20 miles
            'premium_location_flag': avg_amount > 25.0,  # Higher than average fare zone
            
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        }
        
        yield aggregated_features


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
    """Run streaming processing pipeline with enhanced sliding window features."""
    
    # Set up pipeline options for streaming
    pipeline_options = PipelineOptions(streaming=True)
    
    # Get parameters
    subscription_name = getattr(known_args, 'subscription_name', None)
    project_id = getattr(known_args, 'project_id', None)
    dataset_id = getattr(known_args, 'dataset_id', None)
    table_name = getattr(known_args, 'table_name', 'taxi_trips')
    sliding_window_size = getattr(known_args, 'sliding_window_size', 1800)  # 30 minutes
    sliding_window_period = getattr(known_args, 'sliding_window_period', 30)  # 30 seconds
    
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
        
        # Location-based feature engineering with sliding windows
        location_features = (
            processed.valid
            | 'Apply Sliding Windows' >> beam.WindowInto(
                SlidingWindows(
                    size=sliding_window_size,  # 30-minute windows (default)
                    period=sliding_window_period  # Slide every 30 seconds (default)
                )
            )
            | 'Calculate Location Features' >> beam.ParDo(CalculateLocationFeatures(pipeline_mode='streaming'))
            | 'Key by Location' >> beam.Map(lambda x: (x['pickup_location_id'], x))
            | 'Group by Location' >> beam.GroupByKey()
            | 'Aggregate Location Features' >> beam.ParDo(AggregateLocationFeatures())
        )
        
        # Write location features to BigQuery
        (
            location_features
            | 'Write Location Features' >> WriteToBigQuery(
                table=f'{project_id}.{dataset_id}.location_features_sliding_window',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        
        # Traditional hourly stats with fixed windows (keeping for backward compatibility)
        hourly_stats = (
            processed.valid
            | 'Apply Fixed Windows for Hourly Stats' >> beam.WindowInto(
                FixedWindows(3600)  # 1-hour fixed windows
            )
            | 'Calculate Hourly Stats' >> beam.ParDo(CalculateHourlyStats(pipeline_mode='streaming'))
            | 'Key by Hour and Location' >> beam.Map(lambda x: (x['key'], x))
            | 'Group by Key' >> beam.GroupByKey()
            | 'Aggregate Hourly Stats' >> beam.ParDo(AggregateHourlyStats())
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
    parser.add_argument('--sliding_window_size', type=int, default=1800,  # 30 minutes
                        help='Sliding window size in seconds (streaming mode)')
    parser.add_argument('--sliding_window_period', type=int, default=30,  # 30 seconds
                        help='Sliding window period/slide interval in seconds (streaming mode)')
    
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