"""
Taxi trip simulator for generating realistic NYC taxi trip data
and publishing to Pub/Sub for real-time processing.
"""

import json
import random
import time
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
import uuid
import logging
import os


class TaxiTripSimulator:
    """Simulates realistic NYC taxi trip completion events."""
    
    def __init__(self, project_id, topic_name):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        
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
        self.payment_weights = [0.7, 0.25, 0.03, 0.02]  # Realistic distribution
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def generate_trip_event(self):
        """Generate realistic taxi trip completion event."""
        
        pickup_location = random.choice(self.pickup_locations)
        dropoff_location = random.choice(self.pickup_locations)
        
        # Generate realistic trip timing
        pickup_time = datetime.now() - timedelta(minutes=random.randint(5, 60))
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
        
        trip_event = {
            'trip_id': f"live_{uuid.uuid4()}",
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
            'dropoff_location_id': dropoff_location['id'],
            'event_timestamp': datetime.now().isoformat()
        }
        
        # Calculate total amount
        trip_event['total_amount'] = round(
            trip_event['fare_amount'] + trip_event['extra'] + 
            trip_event['mta_tax'] + trip_event['tip_amount'] + 
            trip_event['tolls_amount'], 2
        )
        
        return trip_event
    
    def publish_trip_event(self, trip_data):
        """Publish trip event to Pub/Sub topic."""
        message_data = json.dumps(trip_data).encode('utf-8')
        
        # Add message attributes for routing/filtering
        attributes = {
            'vendor_id': str(trip_data['vendor_id']),
            'pickup_location_id': str(trip_data['pickup_location_id']),
            'payment_type': trip_data['payment_type'],
            'event_type': 'trip_completion'
        }
        
        future = self.publisher.publish(
            self.topic_path, 
            message_data, 
            **attributes
        )
        
        return future.result()
    
    def simulate_continuous_stream(self, trips_per_minute=10, duration_hours=24):
        """Simulate continuous stream of taxi trip completions."""
        
        total_trips = 0
        end_time = datetime.now() + timedelta(hours=duration_hours)
        
        self.logger.info(f"Starting taxi trip simulation...")
        self.logger.info(f"Target: {trips_per_minute} trips/minute for {duration_hours} hours")
        self.logger.info(f"Publishing to topic: {self.topic_path}")
        
        while datetime.now() < end_time:
            batch_start = time.time()
            
            # Generate batch of trips
            for _ in range(trips_per_minute):
                try:
                    trip_event = self.generate_trip_event()
                    message_id = self.publish_trip_event(trip_event)
                    total_trips += 1
                    
                    if total_trips % 100 == 0:
                        self.logger.info(f"Published {total_trips} trips. Latest: {trip_event['trip_id']}")
                        
                except Exception as e:
                    self.logger.error(f"Error publishing trip: {e}")
            
            # Wait to maintain rate
            elapsed = time.time() - batch_start
            sleep_time = max(0, 60 - elapsed)  # Target 1 minute per batch
            time.sleep(sleep_time)
        
        self.logger.info(f"Simulation completed. Total trips published: {total_trips}")
        return total_trips
    
    def simulate_batch(self, trip_count=100):
        """Generate a batch of trip events."""
        
        self.logger.info(f"Generating batch of {trip_count} trips...")
        
        for i in range(trip_count):
            try:
                trip_event = self.generate_trip_event()
                message_id = self.publish_trip_event(trip_event)
                
                if (i + 1) % 10 == 0:
                    self.logger.info(f"Published {i + 1}/{trip_count} trips")
                    
            except Exception as e:
                self.logger.error(f"Error publishing trip {i + 1}: {e}")
        
        self.logger.info(f"Batch simulation completed. Published {trip_count} trips")
        return trip_count
    
    def generate_historical_data(self, start_date, end_date, trips_per_day=1000):
        """Generate historical trip data for a date range."""
        
        current_date = start_date
        total_trips = 0
        
        self.logger.info(f"Generating historical data from {start_date} to {end_date}")
        
        while current_date <= end_date:
            daily_trips = 0
            
            # Generate trips throughout the day
            for hour in range(24):
                # Vary trip volume by hour (more during rush hours)
                if hour in [7, 8, 9, 17, 18, 19]:  # Rush hours
                    hourly_trips = int(trips_per_day * 0.08)  # 8% of daily trips
                elif hour in [10, 11, 12, 13, 14, 15, 16, 20, 21]:  # Business hours
                    hourly_trips = int(trips_per_day * 0.05)  # 5% of daily trips
                else:  # Off-peak hours
                    hourly_trips = int(trips_per_day * 0.02)  # 2% of daily trips
                
                for _ in range(hourly_trips):
                    # Generate trip with historical timestamp
                    trip_event = self.generate_trip_event()
                    
                    # Adjust timestamps to historical date
                    base_time = current_date.replace(
                        hour=hour, 
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59)
                    )
                    
                    trip_duration = random.randint(5, 120)
                    trip_event['pickup_datetime'] = base_time.isoformat()
                    trip_event['dropoff_datetime'] = (base_time + timedelta(minutes=trip_duration)).isoformat()
                    trip_event['trip_id'] = f"hist_{current_date.strftime('%Y%m%d')}_{uuid.uuid4()}"
                    
                    try:
                        self.publish_trip_event(trip_event)
                        daily_trips += 1
                        total_trips += 1
                    except Exception as e:
                        self.logger.error(f"Error publishing historical trip: {e}")
            
            self.logger.info(f"Generated {daily_trips} trips for {current_date.strftime('%Y-%m-%d')}")
            current_date += timedelta(days=1)
        
        self.logger.info(f"Historical data generation completed. Total trips: {total_trips}")
        return total_trips


def main():
    """Main function for running the simulator."""
    import argparse
    
    parser = argparse.ArgumentParser(description='NYC Taxi Trip Simulator')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--topic_name', default='taxi-trips-stream', help='Pub/Sub topic name')
    parser.add_argument('--mode', choices=['continuous', 'batch', 'historical'], 
                       default='batch', help='Simulation mode')
    parser.add_argument('--trips_per_minute', type=int, default=10, 
                       help='Trips per minute for continuous mode')
    parser.add_argument('--duration_hours', type=int, default=1, 
                       help='Duration in hours for continuous mode')
    parser.add_argument('--trip_count', type=int, default=100, 
                       help='Number of trips for batch mode')
    parser.add_argument('--start_date', help='Start date for historical mode (YYYY-MM-DD)')
    parser.add_argument('--end_date', help='End date for historical mode (YYYY-MM-DD)')
    parser.add_argument('--trips_per_day', type=int, default=1000, 
                       help='Trips per day for historical mode')
    
    args = parser.parse_args()
    
    simulator = TaxiTripSimulator(args.project_id, args.topic_name)
    
    if args.mode == 'continuous':
        simulator.simulate_continuous_stream(
            trips_per_minute=args.trips_per_minute,
            duration_hours=args.duration_hours
        )
    elif args.mode == 'batch':
        simulator.simulate_batch(trip_count=args.trip_count)
    elif args.mode == 'historical':
        if not args.start_date or not args.end_date:
            parser.error("Historical mode requires --start_date and --end_date")
        
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        
        simulator.generate_historical_data(
            start_date=start_date,
            end_date=end_date,
            trips_per_day=args.trips_per_day
        )


if __name__ == '__main__':
    main() 