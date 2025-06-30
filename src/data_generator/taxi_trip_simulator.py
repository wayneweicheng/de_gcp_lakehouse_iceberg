"""
Enhanced Taxi trip simulator for generating realistic NYC taxi trip data
with comprehensive location zones and feature engineering support.
Optimized for sliding window analysis and location-based demand patterns.
"""

import json
import random
import time
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
import uuid
import logging
import os
import math


class TaxiTripSimulator:
    """Enhanced simulator for realistic NYC taxi trip completion events."""
    
    def __init__(self, project_id, topic_name):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        
        # Enhanced NYC taxi zone coordinates (top 50 popular zones)
        self.pickup_locations = [
            # Manhattan Core
            {'id': 161, 'lat': 40.7589, 'lon': -73.9851, 'name': 'Midtown Center', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 237, 'lat': 40.7505, 'lon': -73.9934, 'name': 'Times Square/Theatre District', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 162, 'lat': 40.7614, 'lon': -73.9776, 'name': 'Midtown East', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 239, 'lat': 40.7527, 'lon': -73.9772, 'name': 'Turtle Bay East', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 238, 'lat': 40.7519, 'lon': -73.9836, 'name': 'Turtle Bay North', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 79, 'lat': 40.7505, 'lon': -74.0048, 'name': 'East Village', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 13, 'lat': 40.7282, 'lon': -74.0776, 'name': 'Battery Park City', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            {'id': 87, 'lat': 40.7903, 'lon': -73.9597, 'name': 'Upper East Side South', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 88, 'lat': 40.7784, 'lon': -73.9573, 'name': 'Upper East Side North', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            {'id': 90, 'lat': 40.7074, 'lon': -74.0113, 'name': 'Financial District North', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            
            # Manhattan Upper
            {'id': 249, 'lat': 40.7903, 'lon': -73.9765, 'name': 'Upper West Side South', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 234, 'lat': 40.8007, 'lon': -73.9736, 'name': 'Upper West Side North', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            {'id': 125, 'lat': 40.8176, 'lon': -73.9482, 'name': 'Morningside Heights', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            {'id': 127, 'lat': 40.8259, 'lon': -73.9442, 'name': 'Hamilton Heights', 'borough': 'Manhattan', 'demand_tier': 'low'},
            {'id': 153, 'lat': 40.8361, 'lon': -73.9327, 'name': 'Washington Heights North', 'borough': 'Manhattan', 'demand_tier': 'low'},
            
            # Manhattan Lower
            {'id': 142, 'lat': 40.7222, 'lon': -74.0058, 'name': 'SoHo', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 236, 'lat': 40.7194, 'lon': -74.0094, 'name': 'TriBeCa/Civic Center', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            {'id': 68, 'lat': 40.7260, 'lon': -73.9897, 'name': 'East Village', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 186, 'lat': 40.6958, 'lon': -73.9896, 'name': 'Lower East Side', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            {'id': 211, 'lat': 40.7080, 'lon': -73.9857, 'name': 'Two Bridges/Seward Park', 'borough': 'Manhattan', 'demand_tier': 'low'},
            
            # Brooklyn Popular
            {'id': 61, 'lat': 40.6781, 'lon': -73.9442, 'name': 'Crown Heights North', 'borough': 'Brooklyn', 'demand_tier': 'medium'},
            {'id': 17, 'lat': 40.6895, 'lon': -73.9442, 'name': 'Bedford-Stuyvesant', 'borough': 'Brooklyn', 'demand_tier': 'medium'},
            {'id': 181, 'lat': 40.6572, 'lon': -73.9442, 'name': 'Park Slope', 'borough': 'Brooklyn', 'demand_tier': 'high'},
            {'id': 107, 'lat': 40.6895, 'lon': -73.9648, 'name': 'Fort Greene', 'borough': 'Brooklyn', 'demand_tier': 'medium'},
            {'id': 244, 'lat': 40.6781, 'lon': -73.9648, 'name': 'Williamsburg (North Side)', 'borough': 'Brooklyn', 'demand_tier': 'high'},
            {'id': 245, 'lat': 40.6895, 'lon': -73.9854, 'name': 'Williamsburg (South Side)', 'borough': 'Brooklyn', 'demand_tier': 'high'},
            
            # Queens Popular
            {'id': 129, 'lat': 40.7282, 'lon': -73.8370, 'name': 'Jackson Heights', 'borough': 'Queens', 'demand_tier': 'medium'},
            {'id': 7, 'lat': 40.7505, 'lon': -73.8776, 'name': 'Long Island City/Queensboro Hill', 'borough': 'Queens', 'demand_tier': 'medium'},
            {'id': 74, 'lat': 40.7614, 'lon': -73.9064, 'name': 'East Elmhurst', 'borough': 'Queens', 'demand_tier': 'medium'},
            {'id': 82, 'lat': 40.7505, 'lon': -73.8164, 'name': 'Elmhurst/Maspeth', 'borough': 'Queens', 'demand_tier': 'low'},
            {'id': 116, 'lat': 40.7282, 'lon': -73.7958, 'name': 'Jamaica', 'borough': 'Queens', 'demand_tier': 'medium'},
            
            # Airports
            {'id': 1, 'lat': 40.6895, 'lon': -74.1745, 'name': 'Newark Airport', 'borough': 'EWR', 'demand_tier': 'high'},
            {'id': 132, 'lat': 40.6413, 'lon': -73.7781, 'name': 'JFK Airport', 'borough': 'Queens', 'demand_tier': 'high'},
            {'id': 138, 'lat': 40.7769, 'lon': -73.8740, 'name': 'LaGuardia Airport', 'borough': 'Queens', 'demand_tier': 'high'},
            
            # Bronx
            {'id': 3, 'lat': 40.8656, 'lon': -73.8478, 'name': 'Allerton/Pelham Gardens', 'borough': 'Bronx', 'demand_tier': 'low'},
            {'id': 18, 'lat': 40.8361, 'lon': -73.9124, 'name': 'Bronx Park', 'borough': 'Bronx', 'demand_tier': 'low'},
            {'id': 20, 'lat': 40.8259, 'lon': -73.9327, 'name': 'Bronx Park', 'borough': 'Bronx', 'demand_tier': 'low'},
            {'id': 46, 'lat': 40.8176, 'lon': -73.8958, 'name': 'Central Bronx', 'borough': 'Bronx', 'demand_tier': 'low'},
            
            # Staten Island
            {'id': 5, 'lat': 40.5560, 'lon': -74.1827, 'name': 'Arden Heights', 'borough': 'Staten Island', 'demand_tier': 'low'},
            {'id': 121, 'lat': 40.5797, 'lon': -74.1502, 'name': 'Mariners Harbor', 'borough': 'Staten Island', 'demand_tier': 'low'},
            {'id': 188, 'lat': 40.6059, 'lon': -74.0776, 'name': 'Port Richmond', 'borough': 'Staten Island', 'demand_tier': 'low'},
            {'id': 204, 'lat': 40.5560, 'lon': -74.1239, 'name': 'Stapleton', 'borough': 'Staten Island', 'demand_tier': 'low'},
            
            # Additional High-Demand Manhattan Zones
            {'id': 100, 'lat': 40.7282, 'lon': -73.9897, 'name': 'Flatiron', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 114, 'lat': 40.7360, 'lon': -73.9897, 'name': 'Gramercy', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 45, 'lat': 40.7282, 'lon': -73.9964, 'name': 'Chelsea', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 148, 'lat': 40.7194, 'lon': -73.9964, 'name': 'Meatpacking/West Village West', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 246, 'lat': 40.7360, 'lon': -73.9964, 'name': 'West Chelsea/Hudson Yards', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            
            # Business Districts
            {'id': 170, 'lat': 40.7074, 'lon': -74.0226, 'name': 'Murray Hill-Queens', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            {'id': 229, 'lat': 40.7437, 'lon': -73.9776, 'name': 'Sutton Place/Turtle Bay South', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 113, 'lat': 40.7437, 'lon': -73.9897, 'name': 'Garment District', 'borough': 'Manhattan', 'demand_tier': 'medium'},
            {'id': 164, 'lat': 40.7437, 'lon': -73.9964, 'name': 'Midtown South', 'borough': 'Manhattan', 'demand_tier': 'high'},
            {'id': 41, 'lat': 40.7505, 'lon': -73.9897, 'name': 'Central Park South', 'borough': 'Manhattan', 'demand_tier': 'high'},
        ]
        
        # Demand multipliers by time and location tier
        self.demand_multipliers = {
            'high': {'rush': 2.5, 'business': 1.8, 'evening': 1.5, 'night': 0.8, 'weekend': 1.2},
            'medium': {'rush': 1.5, 'business': 1.2, 'evening': 1.0, 'night': 0.6, 'weekend': 0.9},
            'low': {'rush': 0.8, 'business': 0.6, 'evening': 0.5, 'night': 0.3, 'weekend': 0.7}
        }
        
        # Enhanced payment type distributions
        self.payment_types = ['credit_card', 'cash', 'no_charge', 'dispute', 'unknown']
        self.payment_weights = [0.67, 0.28, 0.02, 0.02, 0.01]  # More realistic NYC distribution
        
        # Trip type distribution (street-hail vs dispatch)
        self.trip_types = ['street_hail', 'dispatch']
        self.trip_type_weights = [0.89, 0.11]  # Most NYC trips are street hails
        
        # Rate code distributions
        self.rate_codes = [1, 2, 3, 4, 5]  # 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated
        self.rate_code_weights = [0.85, 0.08, 0.03, 0.02, 0.02]
        
        # Store/forward flag (Y/N for whether trip record was held in vehicle memory)
        self.store_forward_flags = ['N', 'Y']
        self.store_forward_weights = [0.98, 0.02]
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def get_time_period(self, dt):
        """Determine time period for demand calculation."""
        hour = dt.hour
        is_weekend = dt.weekday() >= 5
        
        if is_weekend:
            return 'weekend'
        elif hour in [7, 8, 9, 17, 18, 19]:  # Rush hours
            return 'rush'
        elif hour in [10, 11, 12, 13, 14, 15, 16]:  # Business hours
            return 'business'
        elif hour in [20, 21, 22, 23, 0]:  # Evening hours
            return 'evening'
        else:  # Late night/early morning
            return 'night'
    
    def calculate_realistic_fare(self, distance, duration_minutes, rate_code=1, time_period='business'):
        """Calculate realistic fare based on NYC taxi rates."""
        # NYC taxi fare structure (2024 rates)
        initial_charge = 3.00
        distance_rate = 2.50  # per mile
        time_rate = 0.50  # per minute when speed < 12 mph
        
        # Base fare calculation
        distance_fare = distance * distance_rate
        time_fare = duration_minutes * time_rate * 0.3  # Assume 30% of time in slow traffic
        
        # Rate code adjustments
        if rate_code == 2:  # JFK
            base_fare = 70.00
        elif rate_code == 3:  # Newark
            base_fare = 17.50 + distance_fare
        else:
            base_fare = initial_charge + distance_fare + time_fare
        
        # Peak hour surcharge
        if time_period == 'rush':
            base_fare += 1.00
        elif time_period == 'evening':
            base_fare += 0.50
        
        return max(base_fare, 3.00)  # Minimum fare
    
    def generate_enhanced_trip_event(self):
        """Generate enhanced realistic taxi trip completion event with comprehensive features."""
        
        # Select pickup and dropoff locations with realistic patterns
        pickup_location = self.select_weighted_location('pickup')
        dropoff_location = self.select_weighted_location('dropoff', pickup_location)
        
        # Generate realistic trip timing with current timestamp variations
        current_time = datetime.now()
        
        # Add some randomness to make it seem like trips complete at different times
        pickup_time = current_time - timedelta(
            minutes=random.randint(5, 90),
            seconds=random.randint(0, 59)
        )
        
        # Calculate realistic trip duration based on distance and time of day
        base_distance = self.calculate_distance(pickup_location, dropoff_location)
        time_period = self.get_time_period(pickup_time)
        
        # Duration varies by traffic conditions
        traffic_multiplier = {
            'rush': 1.8, 'business': 1.2, 'evening': 1.0, 'night': 0.8, 'weekend': 0.9
        }.get(time_period, 1.0)
        
        base_duration = max(5, int(base_distance * 3.5 * traffic_multiplier))  # ~3.5 min/mile average
        trip_duration = random.randint(
            max(5, int(base_duration * 0.7)), 
            int(base_duration * 1.5)
        )
        
        dropoff_time = pickup_time + timedelta(minutes=trip_duration)
        
        # Select trip characteristics
        rate_code = random.choices(self.rate_codes, weights=self.rate_code_weights)[0]
        payment_type = random.choices(self.payment_types, weights=self.payment_weights)[0]
        trip_type = random.choices(self.trip_types, weights=self.trip_type_weights)[0]
        store_forward_flag = random.choices(self.store_forward_flags, weights=self.store_forward_weights)[0]
        
        # Vendor ID (1 = Creative Mobile Technologies, 2 = VeriFone Inc.)
        vendor_id = random.choices([1, 2], weights=[0.59, 0.41])[0]  # Real NYC distribution
        
        # Calculate fare
        base_fare = self.calculate_realistic_fare(base_distance, trip_duration, rate_code, time_period)
        
        # Tip calculation with realistic patterns
        tip_amount = self.calculate_realistic_tip(base_fare, payment_type, time_period)
        
        # Extra charges (peak hour, misc)
        extra_charges = 0.0
        if time_period == 'rush':
            extra_charges += 1.0  # Peak hour surcharge
        if random.random() < 0.1:  # 10% chance of misc charges
            extra_charges += random.choice([0.50, 1.00])
        
        # MTA tax
        mta_tax = 0.50
        
        # Tolls (realistic distribution)
        tolls_amount = 0.0
        if random.random() < 0.15:  # 15% of trips have tolls
            tolls_amount = random.choices(
                [5.54, 6.12, 8.50, 10.17], 
                weights=[0.4, 0.3, 0.2, 0.1]
            )[0]
        
        # Airport fees
        airport_fee = 0.0
        if pickup_location['name'] in ['JFK Airport', 'LaGuardia Airport']:
            airport_fee = 1.25
        elif pickup_location['name'] == 'Newark Airport':
            airport_fee = 0.0  # Newark has different fee structure
        
        # Congestion surcharge (Manhattan south of 96th St)
        congestion_surcharge = 0.0
        if (pickup_location['borough'] == 'Manhattan' and 
            pickup_location['lat'] < 40.7850 and  # South of 96th St
            time_period in ['rush', 'business']):
            congestion_surcharge = 2.50
        
        # Improvement surcharge
        improvement_surcharge = 0.30
        
        trip_event = {
            'trip_id': f"live_{uuid.uuid4()}",
            'vendor_id': vendor_id,
            'pickup_datetime': pickup_time.isoformat(),
            'dropoff_datetime': dropoff_time.isoformat(),
            'passenger_count': random.choices([1, 2, 3, 4, 5, 6], weights=[0.68, 0.16, 0.08, 0.05, 0.02, 0.01])[0],
            'trip_distance': round(base_distance, 2),
            'pickup_longitude': pickup_location['lon'] + random.uniform(-0.002, 0.002),
            'pickup_latitude': pickup_location['lat'] + random.uniform(-0.002, 0.002),
            'dropoff_longitude': dropoff_location['lon'] + random.uniform(-0.002, 0.002),
            'dropoff_latitude': dropoff_location['lat'] + random.uniform(-0.002, 0.002),
            'rate_code_id': rate_code,
            'store_and_fwd_flag': store_forward_flag,
            'payment_type': payment_type,
            'fare_amount': round(base_fare, 2),
            'extra': round(extra_charges, 2),
            'mta_tax': mta_tax,
            'tip_amount': round(tip_amount, 2),
            'tolls_amount': round(tolls_amount, 2),
            'improvement_surcharge': improvement_surcharge,
            'pickup_location_id': pickup_location['id'],
            'dropoff_location_id': dropoff_location['id'],
            
            # Enhanced fields for feature engineering
            'pickup_borough': pickup_location['borough'],
            'dropoff_borough': dropoff_location['borough'],
            'pickup_demand_tier': pickup_location['demand_tier'],
            'dropoff_demand_tier': dropoff_location['demand_tier'],
            'time_period': time_period,
            'trip_type': trip_type,
            'is_airport_pickup': pickup_location['name'].endswith('Airport'),
            'is_airport_dropoff': dropoff_location['name'].endswith('Airport'),
            'is_cross_borough': pickup_location['borough'] != dropoff_location['borough'],
            'traffic_multiplier': traffic_multiplier,
            'airport_fee': airport_fee,
            'congestion_surcharge': congestion_surcharge,
            
            # Event metadata
            'event_timestamp': datetime.now().isoformat(),
            'simulator_version': '2.0_enhanced'
        }
        
        # Calculate total amount
        trip_event['total_amount'] = round(
            trip_event['fare_amount'] + trip_event['extra'] + trip_event['mta_tax'] + 
            trip_event['tip_amount'] + trip_event['tolls_amount'] + 
            trip_event['improvement_surcharge'] + trip_event['airport_fee'] + 
            trip_event['congestion_surcharge'], 2
        )
        
        return trip_event
    
    def select_weighted_location(self, trip_stage, reference_location=None):
        """Select location with realistic demand patterns."""
        if trip_stage == 'pickup':
            # Weight by demand tier and time period
            current_time = datetime.now()
            time_period = self.get_time_period(current_time)
            
            weights = []
            for location in self.pickup_locations:
                base_weight = {'high': 10, 'medium': 5, 'low': 1}[location['demand_tier']]
                multiplier = self.demand_multipliers[location['demand_tier']][time_period]
                weights.append(base_weight * multiplier)
            
            return random.choices(self.pickup_locations, weights=weights)[0]
        
        else:  # dropoff
            # For dropoff, consider distance and borough patterns
            if reference_location:
                # 60% chance to stay in same borough, 40% to go elsewhere
                if random.random() < 0.6:
                    same_borough_locations = [
                        loc for loc in self.pickup_locations 
                        if loc['borough'] == reference_location['borough']
                    ]
                    if same_borough_locations:
                        return random.choice(same_borough_locations)
            
            # Otherwise, select randomly but favor high-demand areas
            weights = [{'high': 8, 'medium': 4, 'low': 1}[loc['demand_tier']] for loc in self.pickup_locations]
            return random.choices(self.pickup_locations, weights=weights)[0]
    
    def calculate_distance(self, pickup, dropoff):
        """Calculate approximate distance between two points."""
        # Haversine formula for distance calculation
        lat1, lon1 = math.radians(pickup['lat']), math.radians(pickup['lon'])
        lat2, lon2 = math.radians(dropoff['lat']), math.radians(dropoff['lon'])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Radius of earth in miles
        distance = 3959 * c
        
        # Add some randomness for realistic variation
        return max(0.1, distance + random.uniform(-0.2, 0.2))
    
    def calculate_realistic_tip(self, fare_amount, payment_type, time_period):
        """Calculate realistic tip based on payment type and conditions."""
        if payment_type == 'cash':
            # Cash tips are lower and less frequent
            if random.random() < 0.3:  # 30% of cash trips have tips
                return fare_amount * random.uniform(0.05, 0.15)
            return 0.0
        
        elif payment_type == 'credit_card':
            # Credit card tips are more common and higher
            if random.random() < 0.85:  # 85% of card trips have tips
                base_tip_rate = random.uniform(0.15, 0.25)
                
                # Adjust for time period
                if time_period == 'rush':
                    base_tip_rate *= 0.9  # Slightly lower during rush
                elif time_period == 'evening':
                    base_tip_rate *= 1.1  # Higher in evening
                elif time_period == 'weekend':
                    base_tip_rate *= 1.15  # Higher on weekends
                
                return fare_amount * base_tip_rate
            return 0.0
        
        else:
            # Other payment types rarely have tips
            return 0.0
    
    def generate_trip_event(self):
        """Maintain backward compatibility while using enhanced generation."""
        return self.generate_enhanced_trip_event()
    
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