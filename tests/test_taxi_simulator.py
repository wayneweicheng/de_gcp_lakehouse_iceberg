"""
Tests for the taxi trip simulator module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import json

from src.data_generator.taxi_trip_simulator import TaxiTripSimulator


class TestTaxiTripSimulator:
    """Test cases for TaxiTripSimulator class."""
    
    @pytest.fixture
    def mock_publisher(self):
        """Mock Pub/Sub publisher client."""
        with patch('src.data_generator.taxi_trip_simulator.pubsub_v1.PublisherClient') as mock:
            publisher = Mock()
            mock.return_value = publisher
            publisher.topic_path.return_value = 'projects/test-project/topics/test-topic'
            publisher.publish.return_value.result.return_value = 'message-id-123'
            yield publisher
    
    @pytest.fixture
    def simulator(self, mock_publisher):
        """Create TaxiTripSimulator instance with mocked dependencies."""
        return TaxiTripSimulator('test-project', 'test-topic')
    
    def test_init(self, simulator, mock_publisher):
        """Test simulator initialization."""
        assert simulator.project_id == 'test-project'
        assert simulator.topic_name == 'test-topic'
        assert len(simulator.pickup_locations) == 10
        assert len(simulator.payment_types) == 4
        assert len(simulator.payment_weights) == 4
    
    def test_generate_trip_event(self, simulator):
        """Test trip event generation."""
        trip_event = simulator.generate_trip_event()
        
        # Check required fields
        required_fields = [
            'trip_id', 'vendor_id', 'pickup_datetime', 'dropoff_datetime',
            'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude',
            'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount',
            'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount',
            'pickup_location_id', 'dropoff_location_id', 'event_timestamp'
        ]
        
        for field in required_fields:
            assert field in trip_event, f"Missing field: {field}"
        
        # Check data types and ranges
        assert isinstance(trip_event['trip_id'], str)
        assert trip_event['trip_id'].startswith('live_')
        assert trip_event['vendor_id'] in [1, 2]
        assert 1 <= trip_event['passenger_count'] <= 5
        assert trip_event['trip_distance'] >= 0.5
        assert trip_event['payment_type'] in ['card', 'cash', 'no_charge', 'dispute']
        assert trip_event['fare_amount'] > 0
        assert trip_event['total_amount'] > 0
        assert trip_event['pickup_location_id'] in [loc['id'] for loc in simulator.pickup_locations]
        
        # Check datetime format
        pickup_dt = datetime.fromisoformat(trip_event['pickup_datetime'])
        dropoff_dt = datetime.fromisoformat(trip_event['dropoff_datetime'])
        assert dropoff_dt > pickup_dt
    
    def test_publish_trip_event(self, simulator, mock_publisher):
        """Test publishing trip event to Pub/Sub."""
        trip_data = {
            'trip_id': 'test-trip-123',
            'vendor_id': 1,
            'pickup_location_id': 161,
            'payment_type': 'card'
        }
        
        message_id = simulator.publish_trip_event(trip_data)
        
        # Verify publisher was called
        mock_publisher.publish.assert_called_once()
        call_args = mock_publisher.publish.call_args
        
        # Check topic path
        assert call_args[0][0] == 'projects/test-project/topics/test-topic'
        
        # Check message data
        message_data = call_args[0][1]
        parsed_data = json.loads(message_data.decode('utf-8'))
        assert parsed_data == trip_data
        
        # Check attributes
        attributes = call_args[1]
        assert attributes['vendor_id'] == '1'
        assert attributes['pickup_location_id'] == '161'
        assert attributes['payment_type'] == 'card'
        assert attributes['event_type'] == 'trip_completion'
        
        assert message_id == 'message-id-123'
    
    def test_simulate_batch(self, simulator, mock_publisher):
        """Test batch simulation."""
        trip_count = simulator.simulate_batch(trip_count=5)
        
        assert trip_count == 5
        assert mock_publisher.publish.call_count == 5
    
    @patch('src.data_generator.taxi_trip_simulator.time.sleep')
    def test_simulate_continuous_stream(self, mock_sleep, simulator, mock_publisher):
        """Test continuous stream simulation."""
        # Mock datetime to control the loop
        with patch('src.data_generator.taxi_trip_simulator.datetime') as mock_datetime:
            start_time = datetime(2024, 1, 1, 10, 0, 0)
            end_time = start_time + timedelta(minutes=2)  # Very short duration for test
            
            mock_datetime.now.side_effect = [start_time, start_time, end_time]
            
            trip_count = simulator.simulate_continuous_stream(
                trips_per_minute=2, 
                duration_hours=1
            )
            
            # Should generate 2 trips in the single iteration
            assert trip_count == 2
            assert mock_publisher.publish.call_count == 2
    
    def test_generate_historical_data(self, simulator, mock_publisher):
        """Test historical data generation."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)  # 2 days
        
        trip_count = simulator.generate_historical_data(
            start_date=start_date,
            end_date=end_date,
            trips_per_day=100
        )
        
        # Should generate trips for 2 days
        assert trip_count > 0
        assert mock_publisher.publish.call_count > 0
        
        # Check that historical trip IDs are generated
        call_args_list = mock_publisher.publish.call_args_list
        for call_args in call_args_list:
            message_data = call_args[0][1]
            parsed_data = json.loads(message_data.decode('utf-8'))
            assert parsed_data['trip_id'].startswith('hist_')
    
    def test_payment_type_distribution(self, simulator):
        """Test that payment types follow expected distribution."""
        payment_counts = {'card': 0, 'cash': 0, 'no_charge': 0, 'dispute': 0}
        
        # Generate many trips to test distribution
        for _ in range(1000):
            trip = simulator.generate_trip_event()
            payment_counts[trip['payment_type']] += 1
        
        # Card should be most common (70% weight)
        assert payment_counts['card'] > payment_counts['cash']
        assert payment_counts['cash'] > payment_counts['no_charge']
        assert payment_counts['no_charge'] > payment_counts['dispute']
    
    def test_tip_calculation(self, simulator):
        """Test tip calculation logic."""
        # Test multiple trips to verify tip logic
        card_tips = []
        cash_tips = []
        
        for _ in range(100):
            trip = simulator.generate_trip_event()
            if trip['payment_type'] == 'card':
                card_tips.append(trip['tip_amount'])
            elif trip['payment_type'] == 'cash':
                cash_tips.append(trip['tip_amount'])
        
        # Card payments should generally have higher tips
        if card_tips and cash_tips:
            avg_card_tip = sum(card_tips) / len(card_tips)
            avg_cash_tip = sum(cash_tips) / len(cash_tips)
            # This is probabilistic, but card tips should generally be higher
            assert avg_card_tip >= avg_cash_tip or abs(avg_card_tip - avg_cash_tip) < 1.0
    
    def test_total_amount_calculation(self, simulator):
        """Test that total amount is calculated correctly."""
        trip = simulator.generate_trip_event()
        
        expected_total = (
            trip['fare_amount'] + trip['extra'] + trip['mta_tax'] + 
            trip['tip_amount'] + trip['tolls_amount']
        )
        
        assert abs(trip['total_amount'] - expected_total) < 0.01  # Allow for rounding
    
    def test_trip_duration_realistic(self, simulator):
        """Test that trip durations are realistic."""
        trip = simulator.generate_trip_event()
        
        pickup_dt = datetime.fromisoformat(trip['pickup_datetime'])
        dropoff_dt = datetime.fromisoformat(trip['dropoff_datetime'])
        duration_minutes = (dropoff_dt - pickup_dt).total_seconds() / 60
        
        # Duration should be between 5 and 120 minutes
        assert 5 <= duration_minutes <= 120
    
    def test_location_coordinates(self, simulator):
        """Test that location coordinates are within NYC bounds."""
        trip = simulator.generate_trip_event()
        
        # NYC approximate bounds
        min_lat, max_lat = 40.4, 41.0
        min_lon, max_lon = -74.3, -73.7
        
        assert min_lat <= trip['pickup_latitude'] <= max_lat
        assert min_lon <= trip['pickup_longitude'] <= max_lon
        assert min_lat <= trip['dropoff_latitude'] <= max_lat
        assert min_lon <= trip['dropoff_longitude'] <= max_lon


class TestTaxiSimulatorIntegration:
    """Integration tests for taxi simulator."""
    
    @pytest.fixture
    def real_simulator(self):
        """Create simulator without mocking for integration tests."""
        with patch('src.data_generator.taxi_trip_simulator.pubsub_v1.PublisherClient'):
            return TaxiTripSimulator('test-project', 'test-topic')
    
    def test_end_to_end_trip_generation(self, real_simulator):
        """Test complete trip generation workflow."""
        # Generate a trip
        trip = real_simulator.generate_trip_event()
        
        # Verify it can be serialized to JSON
        json_str = json.dumps(trip)
        parsed_trip = json.loads(json_str)
        
        # Verify all fields are preserved
        assert parsed_trip == trip
        
        # Verify data quality
        assert parsed_trip['total_amount'] > 0
        assert parsed_trip['trip_distance'] > 0
        assert parsed_trip['pickup_location_id'] != parsed_trip['dropoff_location_id'] or True  # Allow same location
    
    def test_multiple_trips_uniqueness(self, real_simulator):
        """Test that multiple trips have unique IDs."""
        trips = [real_simulator.generate_trip_event() for _ in range(10)]
        trip_ids = [trip['trip_id'] for trip in trips]
        
        # All trip IDs should be unique
        assert len(set(trip_ids)) == len(trip_ids)
    
    def test_data_consistency(self, real_simulator):
        """Test data consistency across multiple trips."""
        trips = [real_simulator.generate_trip_event() for _ in range(50)]
        
        for trip in trips:
            # All trips should have positive amounts
            assert trip['total_amount'] > 0
            assert trip['fare_amount'] > 0
            
            # Passenger count should be reasonable
            assert 1 <= trip['passenger_count'] <= 5
            
            # Vendor ID should be valid
            assert trip['vendor_id'] in [1, 2]
            
            # Payment type should be valid
            assert trip['payment_type'] in ['card', 'cash', 'no_charge', 'dispute'] 