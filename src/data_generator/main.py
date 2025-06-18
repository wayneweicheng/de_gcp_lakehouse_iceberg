"""
Cloud Function for generating NYC taxi trip data.
"""

import json
import os
import base64
from taxi_trip_simulator import TaxiTripSimulator
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_taxi_data(event, context):
    """
    Cloud Function entry point triggered by Pub/Sub.
    
    Args:
        event: Pub/Sub event data
        context: Cloud Function context
    """
    
    try:
        # Get configuration from environment variables
        project_id = os.environ.get('PROJECT_ID')
        topic_name = os.environ.get('TOPIC_NAME', 'taxi-trips-stream')
        
        if not project_id:
            logger.error("PROJECT_ID environment variable not set")
            return
        
        # Parse the Pub/Sub message
        if 'data' in event:
            message_data = json.loads(
                base64.b64decode(event['data']).decode('utf-8')
            )
        else:
            message_data = {}
        
        # Default parameters
        action = message_data.get('action', 'generate_batch')
        trip_count = message_data.get('count', 10)
        
        logger.info(f"Generating {trip_count} taxi trips for project {project_id}")
        
        # Initialize simulator
        simulator = TaxiTripSimulator(project_id, topic_name)
        
        # Generate trips based on action
        if action == 'generate_batch':
            result = simulator.simulate_batch(trip_count=trip_count)
            logger.info(f"Successfully generated {result} trips")
        else:
            logger.warning(f"Unknown action: {action}")
        
        return {'status': 'success', 'trips_generated': trip_count}
        
    except Exception as e:
        logger.error(f"Error generating taxi data: {e}")
        return {'status': 'error', 'message': str(e)}


 