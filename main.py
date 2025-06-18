"""
Main entry point for Dataflow Flex Template.
This follows the standard Dataflow Flex Template pattern.
"""

import argparse
import logging
import sys
import os
from datetime import datetime

def run(argv=None):
    """Main entry point for the Dataflow template."""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=== DATAFLOW TEMPLATE STARTING ===")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Working directory: {os.getcwd()}")
        logger.info(f"Python path: {sys.path}")
        logger.info(f"Arguments: {argv}")
        
        # Parse arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('--input_files', required=False, default='test', help='Input files')
        parser.add_argument('--project_id', required=False, default='test', help='Project ID')
        parser.add_argument('--dataset_id', required=False, default='test', help='Dataset ID')
        
        known_args, pipeline_args = parser.parse_known_args(argv)
        
        logger.info(f"Parsed args - input_files: {known_args.input_files}")
        logger.info(f"Parsed args - project_id: {known_args.project_id}")
        logger.info(f"Parsed args - dataset_id: {known_args.dataset_id}")
        logger.info(f"Pipeline args: {pipeline_args}")
        
        # Write success file to prove execution
        try:
            with open('/tmp/dataflow_main_success.log', 'w') as f:
                f.write(f"SUCCESS: main.py executed at {datetime.now()}\n")
                f.write(f"Args: {known_args}\n")
                f.write(f"Pipeline args: {pipeline_args}\n")
                f.write(f"Working dir: {os.getcwd()}\n")
                f.write(f"Python version: {sys.version}\n")
        except Exception as e:
            logger.warning(f"Could not write success file: {e}")
        
        logger.info("=== DATAFLOW TEMPLATE COMPLETED SUCCESSFULLY ===")
        return 0
        
    except Exception as e:
        logger.error(f"ERROR in main.py: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1

if __name__ == '__main__':
    sys.exit(run()) 