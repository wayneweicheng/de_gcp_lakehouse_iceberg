"""
Dataflow Flex Template test following the exact expected pattern.
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
        # Parse arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('--input_files', required=False, default='test', help='Input files')
        parser.add_argument('--project_id', required=False, default='test', help='Project ID')
        parser.add_argument('--dataset_id', required=False, default='test', help='Dataset ID')
        
        known_args, pipeline_args = parser.parse_known_args(argv)
        
        logger.info(f"SUCCESS: Dataflow template test started at {datetime.now()}")
        logger.info(f"Input files: {known_args.input_files}")
        logger.info(f"Project ID: {known_args.project_id}")
        logger.info(f"Dataset ID: {known_args.dataset_id}")
        logger.info(f"Pipeline args: {pipeline_args}")
        logger.info(f"Working directory: {os.getcwd()}")
        logger.info(f"Python version: {sys.version}")
        
        # Write success file
        try:
            with open('/tmp/dataflow_success.log', 'w') as f:
                f.write(f"SUCCESS: Template executed at {datetime.now()}\n")
                f.write(f"Args: {known_args}\n")
                f.write(f"Pipeline args: {pipeline_args}\n")
        except Exception as e:
            logger.warning(f"Could not write success file: {e}")
        
        logger.info("SUCCESS: Template execution completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"ERROR in template execution: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1

if __name__ == '__main__':
    sys.exit(run()) 