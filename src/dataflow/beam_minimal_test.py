"""
Minimal Apache Beam pipeline for Dataflow Flex Template testing.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging

def run(argv=None):
    """Run a minimal Apache Beam pipeline."""
    
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
        
        # Set up pipeline options
        pipeline_options = PipelineOptions(pipeline_args)
        
        logger.info("Starting minimal Apache Beam pipeline")
        
        # Create a minimal pipeline
        with beam.Pipeline(options=pipeline_options) as pipeline:
            
            # Create test data
            test_data = (
                pipeline
                | 'Create Test Data' >> beam.Create(['test1', 'test2', 'test3'])
                | 'Add Prefix' >> beam.Map(lambda x: f"SUCCESS: {x}")
                | 'Log Results' >> beam.Map(lambda x: logger.info(f"Processed: {x}"))
            )
        
        logger.info("SUCCESS: Minimal Apache Beam pipeline completed")
        return 0
        
    except Exception as e:
        logger.error(f"ERROR in pipeline: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(run()) 