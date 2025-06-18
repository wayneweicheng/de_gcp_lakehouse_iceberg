"""
Simple Batch Dataflow Flex Template for testing basic functionality.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
import json
import logging
import argparse


class SimpleParseRecord(beam.DoFn):
    """Simple JSON parser for testing."""
    
    def process(self, element):
        try:
            # Parse JSON line
            if isinstance(element, str):
                record = json.loads(element)
            else:
                record = element
            
            # Just log and pass through
            logging.info(f"Processed record: {record.get('trip_id', 'unknown')}")
            yield record
                
        except Exception as e:
            logging.error(f"Error processing record: {str(e)}")
            yield {'error': str(e), 'raw': str(element)}


def run():
    """Main entry point for the simple test template."""
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_files', required=True, help='Input file pattern')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--dataset_id', required=True, help='BigQuery dataset ID')
    
    known_args, pipeline_args = parser.parse_known_args()
    
    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    
    logging.info(f"Starting simple pipeline with input: {known_args.input_files}")
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read JSON files from GCS
        raw_data = (
            pipeline
            | 'Read JSON Files' >> ReadFromText(known_args.input_files)
        )
        
        # Parse records
        processed = (
            raw_data
            | 'Parse Records' >> beam.ParDo(SimpleParseRecord())
        )
        
        # Count records
        count = (
            processed
            | 'Count Records' >> beam.combiners.Count.Globally()
        )
        
        # Log count
        (
            count
            | 'Log Count' >> beam.Map(lambda x: logging.info(f"Total records processed: {x}"))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run() 