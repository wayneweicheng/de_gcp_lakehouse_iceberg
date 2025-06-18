"""
Minimal test script for Dataflow Flex Template debugging.
"""

import sys
import logging
import argparse

def run():
    """Minimal test function."""
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_files', required=True, help='Input file pattern')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--dataset_id', required=True, help='BigQuery dataset ID')
    
    known_args, pipeline_args = parser.parse_known_args()
    
    print(f"SUCCESS: Minimal test completed!")
    print(f"Input files: {known_args.input_files}")
    print(f"Project ID: {known_args.project_id}")
    print(f"Dataset ID: {known_args.dataset_id}")
    print(f"Pipeline args: {pipeline_args}")
    
    logging.info("Minimal test completed successfully")
    
    # Exit successfully
    sys.exit(0)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run() 