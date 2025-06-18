#!/usr/bin/env python3
"""
Ultra minimal test for Dataflow Flex Template debugging.
This script just writes to a file and exits successfully.
"""

import os
import sys
import time

def main():
    """Ultra minimal test function."""
    
    # Write to a file to prove we're running
    try:
        with open('/tmp/dataflow_test.log', 'w') as f:
            f.write(f"SUCCESS: Ultra minimal test ran at {time.time()}\n")
            f.write(f"Python version: {sys.version}\n")
            f.write(f"Working directory: {os.getcwd()}\n")
            f.write(f"Environment variables:\n")
            for key, value in os.environ.items():
                if 'DATAFLOW' in key or 'FLEX' in key:
                    f.write(f"  {key}={value}\n")
        
        print("SUCCESS: Ultra minimal test completed!")
        print(f"Python version: {sys.version}")
        print(f"Working directory: {os.getcwd()}")
        
        # Exit successfully
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 