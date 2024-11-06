#!/usr/bin/env python3
import sys
import logging
import os

from ether import ether_init
from ether.examples.gen_process_collect import DataProcessor


def setup_logging():
    logging.basicConfig(
        level=os.environ.get('LOGLEVEL', 'DEBUG'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)  # Get logger for this module
    logger.info("Processor script starting")  # Add this line
    return logger


if __name__ == "__main__":
    
    logger = setup_logging()  # Get logger
    
    ether_init()
    
    process_id = int(sys.argv[1])
    logger.info(f"Initializing processor {process_id}")  # Add this line
    
    # instantiate and run dataprocessor
    processor = DataProcessor(process_id)
    processor.run() 