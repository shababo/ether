#!/usr/bin/env python3
import sys
import time
import logging
import os
from ether import ether_init
from ether.examples.gen_process_collect import DataGenerator


def setup_logging():
    logging.basicConfig(
        level=os.environ.get('LOGLEVEL', 'DEBUG'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info("Generator script starting")
    return logger



if __name__ == "__main__":
    logger = setup_logging()

    ether_init()

    process_id = int(sys.argv[1])
    logger.info(f"Initializing generator {process_id}")
    
    generator = DataGenerator(process_id)

    logger.info("Waiting for connections")
    time.sleep(0.5)
    
    logger.info("Generating data 1st time")
    generator.generate_data()
    time.sleep(5.0)
    
    logger.info("Generating data 2nd time")
    generator.generate_data()
    
    logger.info("Data generation complete")
