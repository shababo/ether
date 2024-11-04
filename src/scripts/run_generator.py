#!/usr/bin/env python3
import sys
import time
import logging
import os
from ether import ether_init, ether_pub
from typing import Dict, Any

def setup_logging():
    logging.basicConfig(
        level=os.environ.get('LOGLEVEL', 'DEBUG'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info("Generator script starting")
    return logger

class DataGenerator:
    def __init__(self, process_id: int):
        self.process_id = process_id
    
    @ether_pub(topic="DataProcessor.process_data")
    def generate_data(self, count: int = 42) -> Dict[str, Any]:
        return {"name": f"datagenerator_{self.process_id}", "count": count}

if __name__ == "__main__":
    logger = setup_logging()
    ether_init()
    process_id = int(sys.argv[1])
    logger.info(f"Initializing generator {process_id}")
    
    generator = DataGenerator(process_id)
    logger.info("Waiting for connections")
    time.sleep(0.5)
    logger.info("Generating data")
    generator.generate_data()
    logger.info("Data generation complete")