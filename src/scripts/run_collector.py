#!/usr/bin/env python3
import signal
from multiprocessing import Event
import logging
import os
from ether import ether_init, ether_sub
from ether.examples.gen_process_collect import DataCollector

def setup_logging():
    logging.basicConfig(
        level=os.environ.get('LOGLEVEL', 'DEBUG'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info("Collector script starting")
    return logger

if __name__ == "__main__":
    logger = setup_logging()
    
    ether_init()
    
    logger.info("Instantiate and run datacollector")
    collector = DataCollector()
    collector.run() 