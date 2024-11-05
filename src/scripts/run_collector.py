#!/usr/bin/env python3
import signal
from multiprocessing import Event
import logging
import os
from ether import ether_init, ether_sub

def setup_logging():
    logging.basicConfig(
        level=os.environ.get('LOGLEVEL', 'DEBUG'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info("Collector script starting")
    return logger

class DataCollector:
    def __init__(self):
        pass
    
    @ether_sub()
    def collect_result(self, result_name: str, value: int):
        self._logger.info(f"Collected result: {result_name} = {value}")

if __name__ == "__main__":
    logger = setup_logging()
    
    ether_init()
    
    logger.info("Instantiate and run datacollector")
    collector = DataCollector()
    collector.run() 