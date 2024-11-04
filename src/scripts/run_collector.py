#!/usr/bin/env python3
import signal
from multiprocessing import Event
import logging
import os
from ether import ether_init, ether_sub

def setup_logging():
    logging.basicConfig(
        level=os.environ.get('LOGLEVEL', 'INFO'),
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
        print(f"Collected result: {result_name} = {value}")

if __name__ == "__main__":
    logger = setup_logging()
    ether_init()
    logger.info("Initializing collector")
    stop_event = Event()
    
    collector = DataCollector()
    
    def handle_signal(signum, frame):
        stop_event.set()
    
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    
    logger.info("Starting collector event loop")
    collector.run(stop_event) 