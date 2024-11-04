#!/usr/bin/env python3
import sys
import signal
from multiprocessing import Event
import logging
from ether import ether_init, ether_pub, ether_sub
from typing import Dict, Any
import os

def setup_logging():
    logging.basicConfig(
        level=os.environ.get('LOGLEVEL', 'DEBUG'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)  # Get logger for this module
    logger.info("Processor script starting")  # Add this line
    return logger

class DataProcessor:
    def __init__(self, process_id: int):
        self.process_id = process_id
    
    @ether_sub()
    @ether_pub(topic="DataCollector.collect_result")
    def process_data(self, name: str, count: int = 0) -> Dict[str, Any]:
        print(f"Processing {name} with count {count}")
        processed_count = count * 2
        return {
            "result_name": name,
            "value": processed_count
        }

if __name__ == "__main__":
    logger = setup_logging()  # Get logger
    ether_init()
    process_id = int(sys.argv[1])
    logger.info(f"Initializing processor {process_id}")  # Add this line
    stop_event = Event()
    
    processor = DataProcessor(process_id)
    
    def handle_signal(signum, frame):
        stop_event.set()
    
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    
    processor.run(stop_event) 