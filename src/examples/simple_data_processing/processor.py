import os
from ether import ether_sub, ether_pub

class DataProcessor:
    def __init__(self, multiplier: int = 2):
        self.multiplier = multiplier
    
    @ether_sub
    @ether_pub(topic="DataCollector.collect_result")
    def process_data(self, data: int = 0) -> dict:
        print(f"DataProcessor[PID {os.getpid()}]: Processing input data - {data}")
        result = data * self.multiplier
        print(f"DataProcessor[PID {os.getpid()}]: Processed output result - {result}")
        return {
            "result": result
        }