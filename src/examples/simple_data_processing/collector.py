import os
from ether import ether_sub

class DataCollector:
    
    @ether_sub(topic="result")
    def collect_result(self, result: int):
        print(f"DataCollector[PID {os.getpid()}]: Collected result - {result}")

