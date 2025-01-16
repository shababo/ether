import os
from ether import ether_sub

class DataCollector:
    
    def __init__(self):
        pass
    
    @ether_sub(topic="result")
    def collect_result(self, result_name: str, value: int):
        print(f"(PID: {os.getpid()})::Collected result from {result_name} = {value}")

