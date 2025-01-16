import os
from ether import ether_sub, ether_pub

class DataProcessor:
    def __init__(self, process_id: int = 0, multiplier: int = 2):
        self.process_id = process_id
        self.multiplier = multiplier
    
    @ether_sub(topic="data")
    @ether_pub(topic="result")
    def process_data(self, name: str, data: int = 0) -> dict:
        print(f"(PID: {os.getpid()})::Processing data value: {data} from {name}")
        processed_data = data * self.multiplier
        return {
            "result_name": name + f"_{self.multiplier}x",
            "value": processed_data
        }