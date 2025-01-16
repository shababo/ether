import os
from ether import ether_pub, ether_sub

class DataGenerator:
    def __init__(self, name: str = "DataGenerator", process_id: int = 0):
        self.name = name
        self.process_id = process_id
    
    @ether_sub(topic="start")
    @ether_pub(topic="data")
    def generate_data(self, data: int = 42) -> dict:
        print(f"(PID: {os.getpid()})::{self.name}: Generating data: {data}")
        return {"name": self.name, "data": data}