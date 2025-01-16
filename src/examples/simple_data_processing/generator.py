import os
from ether import ether_pub, ether_sub

class DataGenerator:
    
    @ether_sub(topic="start")
    @ether_pub(topic="data")
    def generate_data(self, data: int = 42) -> dict:
        print(f"DataGenerator[PID {os.getpid()}]: Generating data - {data}")
        return {"data": data}