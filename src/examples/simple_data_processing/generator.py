import os
from ether import ether_pub, ether_sub, ether_start

class DataGenerator:
    
    @ether_start()
    @ether_pub(topic="DataProcessor.process_data")
    def generate_data(self, data: int = 42) -> dict:
        print(f"DataGenerator[PID {os.getpid()}]: Generating data - {data}")
        return {"data": data}