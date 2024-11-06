from typing import Any
import time
from ether import ether_sub, ether_pub, ether_init

class DataGenerator:
    def __init__(self, process_id: int = 0):
        self.process_id = process_id
    
    @ether_pub(topic="DataProcessor.process_data")
    def generate_data(self, data: int = 42) -> dict[str, Any]:
        self._logger.info(f"Generating data: {data}")
        return {"name": f"datagenerator_{self.process_id}", "data": data}
    
class DataProcessor:
    def __init__(self, process_id: int = 0):
        self.process_id = process_id
    
    @ether_sub()
    @ether_pub(topic="DataCollector.collect_result")
    def process_data(self, name: str, data: int = 0) -> dict[str, Any]:
        self._logger.info(f"Processing {name} with data {data}")
        processed_data = data * 2
        return {
            "result_name": name,
            "value": processed_data
        }
    
class DataCollector:
    
    def __init__(self):
        pass
    
    @ether_sub()
    def collect_result(self, result_name: str, value: int):
        self._logger.info(f"Collected result: {result_name} = {value}")

if __name__ == "__main__":

    # Configure processor and collector to autorun, but not generator
    config = {
        "instances": {
            f"processor": {
                "class_path": "ether.examples.gen_process_collect.DataProcessor",
            },
            f"collector": {
                "class_path": "ether.examples.gen_process_collect.DataCollector",

            },
            f"collector2": {
                "class_path": "ether.examples.gen_process_collect.DataCollector",

            }
        }
    }
    ether_init(config)

    generator = DataGenerator(process_id=1)
    time.sleep(0.5)  # Wait for connections
    
    # Generate data twice
    generator.generate_data(data=42)
    time.sleep(0.1)
    generator.generate_data(data=43)
    # time.sleep(0.01)
