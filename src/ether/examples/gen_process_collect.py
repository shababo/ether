from typing import Any
from ether import ether_sub, ether_pub

class DataGenerator:
    def __init__(self, process_id: int):
        self.process_id = process_id
    
    @ether_pub(topic="DataProcessor.process_data")
    def generate_data(self, count: int = 42) -> dict[str, Any]:
        self._logger.info(f"Generating data: {count}")
        return {"name": f"datagenerator_{self.process_id}", "count": count}
    
class DataProcessor:
    def __init__(self, process_id: int):
        self.process_id = process_id
    
    @ether_sub()
    @ether_pub(topic="DataCollector.collect_result")
    def process_data(self, name: str, count: int = 0) -> dict[str, Any]:
        self._logger.info(f"Processing {name} with count {count}")
        processed_count = count * 2
        return {
            "result_name": name,
            "value": processed_count
        }
    
class DataCollector:
    
    def __init__(self):
        pass
    
    @ether_sub()
    def collect_result(self, result_name: str, value: int):
        self._logger.info(f"Collected result: {result_name} = {value}")