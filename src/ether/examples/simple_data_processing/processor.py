from ether import ether_sub, ether_pub

class DataProcessor:
    def __init__(self, process_id: int = 0, multiplier: int = 2):
        self.process_id = process_id
        self.multiplier = multiplier
    
    @ether_sub()
    @ether_pub(topic="DataCollector.collect_result")
    def process_data(self, name: str, data: int = 0) -> dict:
        self._logger.info(f"Processing {name} with data {data}")
        processed_data = data * self.multiplier
        return {
            "result_name": name,
            "value": processed_data
        }