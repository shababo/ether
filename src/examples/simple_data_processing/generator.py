from ether import ether_pub, ether_sub

class DataGenerator:
    def __init__(self, process_id: int = 0):
        self.process_id = process_id
    
    @ether_sub()
    @ether_pub(topic="DataProcessor.process_data")
    def generate_data(self, data: int = 42) -> dict:
        self._logger.info(f"Generating data: {data}")
        return {"name": self.name, "data": data}