from ether import ether_pub, ether_sub

class DataGenerator:
    def __init__(self, name: str = "DataGenerator", process_id: int = 0):
        self.name = name
        self.process_id = process_id
    
    @ether_sub()
    @ether_pub(topic="DataProcessor.process_data")
    def generate_data(self, data: int = 42) -> dict:
        print(f"{self.name}: Generating data: {data}")
        return {"name": self.name, "data": data}