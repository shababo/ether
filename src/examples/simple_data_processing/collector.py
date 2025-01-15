from ether import ether_sub

class DataCollector:
    
    def __init__(self):
        pass
    
    @ether_sub()
    def collect_result(self, result_name: str, value: int):
        print(f"Collected result: {result_name} = {value}")

