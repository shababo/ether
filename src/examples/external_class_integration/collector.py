class DataCollector:
    """Simulates an external data collection class that we don't want to modify"""
    
    def __init__(self):
        self.results = []
    
    def collect_result(self, result_name: str, value: int):
        """Collect and store a result
        
        Args:
            result_name: Name/identifier for the result
            value: The result value to store
        """
        print(f"Collected result: {result_name} = {value}")  # Using print since we don't have _logger yet
        self.results.append((result_name, value)) 