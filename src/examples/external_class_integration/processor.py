class DataProcessor:
    """Simulates an external data processing class that we don't want to modify"""
    
    def __init__(self, process_id: int = 0, multiplier: int = 2):
        self.process_id = process_id
        self.multiplier = multiplier
    
    def process_data(self, name: str, data: int = 0) -> dict:
        """Process incoming data by multiplying it
        
        Returns:
            dict: Contains the result name and processed value
        """
        print(f"Processing {name} with data {data}")  # Using print since we don't have _logger yet
        processed_data = data * self.multiplier
        return {
            "result_name": f"{name}_{self.multiplier}x",
            "value": processed_data
        } 