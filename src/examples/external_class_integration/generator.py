class DataGenerator:
    """Simulates an external data generation class that we don't want to modify"""
    
    def __init__(self, process_id: int = 0):
        self.process_id = process_id
        
    def generate_data(self, data: int = 42) -> dict:
        """Generate some data
        
        Returns:
            dict: Contains the generator name and the data value
        """
        print(f"Generating data: {data}")  # Using print since we don't have _logger yet
        return {"name": f"generator_{self.process_id}", "data": data} 