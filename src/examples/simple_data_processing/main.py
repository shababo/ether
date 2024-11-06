import time

from examples.simple_data_processing import DataGenerator
from ether import ether_init

if __name__ == "__main__":

    # Configure processor and collector to autorun, but not generator
    config = {
        "instances": {
            "processor2x": {
                "class_path": "examples.simple_data_processing.processor.DataProcessor",
            },
            "processor4x": {
                "class_path": "examples.simple_data_processing.processor.DataProcessor",
                "kwargs": {
                    "multiplier": 4
                }
            },
            "collector": {
                "class_path": "examples.simple_data_processing.collector.DataCollector",

            }
        }
    }
    ether_init(config)

    generator = DataGenerator(name="generator", process_id=1)
    time.sleep(0.5)  # Wait for connections
    
    # Generate data twice
    generator.generate_data(data=42)
    generator.generate_data(data=43)