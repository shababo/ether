import os
import time
import click
from ether import ether

def _get_config_path(config_name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "config", f"{config_name}.yaml")

@click.command()
@click.option('--config-name', default='basic_flow', help='Name of the configuration file')
def main(config_name):
    """
    This example demonstrates using external classes with Ether via configuration.
    The classes are not modified with decorators in their source code - instead,
    the decorators are applied through the configuration.

    Config files are located in the config/ directory relative to this file.

    Expected output:
    Generating data: 42
    Processing generator_1 with data 42
    Processing generator_1 with data 42
    Collected result: generator_1_2x = 84
    Collected result: generator_1_4x = 168
    Generating data: 44
    Processing generator_1 with data 44
    Processing generator_1 with data 44
    Collected result: generator_1_2x = 88
    Collected result: generator_1_4x = 176
    """
    # Initialize Ether with our configuration
    config_path = _get_config_path(config_name)
    ether.tap(config=config_path)

    # Use the DataGenerator class directly
    # from examples.external_class_integration import DataGenerator
    # generator = DataGenerator(process_id=1)
    # generator.generate_data(data=42)
    # time.sleep(0.1)  # Give time for processing

    # Use the pub function to trigger DataGenerator.generate_data
    # via the automatically launched instance
    ether.pub({"data": 44}, topic="DataGenerator.generate_data")
    time.sleep(0.1)  # Give time for processing

if __name__ == "__main__":
    main() 