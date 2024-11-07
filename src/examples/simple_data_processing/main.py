import os
import time
import click

from examples.simple_data_processing import DataGenerator
from ether import ether_init

def _get_config_path(config_name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "config", f"{config_name}.yaml")

@click.command()
@click.option('--config-name', default='dual_processors', help='Name of the configuration file')
def main(config_name):
    """
    This example demonstrates a system with a generator, two processors, and a collector.
    The generator produces data, which is processed by both processors (with different multipliers)
    and then collected by the collector.

    Config files are located in the config/ directory relative to this file.

    Command line usage:
    # no .yaml suffix needed for config_name
    # default config_name is dual_processors
    python main.py --config-name {dual_processors} 


    Expected output:
    INFO - EtherMain: - Ether system initialized
    INFO - DataGenerator:generator - Generating data: 42
    INFO - DataProcessor:processor2x - Processing datagenerator_0 with data 42
    INFO - DataProcessor:processor4x - Processing datagenerator_0 with data 42
    INFO - DataGenerator:generator - Generating data: 43
    INFO - DataProcessor:processor2x - Processing datagenerator_0 with data 43
    INFO - DataProcessor:processor4x - Processing datagenerator_0 with data 43
    INFO - DataCollector:collector1 - Collected result: datagenerator_0 = 84
    INFO - DataCollector:collector1 - Collected result: datagenerator_0 = 168
    INFO - DataCollector:collector1 - Collected result: datagenerator_0 = 86
    INFO - DataCollector:collector1 - Collected result: datagenerator_0 = 172
    INFO - EtherMain: - Cleanup complete
    """

    # init ether
    config_path = _get_config_path(config_name)
    ether_init(config=config_path)

    # use the DataGenerator class normally to generate some data
    generator = DataGenerator(name="generator")
    
    # Generate data twice with small delay between calls
    generator.generate_data(data=42)
    time.sleep(0.002)
    generator.generate_data(data=43)


if __name__ == "__main__":
    main()