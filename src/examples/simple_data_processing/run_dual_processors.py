import os
import time
import click
from ether import ether
from examples.simple_data_processing import DataGenerator, DataProcessor, DataCollector


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


    Expected output (ordering of processor and generator outputs may be different):
    Ether system initialized
    INFO - DataGenerator:generator_within_process - Generating data: 42
    INFO - DataProcessor:processor4x - Processing generator_within_process with data 42
    INFO - DataProcessor:processor2x - Processing generator_within_process with data 42
    INFO - DataCollector:collector1 - Collected result: generator_within_process_4x = 168
    INFO - DataCollector:collector1 - Collected result: generator_within_process_2x = 84
    INFO - DataGenerator:generator_interprocess - Generating data: 44
    INFO - DataProcessor:processor4x - Processing generator_interprocess with data 44
    INFO - DataProcessor:processor2x - Processing generator_interprocess with data 44
    INFO - DataCollector:collector1 - Collected result: generator_interprocess_4x = 176
    INFO - DataCollector:collector1 - Collected result: generator_interprocess_2x = 88
    """

    # init ether
    config_path = _get_config_path(config_name)
    ether.init(config=config_path)

    # use the DataGenerator class normally to generate some data
    # generator = DataGenerator(name="generator_within_process")
    # generator.generate_data(data=42)
    # time.sleep(0.002)
    # # delete the instance
    # del generator

    # use the pub function to trigger DataGenerator.generate_data
    # via the automatically launched DataGenerator instance in the config
    ether.pub({"data": 42}, topic="DataGenerator.generate_data")
    time.sleep(1.002)

    generator = DataGenerator(name="generator_within_process")
    processor2x = DataProcessor(process_id=2, multiplier=2)
    processor4x = DataProcessor(process_id=4, multiplier=4)
    collector = DataCollector()
    generated_data = generator.generate_data(data=4242)
    processed2x_result = processor2x.process_data(**generated_data)
    processed4x_result = processor4x.process_data(**generated_data)
    collector.collect_result(**processed2x_result)
    collector.collect_result(**processed4x_result)

if __name__ == "__main__":
    main()