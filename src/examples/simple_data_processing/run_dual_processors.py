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
    This example demonstrates a system with a data generator, two data processors, and a result collector.
    The generator produces data, which is processed by both processors (with different multipliers)
    and then both results are collected by the collector.

    Config files are located in the config directory relative to this file.

    Command line usage:
    # no .yaml suffix needed for config_name
    # default config_name is dual_processors
    python main.py --config-name {dual_processors} 


    Expected output (ordering of processor and generator outputs may be different):

    2025-01-16 10:28:30.814 - Ether:47841 - INFO - Starting Ether session: d6322dbb-8d3f-4064-bb4a-645bcb355b06...
    2025-01-16 10:28:32.371 - Ether:47841 - INFO - Ether system started successfully
    Ether system initialized
    (PID: 47857)::DataGenerator: Generating data: 42
    (PID: 47858)::Processing data value: 42 from DataGenerator
    (PID: 47859)::Processing data value: 42 from DataGenerator
    (PID: 47860)::Collected result from DataGenerator_4x = 168
    (PID: 47860)::Collected result from DataGenerator_2x = 84
    2025-01-16 10:28:33.382 - Ether:47841 - INFO - Shutting down Ether session: d6322dbb-8d3f-4064-bb4a-645bcb355b06...
    2025-01-16 10:28:33.488 - Ether:47841 - INFO - Ether system shutdown complete
    (PID: 47841)::generator_within_process: Generating data: 4242
    (PID: 47841)::Processing data value: 4242 from generator_within_process
    (PID: 47841)::Processing data value: 4242 from generator_within_process
    (PID: 47841)::Collected result from generator_within_process_2x = 8484
    (PID: 47841)::Collected result from generator_within_process_4x = 16968
    """

    # init ether
    config_path = _get_config_path(config_name)
    ether.init(config=config_path)

    # use the pub function to trigger DataGenerator.generate_data
    # via the automatically launched DataGenerator instance in the config
    ether.pub(topic="start")
    time.sleep(1.002)

    ether.shutdown()

    # you can still use your code normally when ether is not running
    # in other words, if your code is still used other places, it will still work
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