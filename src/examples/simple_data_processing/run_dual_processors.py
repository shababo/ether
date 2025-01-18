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
    python run_dual_processors.py --config-name {dual_processors} 


    Expected output (ordering of processor and generator outputs may be different):

    2025-01-16 11:28:42.765 - Ether:51475 - INFO - Starting Ether session: 52a3af6a-3ca2-4b88-811d-256612abc6c3...
    2025-01-16 11:28:44.331 - Ether:51475 - INFO - Ether system started successfully
    DataGenerator[PID 51483]: Generating data - 42
    DataProcessor[PID 51485]: Processing input data - 42
    DataProcessor[PID 51484]: Processing input data - 42
    DataProcessor[PID 51485]: Processed output result - 168
    DataProcessor[PID 51484]: Processed output result - 84
    DataCollector[PID 51486]: Collected result - 168
    DataCollector[PID 51486]: Collected result - 84
    DataCollector[PID 51486]: Summarizing results - mean: 126.0, max: 168, min: 84
    DataCollector[PID 51486]: Summarizing results - mean: 126.0, max: 168, min: 84
    2025-01-16 11:28:45.346 - Ether:51475 - INFO - Shutting down Ether session: 52a3af6a-3ca2-4b88-811d-256612abc6c3...
    2025-01-16 11:28:45.448 - Ether:51475 - INFO - Ether system shutdown complete
    DataGenerator[PID 51475]: Generating data - 4242
    DataProcessor[PID 51475]: Processing input data - 4242
    DataProcessor[PID 51475]: Processed output result - 8484
    DataProcessor[PID 51475]: Processing input data - 4242
    DataProcessor[PID 51475]: Processed output result - 16968
    DataCollector[PID 51475]: Collected result - 8484
    DataCollector[PID 51475]: Collected result - 16968
    DataCollector[PID 51475]: Summarizing results - mean: 12726.0, max: 16968, min: 8484
    """

    # init ether
    config_path = _get_config_path(config_name)
    ether.tap(config=config_path)

    # use ether's start method to publish to the "start" topic
    # this will call any method decorated with @ether_start
    ether.start()
    time.sleep(1.002)

    # ether.shutdown()

    # you can still use your code normally when ether is not running
    # in other words, if your code is still used other places, it will still work
    generator = DataGenerator()
    processor2x = DataProcessor(multiplier=2)
    processor4x = DataProcessor(multiplier=4)
    collector = DataCollector()
    generated_data = generator.generate_data(data=4242)
    processed2x_result = processor2x.process_data(**generated_data)
    processed4x_result = processor4x.process_data(**generated_data)
    collector.collect_result(**processed2x_result)
    collector.collect_result(**processed4x_result)
    collector.summarize()
    



if __name__ == "__main__":
    main()
