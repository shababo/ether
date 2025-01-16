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

    2025-01-16 11:09:11.150 - Ether:50274 - INFO - Starting Ether session: 2a12ac7d-ad67-44e8-af00-e1214a7461ce...
    2025-01-16 11:09:12.708 - Ether:50274 - INFO - Ether system started successfully
    DataGenerator[PID 50284]: Generating data - 42
    DataProcessor[PID 50285]: Processing input data - 42
    DataProcessor[PID 50285]: Processed output result - 84
    DataProcessor[PID 50286]: Processing input data - 42
    DataProcessor[PID 50286]: Processed output result - 168
    DataCollector[PID 50287]: Collected result - 84
    DataCollector[PID 50287]: Collected result - 168
    2025-01-16 11:09:13.721 - Ether:50274 - INFO - Shutting down Ether session: 2a12ac7d-ad67-44e8-af00-e1214a7461ce...
    2025-01-16 11:09:13.829 - Ether:50274 - INFO - Ether system shutdown complete
    DataGenerator[PID 50274]: Generating data - 4242
    DataProcessor[PID 50274]: Processing input data - 4242
    DataProcessor[PID 50274]: Processed output result - 8484
    DataProcessor[PID 50274]: Processing input data - 4242
    DataProcessor[PID 50274]: Processed output result - 16968
    DataCollector[PID 50274]: Collected result - 8484
    DataCollector[PID 50274]: Collected result - 16968
    """

    # init ether
    config_path = _get_config_path(config_name)
    ether.init(config=config_path)

    # use the pub function to trigger DataGenerator.generate_data
    # via the automatically launched DataGenerator instance in the config
    ether.start()
    time.sleep(1.002)

    ether.shutdown()

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