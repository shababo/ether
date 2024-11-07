import os
import time
import argparse

from examples.simple_data_processing import DataGenerator
from ether import ether_init

def _get_config_path(config_name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "config", f"{config_name}.yaml")

# @ether_config(config_path="config/dual_processors.yaml")
if __name__ == "__main__":

    # parse input args for config
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_name", type=str, default="dual_processors")
    args = parser.parse_args()
    config_path = _get_config_path(args.config_name)

    # init ether
    ether_init(config=config_path)

    # use the DataGenerator class normally to generate some data
    generator = DataGenerator(name="generator")
    
    # Generate data twice
    generator.generate_data(data=42)
    time.sleep(0.002)
    generator.generate_data(data=43)
