import pytest
import time
import multiprocessing
from ether import ether
from ether._internal._config import EtherConfig
from ether.liaison import EtherInstanceLiaison

def run_mixed_autorun_test():
    """Test mixed autorun configuration"""
    
    
    config = {
        "instances": {
            "test_generator": {
                "class_path": "examples.simple_data_processing.DataGenerator",
                "args": [1],
                "autorun": True
            },
            "test_processor": {
                "class_path": "examples.simple_data_processing.DataProcessor",
                "args": [1],
                "autorun": False
            }
        }
    }
    
    # Initialize - should only start generator
    ether.init(config=config, restart=True)
    time.sleep(1)
    tracker = EtherInstanceLiaison()

    # Check that only generator is running
    instances = tracker.get_active_instances()
    assert len(instances) == 1
    instance_names = {i['name'] for i in instances.values()}
    assert "test_generator" in instance_names
    
    # launch processor
    config["instances"]["test_processor"]["autorun"] = True
    processes = ether.init(restart=True, config=config)
    time.sleep(1)
    
    # Check both are running
    instances = tracker.get_active_instances()
    assert len(instances) == 2
    instance_names = {i['name'] for i in instances.values()}
    assert "test_generator" in instance_names
    assert "test_processor" in instance_names

def test_mixed_autorun(process_runner):
    """Test configuration with mixed autorun settings"""
    process_runner(run_mixed_autorun_test, timeout=600)