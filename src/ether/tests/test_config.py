import pytest
import time
import multiprocessing
from ether import ether_init
from ether._config import EtherConfig
from ether._instance_tracker import EtherInstanceTracker

def run_test():
    """Run the actual test in a separate process"""
    # Create tracker inside the process
    tracker = EtherInstanceTracker()
    
    config = {
        "instances": [
            {
                "class_path": "ether.examples.gen_process_collect.DataGenerator",
                "args": [1],
                "kwargs": {"name": "test_generator"}
            },
            {
                "class_path": "ether.examples.gen_process_collect.DataProcessor",
                "args": [1],
                "kwargs": {"name": "test_processor"}
            }
        ]
    }
    
    ether_init(config)
    time.sleep(1)  # Wait for instances to start
    
    # Check that instances are registered
    instances = tracker.get_active_instances()
    assert len(instances) == 2
    
    # Verify instance names
    instance_names = {i['name'] for i in instances.values()}
    assert "test_generator" in instance_names
    assert "test_processor" in instance_names

def test_yaml_config():
    """Test launching instances from YAML config"""
    # Use spawn context to avoid issues with fork
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=run_test)
    process.start()
    process.join()
    assert process.exitcode == 0