import pytest
import time
import multiprocessing
from ether import ether_init
from ether._config import EtherConfig
from ether._instance_tracker import EtherInstanceTracker

def run_mixed_autorun_test():
    """Test mixed autorun configuration"""
    tracker = EtherInstanceTracker()
    
    config = {
        "instances": {
            "test_generator": {
                "class_path": "ether.examples.gen_process_collect.DataGenerator",
                "args": [1],
                "autorun": True
            },
            "test_processor": {
                "class_path": "ether.examples.gen_process_collect.DataProcessor",
                "args": [1],
                "autorun": False
            }
        }
    }
    
    # Initialize - should only start generator
    config_obj = ether_init(config)
    time.sleep(1)
    
    # Check that only generator is running
    instances = tracker.get_active_instances()
    assert len(instances) == 1
    instance_names = {i['name'] for i in instances.values()}
    assert "test_generator" in instance_names
    
    # Manually launch processor
    processes = config_obj.launch_instances(only_autorun=False)
    time.sleep(1)
    
    # Check both are running
    instances = tracker.get_active_instances()
    assert len(instances) == 2
    instance_names = {i['name'] for i in instances.values()}
    assert "test_generator" in instance_names
    assert "test_processor" in instance_names

def test_mixed_autorun():
    """Test configuration with mixed autorun settings"""
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=run_mixed_autorun_test)
    process.start()
    process.join()
    assert process.exitcode == 0