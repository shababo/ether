import pytest
import time
import multiprocessing
from ether import ether_init
from ether._config import EtherConfig
from ether._instance_tracker import EtherInstanceTracker
from ether.examples.gen_process_collect import DataGenerator

def run_manual_generator_test():
    """Test manual generator with auto-running processor and collector"""
    tracker = EtherInstanceTracker()
    
    # Configure processor and collector to autorun, but not generator
    config = {
        "instances": {
            "processor1": {
                "class_path": "ether.examples.gen_process_collect.DataProcessor",
                "args": [1],
                "autorun": True
            },
            "collector1": {
                "class_path": "ether.examples.gen_process_collect.DataCollector",
                "autorun": True
            }
        }
    }
    
    # Initialize system and start processor/collector
    ether_init(config)
    time.sleep(1)  # Wait for services to start
    
    # Verify processor and collector are running
    instances = tracker.get_active_instances()
    assert len(instances) == 2
    instance_names = {i['name'] for i in instances.values()}
    assert "processor1" in instance_names
    assert "collector1" in instance_names
    
    # Manually create and run generator
    generator = DataGenerator(process_id=1)
    time.sleep(0.5)  # Wait for connections
    
    # Generate data twice
    generator.generate_data()
    time.sleep(5.0)
    generator.generate_data()
    
    # Cleanup
    del generator

def test_manual_generator():
    """Test manual generator operation"""
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=run_manual_generator_test)
    process.start()
    process.join()
    assert process.exitcode == 0 