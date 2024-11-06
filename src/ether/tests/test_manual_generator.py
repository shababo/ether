import pytest
import time
import multiprocessing
import logging
import uuid
from ether import ether_init
from ether._config import EtherConfig
from ether._instance_tracker import EtherInstanceTracker
from ether.examples.gen_process_collect import DataGenerator

def run_manual_generator_test():
    """Test manual generator with auto-running processor and collector"""
    tracker = EtherInstanceTracker()
    tracker.cleanup_all()  # Ensure clean state
    
    # Use unique names for each test run
    run_id = uuid.uuid4().hex[:8]
    
    # Configure processor and collector to autorun, but not generator
    config = {
        "instances": {
            f"processor_{run_id}": {
                "class_path": "ether.examples.gen_process_collect.DataProcessor",
                "args": [1],
                "autorun": True
            },
            f"collector_{run_id}": {
                "class_path": "ether.examples.gen_process_collect.DataCollector",
                "autorun": True,
                "kwargs": {
                    # "log_level": logging.DEBUG,
                    "name": f"collector_{run_id}"  # Explicit name for logging
                }
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
    assert f"processor_{run_id}" in instance_names
    assert f"collector_{run_id}" in instance_names
    
    # Manually create and use generator
    generator = DataGenerator(process_id=1)
    time.sleep(0.5)  # Wait for connections
    
    # Generate data twice
    generator.generate_data(data=42)
    time.sleep(0.1)
    generator.generate_data(data=43)
    

def test_manual_generator():
    """Test manual generator operation"""
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=run_manual_generator_test)
    process.start()
    process.join()
    assert process.exitcode == 0