import pytest
import time
import multiprocessing
from ether import ether
from ether.config import EtherConfig
from ether.liaison import EtherInstanceLiaison
    

def test_mixed_autorun():
    """Test mixed autorun configuration"""
    try:
        tracker = EtherInstanceLiaison()
        
        config = {
            "instances": {
                "test_generator": {
                    "class_path": "examples.simple_data_processing.DataGenerator",
                    "autorun": True
                },
                "test_processor": {
                    "class_path": "examples.simple_data_processing.DataProcessor",
                    "autorun": False
                }
            }
        }
        
        # Initialize - should only start generator
        ether.tap(config=config, restart=True)
        time.sleep(1)
        
        # Check that only generator is running
        instances = tracker.get_active_instances()
        assert len(instances) == 1
        instance_names = {i['name'] for i in instances.values()}
        assert "test_generator" in instance_names
        
        # launch processor
        config["instances"]["test_processor"]["autorun"] = True
        ether.tap(config=config, restart=True)
        time.sleep(1)
        
        # Check both are running
        instances = tracker.get_active_instances()
        assert len(instances) == 2
        instance_names = {i['name'] for i in instances.values()}
        assert "test_generator" in instance_names
        assert "test_processor" in instance_names
    finally:
        ether.shutdown()
