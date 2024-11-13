import logging
import pytest
import time
from pathlib import Path
from ether import ether

def test_external_integration():
    """Test that external classes work with Ether via configuration"""
    
    # Track starting positions of log files
    log_dir = Path("logs/instances")
    log_positions = {}
    
    for class_name in ["DataGenerator", "DataProcessor", "DataCollector"]:
        log_file = log_dir / class_name / f"{class_name}.log"
        if log_file.exists():
            log_positions[class_name] = log_file.stat().st_size
        else:
            log_positions[class_name] = 0
    
    config_dict = {
        "registry": {
            "examples.external_class_integration.DataGenerator": {
                "methods": {
                    "generate_data": {
                        "ether_sub": {"topic": "generate_data"},
                        "ether_pub": {"topic": "test_data"}
                    }
                }
            },
            "examples.external_class_integration.DataProcessor": {
                "methods": {
                    "process_data": {
                        "ether_sub": {"topic": "test_data"},
                        "ether_pub": {"topic": "test_processed"}
                    }
                }
            },
            "examples.external_class_integration.DataCollector": {
                "methods": {
                    "collect_result": {
                        "ether_sub": {"topic": "test_processed"}
                    }
                }
            }
        },
        "instances": {
            "generator1": {
                "class_path": "examples.external_class_integration.DataGenerator",
                "kwargs": {"process_id": 1, "log_level": logging.DEBUG}
            },
            "processor1": {
                "class_path": "examples.external_class_integration.DataProcessor",
                "kwargs": {"multiplier": 2, "log_level": logging.DEBUG}
            },
            "collector1": {
                "class_path": "examples.external_class_integration.DataCollector",
                "kwargs": {"log_level": logging.DEBUG}
            }
        }
    }
    
    # Initialize Ether with test configuration
    ether.init(config=config_dict)
    
    # Give time for instances to start
    time.sleep(0.1)
    
    # Trigger data generation
    ether.pub(topic="generate_data", data={"data": 42})
    
    # Give time for processing
    time.sleep(0.1)
    
    # Check logs for expected messages
    def read_new_lines(class_name):
        log_file = log_dir / class_name / f"{class_name}.log"
        assert log_file.exists(), f"{class_name} log not found"
        with open(log_file) as f:
            # Skip to where we were before the test
            f.seek(log_positions[class_name])
            return f.read()
    
    # Check Generator log
    generator_content = read_new_lines("DataGenerator")
    print(generator_content)
    assert "Received message: topic=generate_data, data={'data': 42}" in generator_content, "Generator didn't process data"
    assert "Publishing to topic: test_data" in generator_content, "Generator didn't publish"
    
    # Check Processor log
    processor_content = read_new_lines("DataProcessor")
    assert "Received message: topic=test_data, data={'name': 'generator_1', 'data': 42}" in processor_content, "Processor didn't process data"
    assert "Publishing to topic: test_processed" in processor_content, "Processor didn't publish"
    
    # Check Collector log
    collector_content = read_new_lines("DataCollector")
    assert "Received message: topic=test_processed, data={'result_name': 'generator_1_2x', 'value': 84}" in collector_content, "Collector didn't receive result"

    # Cleanup
    ether.shutdown() 