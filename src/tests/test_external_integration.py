import logging
import pytest
import time
import json
import zmq
from pathlib import Path
from ether import ether
from ether.liaison import EtherInstanceLiaison

def test_external_integration():
    """Test that external classes work with Ether via configuration"""
    
    # Setup ZMQ subscriber to listen for messages
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")
    socket.subscribe("test_processed")  # Subscribe to processor output topic
    
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
    ether.tap(config=config_dict)
    
    # Give time for instances to start
    time.sleep(0.1)
    
    # Check that instances are registered
    liaison = EtherInstanceLiaison()
    active_instances = liaison.get_active_instances()
    
    # Verify all expected instances are running
    expected_instances = {"generator1", "processor1", "collector1"}
    running_instances = {info["name"] for info in active_instances.values()}
    assert expected_instances.issubset(running_instances), f"Not all instances running. Expected {expected_instances}, got {running_instances}"
    
    # Trigger data generation
    ether.pub(topic="generate_data", data={"data": 42})
    
    # Wait for and verify the processed message
    socket.RCVTIMEO = 1000  # 1 second timeout
    try:
        topic = socket.recv_string()
        message = socket.recv_string()
        data = json.loads(message)
        
        assert topic == "test_processed", f"Unexpected topic: {topic}"
        assert data["result_name"] == "generator_1_2x", f"Unexpected result name: {data['result_name']}"
        assert data["value"] == 84, f"Unexpected value: {data['value']}"
        
    except zmq.error.Again:
        pytest.fail("Timeout waiting for processed message")
    finally:
        socket.close()
        context.term()
        
    # Cleanup
    ether.shutdown() 