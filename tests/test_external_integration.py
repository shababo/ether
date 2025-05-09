import logging
import pytest
import time
import json
import zmq
from ether import ether

def test_external_integration():
    """Test that external classes work with Ether via configuration"""
    
    
    
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
                "kwargs": {"process_id": 1, "ether_log_level": logging.DEBUG}
            },
            "processor1": {
                "class_path": "examples.external_class_integration.DataProcessor",
                "kwargs": {"multiplier": 2, "ether_log_level": logging.DEBUG}
            },
            "collector1": {
                "class_path": "examples.external_class_integration.DataCollector",
                "kwargs": {"ether_log_level": logging.DEBUG}
            }
        }
    }

    try:
    
        # Initialize Ether with test configuration
        ether.tap(config=config_dict)
        
        # Check that instances are registered
        active_instances = ether.get_active_instances()
        
        # Verify all expected instances are running
        expected_instances = {"generator1", "processor1", "collector1"}
        running_instances = {info["name"] for info in active_instances.values()}
        assert expected_instances.issubset(running_instances), f"Not all instances running. Expected {expected_instances}, got {running_instances}"

        # Setup ZMQ subscriber to listen for messages
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect("tcp://localhost:13311")
        socket.subscribe("test_processed")  # Subscribe to processor output topic
        
        # Trigger data generation
        ether.pub(topic="generate_data", data={"data": 42})

        time.sleep(3.0)

        # Wait for and verify the processed message
        socket.RCVTIMEO = 5000  # 5 second timeout
        try:
            msg = socket.recv_multipart()
            topic = msg[0].decode("utf-8")
            data = json.loads(msg[1].decode("utf-8"))
            
            assert topic == "test_processed", f"Unexpected topic: {topic}"
            assert data["result_name"] == "generator_1_2x", f"Unexpected result name: {data['result_name']}"
            assert data["value"] == 84, f"Unexpected value: {data['value']}"
            
        except zmq.error.Again:
            pytest.fail("Timeout waiting for processed message")
        finally:
            socket.close()
            context.term()
    finally:
        
        # Cleanup
        ether.shutdown() 