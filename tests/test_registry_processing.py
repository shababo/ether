import pytest
from ether._internal._config import EtherConfig
from ether._internal._registry import EtherRegistry
from examples.external_class_integration import DataGenerator, DataProcessor

def test_registry_decorator_application():
    """Test that decorators are properly applied via registry configuration"""
    
    config_dict = {
        "registry": {
            "examples.external_class_integration.DataGenerator": {
                "methods": {
                    "generate_data": {
                        "ether_pub": {
                            "topic": "test_data"
                        }
                    }
                }
            }
        }
    }
    
    config = EtherConfig.model_validate(config_dict)
    
    # Process the registry configuration
    EtherRegistry().process_registry_config(config.registry)
    
    # Verify decorator was applied
    generator = DataGenerator()
    method = generator.generate_data
    
    # Check for decorator metadata
    assert hasattr(method, '_pub_metadata'), "Pub decorator was not applied"
    assert method._pub_metadata.topic == "test_data"

def test_registry_multiple_decorators():
    """Test applying both pub and sub decorators via registry"""
    
    config_dict = {
        "registry": {
            "examples.external_class_integration.DataProcessor": {
                "methods": {
                    "process_data": {
                        "ether_sub": {
                            "topic": "input_data"
                        },
                        "ether_pub": {
                            "topic": "output_data"
                        }
                    }
                }
            }
        }
    }
    
    config = EtherConfig.model_validate(config_dict)
    EtherRegistry().process_registry_config(config.registry)
    
    # Verify both decorators were applied
    processor = DataProcessor()
    method = processor.process_data
    
    assert hasattr(method, '_sub_metadata'), "Sub decorator was not applied"
    assert hasattr(method, '_pub_metadata'), "Pub decorator was not applied"
    assert method._sub_metadata.topic == "input_data"
    assert method._pub_metadata.topic == "output_data" 