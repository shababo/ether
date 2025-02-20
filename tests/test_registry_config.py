from ether._internal._config import EtherConfig

def test_registry_config_parsing():
    """Test parsing of registry configuration"""
    config_dict = {
        "registry": {
            "examples.external_class_integration.DataGenerator": {
                "methods": {
                    "generate_data": {
                        "ether_sub": {},
                        "ether_pub": {
                            "topic": "data"
                        }
                    }
                }
            },
            "examples.external_class_integration.DataProcessor": {
                "methods": {
                    "process_data": {
                        "ether_sub": {
                            "topic": "data"
                        },
                        "ether_pub": {
                            "topic": "processed_data"
                        }
                    }
                }
            }
        },
        "instances": {
            "generator1": {
                "class_path": "examples.external_class_integration.DataGenerator"
            },
            "processor1": {
                "class_path": "examples.external_class_integration.DataProcessor",
                "kwargs": {
                    "multiplier": 4
                }
            }
        }
    }
    
    config = EtherConfig.model_validate(config_dict)
    
    # Test registry parsing
    assert "examples.external_class_integration.DataGenerator" in config.registry
    gen_config = config.registry["examples.external_class_integration.DataGenerator"]
    assert "generate_data" in gen_config.methods
    
    # Test method config parsing
    gen_method = gen_config.methods["generate_data"]
    assert gen_method.ether_pub is not None
    assert gen_method.ether_pub.topic == "data"
    assert gen_method.ether_sub is not None
    
    # Test instance config still works
    assert "generator1" in config.instances
    assert config.instances["generator1"].class_path == "examples.external_class_integration.DataGenerator" 