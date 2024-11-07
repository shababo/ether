import pytest
import time
import logging
from ether import ether_init
from examples.simple_data_processing import DataGenerator, DataProcessor, DataCollector
from ether._daemon import daemon_manager
from ether._instance_tracker import EtherInstanceTracker

@pytest.fixture
def setup_logging():
    logging.basicConfig(level=logging.DEBUG)
    yield

@pytest.fixture
def clean_redis():
    """Clean up Redis before and after each test"""
    tracker = EtherInstanceTracker()
    tracker.cleanup_all()
    yield tracker
    tracker.cleanup_all()

def test_instance_tracking(setup_logging, clean_redis):
    """Test that instances are properly tracked in Redis"""
    tracker = clean_redis  # Use the cleaned tracker
    
    # Initialize Ether system with force_reinit
    ether_init(force_reinit=True)
    time.sleep(1)  # Wait for services to start
    
    # Create instances
    generator = DataGenerator(process_id=1)
    processor = DataProcessor(process_id=1)
    collector = DataCollector()
    
    # Check that instances are registered
    instances = tracker.get_active_instances()
    assert len(instances) == 3
    
    # Verify instance metadata
    generator_instances = [i for i in instances.values() if i['class'] == 'DataGenerator']
    assert len(generator_instances) == 1
    gen_metadata = generator_instances[0]
    assert gen_metadata['pub_topics'] == ['DataProcessor.process_data']
    assert gen_metadata['sub_topics'] == ['DataGenerator.generate_data']
    
    processor_instances = [i for i in instances.values() if i['class'] == 'DataProcessor']
    assert len(processor_instances) == 1
    proc_metadata = processor_instances[0]
    assert 'DataProcessor.process_data' in proc_metadata['sub_topics']
    assert 'DataCollector.collect_result' in proc_metadata['pub_topics']
    
    collector_instances = [i for i in instances.values() if i['class'] == 'DataCollector']
    assert len(collector_instances) == 1
    coll_metadata = collector_instances[0]
    assert 'DataCollector.collect_result' in coll_metadata['sub_topics']
    assert not coll_metadata.get('pub_topics')
    
    # Test message flow
    generator.generate_data(data=42)
    time.sleep(0.5)  # Allow time for message processing
    
    # Cleanup
    del generator
    del processor
    del collector
    time.sleep(0.5)  # Allow time for cleanup
    
    # Verify instances are deregistered
    instances = tracker.get_active_instances()
    assert len(instances) == 0

def test_instance_ttl(setup_logging, clean_redis):
    """Test that instances are properly expired"""
    tracker = clean_redis  # Use the cleaned tracker
    
    # Initialize with force_reinit
    ether_init(force_reinit=True)
    
    # Create instance
    generator = DataGenerator(process_id=1)
    
    # Verify registration
    instances = tracker.get_active_instances()
    assert len(instances) == 1
    
    # Wait for TTL to expire (use short TTL for testing)
    tracker.ttl = 2  # Set short TTL for testing
    time.sleep(3)  # Wait longer than TTL
    
    # Verify instance is expired
    instances = tracker.get_active_instances()
    assert len(instances) == 0
