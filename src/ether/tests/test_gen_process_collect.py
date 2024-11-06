import pytest
import time
import logging
from ether import ether_init
from ether.examples.gen_process_collect import DataGenerator, DataProcessor, DataCollector
from ether._daemon import daemon_manager
from ether._instance_tracker import EtherInstanceTracker

@pytest.fixture
def setup_logging():
    logging.basicConfig(level=logging.DEBUG)
    yield
    
@pytest.fixture
def tracker():
    return EtherInstanceTracker()

def test_instance_tracking(setup_logging, tracker):
    """Test that instances are properly tracked in Redis"""
    # Initialize Ether system
    ether_init()
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
    assert not gen_metadata.get('sub_topics')
    
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
    generator.generate_data(count=42)
    time.sleep(0.5)  # Allow time for message processing
    
    # Cleanup
    del generator
    del processor
    del collector
    time.sleep(0.5)  # Allow time for cleanup
    
    # Verify instances are deregistered
    instances = tracker.get_active_instances()
    assert len(instances) == 0

def test_instance_ttl(setup_logging, tracker):
    """Test that instances are properly expired"""
    ether_init()
    
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

def test_daemon_monitoring(setup_logging, tracker):
    """Test that daemon properly monitors instances"""
    ether_init()
    time.sleep(1)  # Wait for services
    
    # Create instances
    generator = DataGenerator(process_id=1)
    processor = DataProcessor(process_id=1)
    
    # Wait for monitoring cycle
    time.sleep(11)  # Daemon monitors every 10 seconds
    
    # Check daemon's monitor log for instance data
    # (This would require adding some way to access monitor logs)
    
    # Cleanup
    del generator
    del processor 