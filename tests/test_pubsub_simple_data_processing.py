import time
import gc 

from ether import ether
from examples.simple_data_processing import DataGenerator, DataProcessor, DataCollector

def test_instance_tracking():
    """Run instance tracking test in a separate process"""

    try:
        # Initialize Ether system with force_reinit
        ether.tap(restart=True)
        time.sleep(1)  # Wait for services to start
        
        # Create instances
        generator = DataGenerator(process_id=1)
        processor = DataProcessor(process_id=1)
        collector = DataCollector()
        
        # Check that instances are registered
        instances = ether.get_active_instances()
        assert len(instances) == 3
        
        # Verify instance metadata
        generator_instances = [i for i in instances.values() if i['class'] == 'DataGenerator']
        assert len(generator_instances) == 1
        gen_metadata = generator_instances[0]
        assert gen_metadata['pub_topics'] == ['DataProcessor.process_data']
        assert gen_metadata['sub_topics'] == ['Ether.start', 'Ether.cleanup']
        
        processor_instances = [i for i in instances.values() if i['class'] == 'DataProcessor']
        assert len(processor_instances) == 1
        proc_metadata = processor_instances[0]
        assert 'DataProcessor.process_data' in proc_metadata['sub_topics']
        assert 'DataCollector.collect_result' in proc_metadata['pub_topics']
        
        collector_instances = [i for i in instances.values() if i['class'] == 'DataCollector']
        assert len(collector_instances) == 1
        coll_metadata = collector_instances[0]
        assert 'DataCollector.collect_result' in coll_metadata['sub_topics']
        assert 'summarize' in coll_metadata['pub_topics']
        
        # Test message flow
        generator.generate_data(data=42)
        time.sleep(0.5)  # Allow time for message processing
        
        # Cleanup
        generator.cleanup()
        processor.cleanup()
        collector.cleanup()

        time.sleep(1.0)  # Allow time for cleanup
        
        # Verify instances are deregistered
        instances = ether.get_active_instances()
        assert len(instances) == 0
    finally:
        ether.shutdown()

def test_instance_ttl():
    """Run TTL test in a separate process"""
    # Initialize with force_reinit
    ether.tap(restart=True)
    
    # Create instance
    generator = DataGenerator(process_id=1)
    
    # Verify registration
    instances = ether.get_active_instances()
    assert len(instances) == 1
    
    # Wait for TTL to expire (use short TTL for testing)
    ether._ether._instance_manager.ttl = 2  # Set short TTL for testing
    time.sleep(3)  # Wait longer than TTL
    
    # Verify instance is expired
    instances = ether.get_active_instances()
    assert len(instances) == 0


