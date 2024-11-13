from examples.external_class_integration import DataGenerator, DataProcessor, DataCollector

def test_basic_functionality():
    """Test that the external classes work correctly without Ether"""
    
    # Create instances
    generator = DataGenerator(process_id=1)
    processor = DataProcessor(process_id=1, multiplier=2)
    collector = DataCollector()
    
    # Generate some data
    gen_result = generator.generate_data(data=42)
    assert gen_result == {"name": "generator_1", "data": 42}
    
    # Process the data
    proc_result = processor.process_data(name=gen_result["name"], data=gen_result["data"])
    assert proc_result == {"result_name": "generator_1_2x", "value": 84}
    
    # Collect the result
    collector.collect_result(proc_result["result_name"], proc_result["value"])
    assert collector.results == [("generator_1_2x", 84)] 