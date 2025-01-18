import pytest
import time
import multiprocessing
import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from pydantic import BaseModel
from ether import ether, ether_pub, ether_sub, ether_save
from ether.liaison import EtherInstanceLiaison
from ether.utils import get_ether_logger
from pprint import pprint

# Test Models
class ComplexMessage(BaseModel):
    text: str
    numbers: List[int]
    nested: Dict[str, Dict[str, float]]
    optional: Optional[str] = None

class DataVerificationPublisher:
    """Publisher that sends messages with verifiable data"""
    def __init__(self):
        self._logger = get_ether_logger(self.__class__.__name__)
    
    @ether_pub(topic="complex_data")
    def send_complex_data(self) -> ComplexMessage:
        return ComplexMessage(
            text="test message",
            numbers=[1, 2, 3, 4, 5],
            nested={
                "outer1": {"inner1": 1.1, "inner2": 1.2},
                "outer2": {"inner3": 2.1, "inner4": 2.2}
            },
            optional="optional value"
        )
    
    @ether_pub(topic="primitive_data")
    def send_primitive(self) -> int:
        return 42
    
    @ether_pub(topic="list_data")
    def send_list(self) -> list:
        return [1, 2, 3, 4, 5]

class ReceivedData(BaseModel):
    """Holds received message data for verification"""
    complex_received: Optional[bool] = False
    complex_data: Optional[ComplexMessage] = None
    primitive_received: Optional[bool] = False
    primitive_value: Optional[int] = None
    list_received: Optional[bool] = False
    list_data: Optional[List[int]] = None

class DataVerificationSubscriber:
    """Subscriber that verifies received message data"""
    def __init__(self):
        self._logger = get_ether_logger(self.__class__.__name__)
        self.received_data = ReceivedData()
        self.liaison = EtherInstanceLiaison()

    def _update_received_data(self):
        self.liaison.update_instance_data(
            self.id, 
            {"received_data": self.received_data.model_dump()}
        )
        self._logger.info(f"Updated received data for {self.id}")
    
    @ether_sub(topic="complex_data")
    def receive_complex(self, text: str, numbers: List[int], 
                       nested: Dict[str, Dict[str, float]], 
                       optional: Optional[str] = None):
        self._logger.info(f"Received complex data: {text}, {numbers}, {nested}, {optional}")
        self.received_data.complex_received = True
        self.received_data.complex_data = ComplexMessage(
            text=text,
            numbers=numbers,
            nested=nested,
            optional=optional
        )
        self._update_received_data()

    @ether_sub(topic="primitive_data")
    def receive_primitive(self, root: int):
        self._logger.info(f"Received primitive: {root}")
        self.received_data.primitive_received = True
        self.received_data.primitive_value = root
        self._update_received_data()
    
    @ether_sub(topic="list_data")
    def receive_list(self, root: List[int]):
        self._logger.info(f"Received list: {root}")
        self.received_data.list_received = True
        self.received_data.list_data = root
        self._update_received_data()


def verify_received_data(received: ReceivedData):
    """Verify that all received data matches expected values"""
    # Verify complex message
    assert received.complex_received, "Complex message not received"
    assert received.complex_data.text == "test message"
    assert received.complex_data.numbers == [1, 2, 3, 4, 5]
    assert received.complex_data.nested == {
        "outer1": {"inner1": 1.1, "inner2": 1.2},
        "outer2": {"inner3": 2.1, "inner4": 2.2}
    }
    assert received.complex_data.optional == "optional value"
    
    # Verify primitive
    assert received.primitive_received, "Primitive not received"
    assert received.primitive_value == 42
    
    # Verify list
    assert received.list_received, "List not received"
    assert received.list_data == [1, 2, 3, 4, 5]

def run_data_verification_test():
    """Run the data verification test"""
    config = {
        "instances": {
            "data_verification_subscriber": {
                "class_path": "tests.test_message_data.DataVerificationSubscriber",
                "autorun": True
            }
        }
    }
    
    try:
        # Initialize system
        ether.tap(config=config, restart=True)
        time.sleep(1.0)  # Allow time for setup
        
        # Create publisher and send messages
        publisher = DataVerificationPublisher()
        publisher.send_complex_data()
        publisher.send_primitive()
        publisher.send_list()
        
        # Allow time for message processing
        time.sleep(1.0)
        
        # Get subscriber instance and verify data
        liaison = EtherInstanceLiaison()
        instances = liaison.get_active_instances()

        subscriber_found = False
        for instance_info in instances.values():
            if instance_info.get('name') == 'data_verification_subscriber':
                subscriber_found = True
                pprint(instance_info['received_data'])
                verify_received_data(ReceivedData(**instance_info['received_data']))
                break
        
        assert subscriber_found, "Subscriber instance not found"
        
    except Exception as e:
        print(f"Error in data verification test: {e}")
        raise

def test_message_data():
    """Test that message data is accurately transmitted and received"""
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=run_data_verification_test)
    process.start()
    process.join()
    assert process.exitcode == 0

if __name__ == "__main__":
    pytest.main([__file__]) 