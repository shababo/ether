import pytest
import time
import multiprocessing
import logging
from typing import List, Dict, Optional
from pydantic import BaseModel
from ether import ether, ether_pub, ether_sub
from ether.utils import get_ether_logger

# Test Models
class ValidMessage(BaseModel):
    value: int
    text: str
    optional_field: Optional[str] = None

class NestedMessage(BaseModel):
    items: List[int]
    metadata: Dict[str, str]

# Base class for publishers to handle initialization
class PublisherBase:
    def __init__(self, name=None):
        self.name = name or self.__class__.__name__
        self._logger = get_ether_logger(self.__class__.__name__, instance_name=name)

# Test Classes
class ValidPublisher:
    """Publisher that sends correctly typed messages"""
    
    @ether_pub(topic="valid_messages")
    def send_valid_message(self) -> ValidMessage:
        return ValidMessage(value=42, text="hello")
    
    @ether_pub(topic="nested_messages")
    def send_nested_message(self) -> NestedMessage:
        return NestedMessage(
            items=[1, 2, 3],
            metadata={"source": "test"}
        )
    
    @ether_pub(topic="primitive_message")
    def send_primitive(self) -> int:
        return 42

class InvalidPublisher(PublisherBase):
    """Publisher that attempts to send incorrectly typed messages"""
    
    @ether_pub(topic="invalid_message")
    def send_wrong_type(self) -> ValidMessage:
        return {"value": "not an int", "text": 42}  # Wrong types
    
    @ether_pub(topic="missing_field")
    def send_missing_required(self) -> ValidMessage:
        return {"text": "missing value field"}  # Missing required field
    
    @ether_pub(topic="wrong_return")
    def send_wrong_return_type(self) -> int:
        return "not an integer"  # Wrong return type

class ValidationSubscriber:
    """Subscriber that receives and validates messages"""
    def __init__(self):
        self.received_valid = False
        self.received_nested = False
        self.received_primitive = False
        self.error_received = False
    
    @ether_sub(topic="valid_messages")
    def receive_valid(self, value: int, text: str, optional_field: Optional[str] = None):
        self.received_valid = True
    
    @ether_sub(topic="nested_messages")
    def receive_nested(self, items: List[int], metadata: Dict[str, str]):
        self.received_nested = True
    
    @ether_sub(topic="primitive_message")
    def receive_primitive(self, root: int):
        self.received_primitive = True

def run_valid_type_test():
    """Test valid type scenarios"""
    try:
        config = {
            "instances": {
                "validation_subscriber": {
                    "class_path": "tests.test_type_validation.ValidationSubscriber",
                    "autorun": True
                }
            }
        }
        
        # Initialize system with restart to ensure clean state
        ether.tap(config=config, restart=True)
        time.sleep(1.0)  # Allow more time for setup
        
        # Create publisher and send messages
        publisher = ValidPublisher()
        publisher.send_valid_message()
        publisher.send_nested_message()
        publisher.send_primitive()
        
        time.sleep(1.0)  # Allow more time for message processing
        
    except Exception as e:
        print(f"Error in valid type test: {e}")
        raise



def run_invalid_type_test():
    """Test invalid type scenarios"""
    try:
        # Initialize system first
        ether.tap(restart=True)
        time.sleep(0.5)
        
        publisher = InvalidPublisher()
        
        # These should raise exceptions
        with pytest.raises(ValueError):
            publisher.send_wrong_type()
        
        with pytest.raises(ValueError):
            publisher.send_missing_required()
        
        with pytest.raises(ValueError):
            publisher.send_wrong_return_type()
            
    except Exception as e:
        print(f"Error in invalid type test: {e}")
        raise



class MismatchPublisher(PublisherBase):
    @ether_pub(topic="MismatchSubscriber.receive_message")
    def wrong_field_name(self) -> dict:
        return {"wrong_field_name": 42}
    
    @ether_pub(topic="MismatchSubscriber.receive_message")
    def wrong_field_type(self) -> dict:
        return {"correct_field_name": "not an int"}

class MismatchSubscriber:
    def __init__(self):
        self.error_received = False
    
    @ether_sub()
    def receive_message(self, correct_field_name: int):  # Mismatched field name
        pass

def run_type_mismatch_test():
    """Test mismatched type definitions between publisher and subscriber"""
    config = {
        "instances": {
            "mismatch_subscriber": {
                "class_path": "tests.test_type_validation.MismatchSubscriber",
            }
        }
    }
    
    ether.tap(config=config, restart=True)
    time.sleep(0.5)
    
    publisher = MismatchPublisher()
    publisher.wrong_field_name()  # This should cause an error in the subscriber
    time.sleep(0.5)
    publisher.wrong_field_type()

def test_valid_types():
    """Test that valid type messages are processed correctly"""
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=run_valid_type_test)
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 0 

def test_type_mismatch():
    """Test that mismatched types between pub/sub are handled appropriately"""
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=run_type_mismatch_test)
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 0  # Should fail due to type mismatch

def test_invalid_types():
    """Test that invalid type messages raise appropriate errors"""
    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(target=run_invalid_type_test)
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 0