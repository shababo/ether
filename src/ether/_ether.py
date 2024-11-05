from functools import wraps
import importlib
from multiprocessing import Event, Process
import inspect
from typing import Any, Set, Type, Optional
import uuid
import zmq
from pydantic import BaseModel, create_model, RootModel
import signal
import logging
import time
import json
import atexit
import os
import tempfile
import sys

from ._registry import EtherRegistry
from ._utils import _get_logger, _ETHER_SUB_PORT, _ETHER_PUB_PORT


# class EtherMixin:
#     def __init__(self, *args, **kwargs):
#         # Initialize sockets right away
#         self.setup_sockets(
#             name=kwargs.get('name'),
#             log_level=kwargs.get('log_level', logging.INFO)
#         )
    
#     def setup_sockets(self, name: str = None, log_level: int = logging.INFO):
#         """Setup ZMQ sockets and initialize Ether functionality"""
#         # Move initialization here
#         self.id = uuid.uuid4()
#         self.name = name or self.id
#         self._logger = _get_logger(f"{self.__class__.__name__}:{self.name}", log_level)
#         self._sub_address = f"tcp://localhost:{_ETHER_SUB_PORT}"
#         self._pub_address = f"tcp://localhost:{_ETHER_PUB_PORT}"
        
#         # Socket handling
#         self._zmq_context = zmq.Context()
#         self._sub_socket = None
#         self._pub_socket = None
#         self._zmq_methods = {}
        
#         # Message tracking
#         self.received_messages = set()
#         self.latencies = []
#         self.publishers = {}
#         self.first_message_time = None
#         self.last_message_time = None
#         self.subscription_time = None
        
#         # Setup sockets
#         if hasattr(self, '_sub_address'):
#             self._sub_socket = self._zmq_context.socket(zmq.SUB)
#             if self._sub_address.startswith("tcp://*:"):
#                 self._sub_socket.bind(self._sub_address)
#             else:
#                 self._sub_socket.connect(self._sub_address)
#             self._sub_socket.setsockopt(zmq.RCVHWM, 1000000)
#             self._sub_socket.setsockopt(zmq.RCVBUF, 65536)
#             self.subscription_time = time.time()
            
#             # Setup subscriptions
#             for attr_name in dir(self):
#                 attr = getattr(self, attr_name)
#                 if hasattr(attr, '_sub_metadata'):
#                     metadata = attr._sub_metadata
#                     topic = metadata.topic
#                     self._sub_socket.subscribe(topic.encode())
#                     self._logger.debug(f"Subscribed to topic: {topic}")
#                     self._zmq_methods[topic] = metadata
        
#         if hasattr(self, '_pub_address'):
#             self._pub_socket = self._zmq_context.socket(zmq.PUB)
#             if self._pub_address.startswith("tcp://*:"):
#                 self._pub_socket.bind(self._pub_address)
#             else:
#                 self._pub_socket.connect(self._pub_address)
#             self._pub_socket.setsockopt(zmq.SNDHWM, 1000000)
#             self._pub_socket.setsockopt(zmq.SNDBUF, 65536)
        
#         time.sleep(0.1)
    
#     def track_message(self, publisher_id: str, sequence: int, timestamp: float):
#         """Track message statistics"""
#         now = time.time()
        
#         # Initialize publisher tracking if needed
#         if publisher_id not in self.publishers:
#             self.publishers[publisher_id] = {
#                 "sequences": set(),
#                 "gaps": [],
#                 "last_sequence": None,
#                 "first_time": now
#             }
        
#         pub_stats = self.publishers[publisher_id]
        
#         # Track sequence numbers for this publisher
#         if pub_stats["last_sequence"] is not None:
#             expected = pub_stats["last_sequence"] + 1
#             if sequence > expected:
#                 gap = sequence - expected
#                 pub_stats["gaps"].append((expected, sequence, gap))
#                 self._logger.debug(f"Gap from publisher {publisher_id}: "
#                                  f"expected {expected}, got {sequence}")
        
#         pub_stats["last_sequence"] = sequence
#         pub_stats["sequences"].add(sequence)
        
#         # Track timing
#         latency = (now - timestamp) * 1000
#         self.latencies.append(latency)
#         self.received_messages.add((publisher_id, sequence))
        
#         if self.first_message_time is None:
#             self.first_message_time = now
#         self.last_message_time = now
    
#     def save_results(self):
#         """Save results to file if results_file is set"""
#         if not self.results_file:
#             return
            
#         results = {
#             "latencies": self.latencies,
#             "received_messages": list(self.received_messages),
#             "publishers": {
#                 pid: {
#                     "sequences": list(stats["sequences"]),
#                     "gaps": stats["gaps"],
#                     "last_sequence": stats["last_sequence"],
#                     "first_time": stats["first_time"]
#                 }
#                 for pid, stats in self.publishers.items()
#             }
#         }
        
#         with open(self.results_file, 'w') as f:
#             json.dump(results, f)
    
#     def run(self, stop_event: Event):
#         """Run with result saving support"""
#         def handle_signal(signum, frame):
#             stop_event.set()
        
#         signal.signal(signal.SIGTERM, handle_signal)
#         # self.setup_sockets()
        
#         while not stop_event.is_set():
#             try:
#                 if self._sub_socket:
#                     self.receive_single_message()
#             except Exception as e:
#                 self._logger.error(f"Error in run loop: {e}")
#                 break
        
#         # Save results before cleanup if results_file is set
#         self.save_results()
#         self.cleanup()

#     def cleanup(self):
#         if self._sub_socket:
#             self._sub_socket.close()
#         if self._pub_socket:
#             self._pub_socket.close()
#         if self._zmq_context:
#             self._zmq_context.term()

#     def __del__(self):
#         self.cleanup()

#     def receive_single_message(self, timeout=1000):
#         if self._sub_socket and self._sub_socket.poll(timeout):
#             topic = self._sub_socket.recv_string()
#             data = self._sub_socket.recv_json()
            
#             if topic in self._zmq_methods:
#                 metadata = self._zmq_methods[topic]
                
#                 if isinstance(metadata.args_model, type) and issubclass(metadata.args_model, RootModel):
#                     args = {'root': metadata.args_model(data).root}
#                 else:
#                     try:
#                         model_instance = metadata.args_model(**data)
#                         args = model_instance.model_dump()
#                     except Exception as e:
#                         self._logger.error(f"Error processing data: {e}")
#                         raise
                
#                 metadata.func(self, **args)
#             else:
#                 self._logger.warning(f"Received message for unknown topic: {topic}")

def _create_model_from_signature(func) -> Type[BaseModel]:
    """Creates a Pydantic model from a function's signature"""
    sig = inspect.signature(func)
    fields = {}
    
    for name, param in sig.parameters.items():
        if name == 'self':
            continue
        
        annotation = param.annotation if param.annotation != inspect.Parameter.empty else Any
        default = param.default if param.default != inspect.Parameter.empty else ...
        
        # Handle root model case
        if name == 'root':
            return RootModel[annotation]
        
        fields[name] = (annotation, default)
    
    model_name = f"{func.__name__}Args"
    return create_model(model_name, **fields)

class EtherPubMetadata:
    """Holds metadata about Ether publisher methods"""
    def __init__(self, func, topic: str):
        self.func = func
        self.topic = topic

class EtherSubMetadata:
    """Holds metadata about Ether subscriber methods"""
    def __init__(self, func, topic: str, args_model: Type[BaseModel]):
        self.func = func
        self.topic = topic
        self.args_model = args_model

def ether_pub(topic: Optional[str] = None):
    """Decorator for methods that should publish messages."""
    def decorator(func):
        # Get return type hint if it exists
        return_type = inspect.signature(func).return_annotation
        if return_type == inspect.Parameter.empty:
            raise TypeError(f"Function {func.__name__} must have a return type hint")
        
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not hasattr(self, '_pub_socket'):
                raise RuntimeError("Cannot publish: no publisher socket configured")
            
            # Execute the function and get result
            result = func(self, *args, **kwargs)
            
            self._logger.info(f"Publishing result: {result}")
            # Validate result against return type
            if isinstance(return_type, type) and issubclass(return_type, BaseModel):
                validated_result = return_type(**result).model_dump_json()
            else:
                ResultModel = RootModel[return_type]
                validated_result = ResultModel(result).model_dump_json()
            
            self._logger.info(f"Validated result: {validated_result}")
            # Get topic from metadata
            actual_topic = topic or f"{func.__qualname__}"
            self._logger.info(f"Publishing to topic: {actual_topic}")

            # Publish the validated result
            self._pub_socket.send_multipart([
                actual_topic.encode(),
                validated_result.encode()
            ])
            
            self._logger.info(f"Published result: {result}")
            return result
        
        # Create and attach the metadata
        actual_topic = topic or f"{func.__qualname__}"
        wrapper._pub_metadata = EtherPubMetadata(func, actual_topic)
        
        # Mark the containing class for Ether processing
        frame = inspect.currentframe().f_back
        while frame:
            locals_dict = frame.f_locals
            if '__module__' in locals_dict and '__qualname__' in locals_dict:
                EtherRegistry.mark_for_processing(
                    locals_dict['__qualname__'],
                    locals_dict['__module__']
                )
                break
            frame = frame.f_back
        
        return wrapper
    return decorator

def ether_sub(topic: Optional[str] = None):
    """Decorator for methods that should receive messages."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        
        # Create and attach the metadata
        args_model = _create_model_from_signature(func)
        actual_topic = topic or f"{func.__qualname__}"
        wrapper._sub_metadata = EtherSubMetadata(func, actual_topic, args_model)
        
        # Mark the containing class for Ether processing
        frame = inspect.currentframe().f_back
        while frame:
            locals_dict = frame.f_locals
            if '__module__' in locals_dict and '__qualname__' in locals_dict:
                EtherRegistry.mark_for_processing(
                    locals_dict['__qualname__'],
                    locals_dict['__module__']
                )
                break
            frame = frame.f_back
        
        return wrapper
    return decorator


