from functools import wraps
from multiprocessing import Event, Process
import inspect
from typing import Any, Set, Type, Optional, Dict
import uuid
import zmq
from pydantic import BaseModel, ValidationError, create_model, RootModel
import logging
import time
import json
import os
import sys
import importlib

from ._utils import _get_logger, _ETHER_SUB_PORT, _ETHER_PUB_PORT
from ether.liaison import EtherInstanceLiaison
from ._config import EtherConfig, EtherClassConfig

class EtherRegistry:
    """Registry to track and process classes with Ether methods"""
    _pending_classes: dict[str, str] = {}  # qualname -> module_name
    _processed_classes: Set[str] = set()
    _logger = _get_logger("EtherRegistry", log_level=logging.DEBUG)
    
    @classmethod
    def mark_for_processing(cls, class_qualname: str, module_name: str):
        cls._pending_classes[class_qualname] = module_name
    
    @classmethod
    def process_registry_config(cls, config: Dict[str, EtherClassConfig]):
        """Process registry configuration and apply decorators
        
        Args:
            config: Dictionary mapping class paths to their configurations
        """
        cls._logger.debug("Processing registry configuration...")
        
        for class_path, class_config in config.items():
            # Import the class
            module_path, class_name = class_path.rsplit('.', 1)
            try:
                module = importlib.import_module(module_path)
                target_class = getattr(module, class_name)
            except (ImportError, AttributeError) as e:
                cls._logger.error(f"Failed to import {class_path}: {e}")
                continue
            
            cls._logger.debug(f"Processing class {class_path}")
            
            # Process each method
            for method_name, method_config in class_config.methods.items():
                if not hasattr(target_class, method_name):
                    cls._logger.warning(f"Method {method_name} not found in {class_path}")
                    continue
                
                # Get the original method
                original_method = getattr(target_class, method_name)
                
                # Apply decorators in the correct order (sub then pub)
                decorated_method = original_method

                if method_config.ether_pub:
                    from ..decorators import ether_pub
                    kwargs = {}
                    if method_config.ether_pub.topic:
                        kwargs['topic'] = method_config.ether_pub.topic
                    decorated_method = ether_pub(**kwargs)(decorated_method)
                
                if method_config.ether_sub:
                    from ..decorators import ether_sub
                    kwargs = {}
                    if method_config.ether_sub.topic:
                        kwargs['topic'] = method_config.ether_sub.topic
                    decorated_method = ether_sub(**kwargs)(decorated_method)
                
                
                
                # Replace the original method with the decorated version
                setattr(target_class, method_name, decorated_method)
                cls._logger.debug(f"Applied decorators to {class_path}.{method_name}")
            
            # Mark class for Ether functionality
            cls.mark_for_processing(class_name, module_path)
    
    @classmethod
    def process_pending_classes(cls):
        cls._logger.info("Processing pending classes...")
        cls._logger.debug(f"Pending classes: {cls._pending_classes}")
        cls._logger.debug(f"Processed classes: {cls._processed_classes}")
        
        for qualname, module_name in list(cls._pending_classes.items()):  # Create a copy of items to modify dict
            if qualname in cls._processed_classes:
                cls._logger.debug(f"Class {qualname} already processed, skipping")
                continue
                
            # Import the module that contains the class
            module = sys.modules.get(module_name)
            if module and hasattr(module, qualname):
                class_obj = getattr(module, qualname)
                cls._logger.debug(f"Adding Ether functionality to {qualname}")
                add_ether_functionality(class_obj)
                cls._processed_classes.add(qualname)
                cls._logger.debug(f"Successfully processed {qualname}")
            else:
                cls._logger.warning(f"Could not find class {qualname} in module {module_name}")

def add_ether_functionality(cls):
    """Adds Ether functionality directly to a class"""
    # If class already has Ether functionality, return it
    if hasattr(cls, '_ether_methods_info'):
        return cls
        
    # Collect Ether methods
    ether_methods = {
        name: method for name, method in cls.__dict__.items()
        if hasattr(method, '_pub_metadata') or hasattr(method, '_sub_metadata')
    }
    
    # Store Ether method information (even if empty)
    cls._ether_methods_info = ether_methods
    
    # Add core attributes
    def init_ether_vars(self, name=None, log_level=logging.INFO):
        self.id = str(uuid.uuid4())
        self.name = name or self.id
        self._logger = _get_logger(
            process_name=self.__class__.__name__,
            instance_name=self.name,
            log_level=log_level
        )
        self._logger.debug(f"Initializing {self.name}")
        self._sub_address = f"tcp://localhost:{_ETHER_SUB_PORT}"
        self._pub_address = f"tcp://localhost:{_ETHER_PUB_PORT}"
        
        # Socket handling
        self._zmq_context = zmq.Context()
        self._sub_socket = None
        self._sub_topics = set()
        self._sub_metadata = {}
        self._pub_socket = None
        self._pub_metadata = {}
        
        # Message tracking
        self.received_messages = set()
        self.latencies = []
        self.publishers = {}
        self.first_message_time = None
        self.last_message_time = None
        self.subscription_time = None
        self.results_file = None
        
        # Register with instance tracker
        self._instance_tracker = EtherInstanceLiaison()
        self._instance_tracker.register_instance(self.id, {
            'name': self.name,
            'process_name': name or self.id,  # Use ID if no name provided
            'class': self.__class__.__name__,
            'pub_topics': [m._pub_metadata.topic for m in self._ether_methods_info.values() 
                          if hasattr(m, '_pub_metadata')],
            'sub_topics': [m._sub_metadata.topic for m in self._ether_methods_info.values() 
                          if hasattr(m, '_sub_metadata')]
        })
    
    def setup_sockets(self):
        self._logger.debug(f"Setting up sockets for {self.name}")


        has_sub_method = False
        has_pub_method = False
        for method in self._ether_methods_info.values():
            if hasattr(method, '_sub_metadata'):
                has_sub_method = True
                topic = method._sub_metadata.topic
                self._logger.debug(f"Adding sub topic: {topic}")
                self._sub_topics.add(topic)
                self._sub_metadata[topic] = method._sub_metadata
            if hasattr(method, '_pub_metadata'):
                self._logger.debug(f'pub to topic {method._pub_metadata.topic}')
                has_pub_method = True

        self._logger.debug(f"Final topics: {self._sub_topics}")
        self._logger.debug(f"Final metadata: {self._sub_metadata}")

        if hasattr(self, '_sub_address') and has_sub_method:
            self._sub_socket = self._zmq_context.socket(zmq.SUB)
            if self._sub_address.startswith("tcp://*:"):
                self._sub_socket.bind(self._sub_address)
            else:
                self._sub_socket.connect(self._sub_address)
            self._sub_socket.setsockopt(zmq.RCVHWM, 1000000)
            self._sub_socket.setsockopt(zmq.RCVBUF, 65536)
            self.subscription_time = time.time()
            
            # Setup subscriptions
            for method in self._ether_methods_info.values():
                if hasattr(method, '_sub_metadata'):
                    self._logger.debug(f"Subscribing with sub_metadata: {method._sub_metadata}")
                    topic = method._sub_metadata.topic
                    self._sub_socket.subscribe(topic.encode())
                    self._logger.debug(f"Subscribed to topic: {topic}")
        
        if hasattr(self, '_pub_address') and has_pub_method:
            self._pub_socket = self._zmq_context.socket(zmq.PUB)
            if self._pub_address.startswith("tcp://*:"):
                self._pub_socket.bind(self._pub_address)
            else:
                self._pub_socket.connect(self._pub_address)
            self._pub_socket.setsockopt(zmq.SNDHWM, 1000000)
            self._pub_socket.setsockopt(zmq.SNDBUF, 65536)


        time.sleep(0.1)
    
    # Add message tracking
    def track_message(self, publisher_id: str, sequence: int, timestamp: float):
        now = time.time()
        if publisher_id not in self.publishers:
            self.publishers[publisher_id] = {
                "sequences": set(),
                "gaps": [],
                "last_sequence": None,
                "first_time": now
            }
        
        pub_stats = self.publishers[publisher_id]
        if pub_stats["last_sequence"] is not None:
            expected = pub_stats["last_sequence"] + 1
            if sequence > expected:
                gap = sequence - expected
                pub_stats["gaps"].append((expected, sequence, gap))
        
        pub_stats["last_sequence"] = sequence
        pub_stats["sequences"].add(sequence)
        
        latency = (now - timestamp) * 1000
        self.latencies.append(latency)
        self.received_messages.add((publisher_id, sequence))
        
        if self.first_message_time is None:
            self.first_message_time = now
        self.last_message_time = now

    def save_results(self):
        """Save results to file if results_file is set"""
        if not self.results_file:
            return
            
        results = {
            "latencies": self.latencies,
            "received_messages": list(self.received_messages),
            "publishers": {
                pid: {
                    "sequences": list(stats["sequences"]),
                    "gaps": stats["gaps"],
                    "last_sequence": stats["last_sequence"],
                    "first_time": stats["first_time"]
                }
                for pid, stats in self.publishers.items()
            }
        }
        
        with open(self.results_file, 'w') as f:
            json.dump(results, f)

    def receive_single_message(self, timeout=1000):
        if self._sub_socket and self._sub_socket.poll(timeout=timeout):
            # try:
                # Receive multipart message
            message = self._sub_socket.recv_multipart()
            topic = message[0].decode()
            data = json.loads(message[1].decode())
            
            self._logger.debug(f"Received message: topic={topic}, data={data}")
            
            if topic in self._sub_topics:
                metadata = self._sub_metadata.get(topic)
                if metadata:
                    if isinstance(metadata.args_model, type) and issubclass(metadata.args_model, RootModel):
                        args = {'root': metadata.args_model(data).root}
                    else:
                        model_instance = metadata.args_model(**data)
                        args = model_instance.model_dump()
                    # try:
                    metadata.func(self, **args)
                    # except ValidationError as e:
                    #     validation_err_msg = f"Validation error receiving message: {e}"
                        # self._logger.error(validation_err_msg)
                        # assert False, validation_err_msg
                        #     # raise
            # except Exception as e:
            #     self._logger.error(f"Error receiving message: {e}")

    
    # Add run method
    def run(self):
        last_refresh = 0
        while True:
            try:
                # Refresh TTL periodically (half the TTL time)
                now = time.time()
                if now - last_refresh >= (self._instance_tracker.ttl / 2):
                    self._instance_tracker.refresh_instance(self.id)
                    last_refresh = now
                
                if self._sub_socket:
                    self.receive_single_message()
            except Exception as e:
                self._logger.error(f"Error in run loop: {e}")
                break
        
        self.save_results()
        self.cleanup()
    
    # Add cleanup
    def cleanup(self):
        if hasattr(self, '_instance_tracker'):
            self._instance_tracker.deregister_instance(self.id)
        if hasattr(self, '_sub_socket') and self._sub_socket:
            self._sub_socket.close()
        if hasattr(self, '_pub_socket') and self._pub_socket:
            self._pub_socket.close()
        if hasattr(self, '_zmq_context') and self._zmq_context:
            self._zmq_context.term()
    
    # Add methods to class
    cls.init_ether = init_ether_vars
    cls.setup_sockets = setup_sockets
    cls.track_message = track_message
    cls.save_results = save_results
    cls.run = run
    cls.cleanup = cleanup
    cls.receive_single_message = receive_single_message
    
    # Modify __init__ to initialize attributes
    original_init = cls.__init__
    def new_init(self, *args, **kwargs):
        # Initialize Ether functionality first
        self.init_ether(
            name=kwargs.pop('name', None),
            log_level=kwargs.pop('log_level', logging.INFO),
        )
        # Call original init with remaining args
        original_init(self, *args, **kwargs)
        # Setup sockets after initialization
        self.setup_sockets()
    cls.__init__ = new_init
    
    # Add cleanup on deletion
    def new_del(self):
        self.cleanup()
    cls.__del__ = new_del
    
    return cls


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



def _ether_pub(topic: Optional[str] = None):
    """Decorator for methods that should publish messages."""
    def decorator(func):
        # Get return type hint if it exists
        return_type = inspect.signature(func).return_annotation
        if return_type == inspect.Parameter.empty:
            raise TypeError(f"Function {func.__name__} must have a return type hint")
        
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self._logger.debug(f"Inside pub wrapper for {func.__name__}")
            if not hasattr(self, '_pub_socket'):
                raise RuntimeError("Cannot publish: no publisher socket configured")
            
            # Execute the function and get result
            result = func(self, *args, **kwargs)
            
            # self._logger.debug(f"Publishing result: {result}")
            # Validate result against return type
            if not isinstance(result, return_type) and isinstance(return_type, type) and issubclass(return_type, BaseModel):
                validated_result = return_type(**result).model_dump_json()
            else:
                ResultModel = RootModel[return_type]
                validated_result = ResultModel(result).model_dump_json()
            
            # self._logger.debug(f"Validated result: {validated_result}")
            # Get topic from metadata
            actual_topic = topic or f"{func.__qualname__}"
            # self._logger.debug(f"Publishing to topic: {actual_topic}")
            self._logger.debug(f"Publishing to topic: {actual_topic}")
            # Publish the validated result
            self._pub_socket.send_multipart([
                actual_topic.encode(),
                validated_result.encode()
            ])
            
            # self._logger.debug(f"Published result: {result}")
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

def _ether_sub(topic: Optional[str] = None, subtopic: Optional[str] = None):
    """Decorator for methods that should receive messages."""
    def decorator(func):
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        
        # Create and attach the metadata
        args_model = _create_model_from_signature(func)
        
        actual_topic = topic or f"{func.__qualname__}"
        if subtopic:
            actual_topic = f"{actual_topic.split('.')[0]}.{subtopic}"
        
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





