from functools import wraps
import inspect
from typing import Any, Set, Type, Optional, Dict
import uuid
import zmq
from pydantic import BaseModel, ValidationError, create_model, RootModel
import logging
import time
import json
import sys
import importlib

from ..utils import _get_logger, _ETHER_SUB_PORT, _ETHER_PUB_PORT
from ether.liaison import EtherInstanceLiaison
from ._config import EtherClassConfig
from ._reqrep import (
    W_READY, W_REQUEST, W_REPLY, MDPW_WORKER, MDPC_CLIENT,
    REQUEST_WORKER_INDEX, REQUEST_COMMAND_INDEX, REQUEST_CLIENT_ID_INDEX, REQUEST_DATA_INDEX,
    REPLY_CLIENT_INDEX, REPLY_SERVICE_INDEX, REPLY_DATA_INDEX
)

class EtherRegistry:
    """Registry to track and process classes with Ether methods"""
    _instance = None
    _pending_classes: dict[str, str] = {}  # qualname -> module_name
    _processed_classes: Set[str] = set()
    _logger = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EtherRegistry, cls).__new__(cls)
            cls._instance._logger = _get_logger("EtherRegistry")  # Will use default levels
        return cls._instance
    
    def mark_for_processing(self, class_qualname: str, module_name: str):
        
        if class_qualname not in self._pending_classes:
            self._logger.debug(f"Marking class for processing: {class_qualname} from {module_name}")
            self._pending_classes[class_qualname] = module_name

        
    def process_registry_config(self, config: Dict[str, EtherClassConfig]):
        """Process registry configuration and apply decorators
        
        Args:
            config: Dictionary mapping class paths to their configurations
        """
        self._logger.debug("Processing registry configuration...")
        
        for class_path, class_config in config.items():
            # Import the class
            module_path, class_name = class_path.rsplit('.', 1)
            try:
                module = importlib.import_module(module_path)
                target_class = getattr(module, class_name)
            except (ImportError, AttributeError) as e:
                self._logger.error(f"Failed to import {class_path}: {e}")
                continue
            
            self._logger.debug(f"Processing class {class_path}")
            
            # Process each method
            for method_name, method_config in class_config.methods.items():
                if not hasattr(target_class, method_name):
                    self._logger.warning(f"Method {method_name} not found in {class_path}")
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
                self._logger.debug(f"Applied decorators to {class_path}.{method_name}")
            
            # Mark class for Ether functionality
            self.mark_for_processing(class_name, module_path)
    
    def process_pending_classes(self):
        self._logger.debug("Processing pending classes...")
        self._logger.debug(f"Pending classes: {self._pending_classes}")
        self._logger.debug(f"Processed classes: {self._processed_classes}")
        
        for qualname, module_name in list(self._pending_classes.items()):  # Create a copy of items to modify dict
            if qualname in self._processed_classes:
                self._logger.debug(f"Class {qualname} already processed, skipping")
                continue
                
            # Import the module that contains the class
            module = sys.modules.get(module_name)
            if module and hasattr(module, qualname):
                class_obj = getattr(module, qualname)
                self._logger.debug(f"Adding Ether functionality to {qualname}")
                add_ether_functionality(class_obj)
                self._processed_classes.add(qualname)
                self._logger.debug(f"Successfully processed {qualname}")
            else:
                self._logger.warning(f"Could not find class {qualname} in module {module_name}")

def add_ether_functionality(cls):
    """Adds Ether functionality directly to a class"""
    # Use the class name for logging
    logger = _get_logger(cls.__name__)  # Will use default levels
    logger.debug(f"Adding Ether functionality to class: {cls.__name__}")
    
    # Check if already processed
    if hasattr(cls, '_ether_methods_info'):
        logger.debug(f"Class {cls.__name__} already has Ether functionality")
        return cls
    
    # Collect Ether methods
    ether_methods = {
        name: method for name, method in cls.__dict__.items()
        if hasattr(method, '_pub_metadata') or 
           hasattr(method, '_sub_metadata') or
           hasattr(method, '_get_metadata')  # Add get methods
    }
    logger.debug(f"Found {len(ether_methods)} Ether methods in {cls.__name__}")
    
    # Store Ether method information (even if empty)
    cls._ether_methods_info = ether_methods
    
    # Add core attributes
    def init_ether_vars(self, name=None, log_level=logging.INFO):
        self.id = str(uuid.uuid4())
        self.name = name or self.id
        # Pass log_level as both console and file level if specified
        self._logger = _get_logger(
            process_name=self.__class__.__name__,
            instance_name=self.name,
            # console_level=log_level,
            # file_level=log_level
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
        
        # Add reqrep worker socket
        self._worker_socket = None
        self._worker_metadata = {}
        
        # Register get methods
        for method in self._ether_methods_info.values():
            if hasattr(method, '_get_metadata'):
                metadata = method._get_metadata
                self._worker_metadata[metadata.service_name] = metadata
    
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
        
        # Setup worker socket if we have get methods
        has_get_method = any(hasattr(m, '_get_metadata') 
                           for m in self._ether_methods_info.values())
        
        if has_get_method:
            self._logger.debug("Setting up worker socket")
            self._worker_socket = self._zmq_context.socket(zmq.DEALER)
            self._worker_socket.connect("tcp://localhost:5560")
            self._worker_socket.setsockopt(zmq.RCVTIMEO, 1000)
            
            # Register all services
            for metadata in self._worker_metadata.values():
                service_name = metadata.service_name.encode()
                self._logger.debug(f"Registering service: {service_name}")
                self._worker_socket.send_multipart([
                    b'',
                    MDPW_WORKER,
                    W_READY,
                    service_name
                ])
                self._logger.debug(f"Service registered: {service_name}")
    
    def _handle_subscriber_message(self, timeout=1000):
        """Handle a message from the subscriber socket"""
        if self._sub_socket and self._sub_socket.poll(timeout=timeout):
            message = self._sub_socket.recv_multipart()
            topic = message[0].decode()
            data = json.loads(message[1].decode())
            
            self._logger.debug(f"Received message: topic={topic}, data={data}")
            
            if topic in self._sub_topics:
                metadata = self._sub_metadata.get(topic)
                if metadata:
                    # Get the method's signature parameters
                    sig = inspect.signature(metadata.func)
                    valid_params = sig.parameters.keys()
                    
                    # Remove 'self' from valid params if present
                    if 'self' in valid_params:
                        valid_params = [p for p in valid_params if p != 'self']
                    
                    # If using root model, pass the entire data as 'root'
                    if isinstance(metadata.args_model, type) and issubclass(metadata.args_model, RootModel):
                        args = {'root': metadata.args_model(data).root}
                    else:
                        # Validate data with the model
                        model_instance = metadata.args_model(**data)
                        validated_data = model_instance.model_dump()
                        args = {k: v for k, v in validated_data.items() if k in valid_params}
                    
                    metadata.func(self, **args)

    def _handle_worker_message(self, timeout=1000):
        """Handle a message from the worker socket"""
        if self._worker_socket and self._worker_socket.poll(timeout=timeout):
            msg = self._worker_socket.recv_multipart()
            self._logger.debug(f"Received worker message: {msg}")
            if msg[REQUEST_WORKER_INDEX] == MDPW_WORKER and msg[REQUEST_COMMAND_INDEX] == W_REQUEST:
                client_id = msg[REQUEST_CLIENT_ID_INDEX]
                service_name = msg[4].decode()  # Get service name from message
                request = json.loads(msg[5].decode())  # Request data now at index 5
                
                self._logger.debug(f"Handling request for service: {service_name}")
                
                metadata = self._worker_metadata.get(service_name)
                if metadata:
                    try:
                        # Get the method's signature parameters
                        sig = inspect.signature(metadata.func)
                        valid_params = sig.parameters.keys()
                        
                        # Remove 'self' from valid params if present
                        if 'self' in valid_params:
                            valid_params = [p for p in valid_params if p != 'self']
                        
                        self._logger.debug(f"Validating request parameters for {service_name}")
                        
                        # Validate request parameters using the model
                        if isinstance(metadata.args_model, type) and issubclass(metadata.args_model, RootModel):
                            args = {'root': metadata.args_model(request["params"]).root}
                        else:
                            model_instance = metadata.args_model(**request.get("params", {}))
                            validated_data = model_instance.model_dump()
                            args = {k: v for k, v in validated_data.items() if k in valid_params}
                        
                        self._logger.debug(f"Executing {service_name} with args: {args}")
                        
                        # Call function with validated parameters
                        result = metadata.func(self, **args)
                        reply_data = {
                            "result": result,
                            "status": "success"
                        }
                        self._logger.debug(f"Successfully executed {service_name}")
                        
                    except ValidationError as e:
                        self._logger.error(f"Validation error in {service_name}: {str(e)}")
                        reply_data = {
                            "error": f"Invalid parameters: {str(e)}",
                            "status": "error"
                        }
                    except Exception as e:
                        self._logger.error(f"Error executing {service_name}: {str(e)}")
                        reply_data = {
                            "error": str(e),
                            "status": "error"
                        }
                        
                    self._logger.debug(f"Sending reply for {service_name}")
                    self._worker_socket.send_multipart([
                        b'',
                        MDPW_WORKER,
                        W_REPLY,
                        service_name.encode(),
                        client_id,
                        json.dumps(reply_data).encode()
                    ])

        
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


    def run(self):
        last_refresh = 0
        while True:
            try:
                # Refresh TTL periodically
                now = time.time()
                if now - last_refresh >= (self._instance_tracker.ttl / 2):
                    self._instance_tracker.refresh_instance(self.id)
                    last_refresh = now
                
                # Create a poller to handle both sub and worker sockets
                poller = zmq.Poller()
                if self._sub_socket:
                    poller.register(self._sub_socket, zmq.POLLIN)
                if self._worker_socket:
                    poller.register(self._worker_socket, zmq.POLLIN)
                
                # Poll for messages with timeout
                sockets = dict(poller.poll(timeout=1000))
                
                # Handle messages based on socket type
                if self._sub_socket in sockets:
                    self._handle_subscriber_message(timeout=0)
                if self._worker_socket in sockets:
                    self._handle_worker_message(timeout=0)
                
            except Exception as e:
                self._logger.error(f"Error in run loop: {e}", exc_info=True)
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
        if hasattr(self, '_worker_socket') and self._worker_socket:
            self._worker_socket.close()
    
    # Add methods to class
    cls.init_ether = init_ether_vars
    cls.setup_sockets = setup_sockets
    cls._handle_subscriber_message = _handle_subscriber_message
    cls._handle_worker_message = _handle_worker_message
    cls.track_message = track_message
    cls.save_results = save_results
    cls.run = run
    cls.cleanup = cleanup
    
    # Modify __init__ to initialize attributes
    original_init = cls.__init__
    def new_init(self, *args, **kwargs):
        # Initialize Ether functionality first
        self.init_ether(
            name=kwargs.pop('name', None),
            log_level=kwargs.pop('log_level', logging.DEBUG), # TODO: use ether global log level
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
    
    # If no fields (other than self), create a model with a non-underscore field
    if not fields:
        fields = {"data": (dict, {})}
    
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

class EtherRequestMetadata:
    def __init__(self, func, service_name, args_model: Type[BaseModel]):
        self.func = func
        self.service_name = service_name
        self.args_model = args_model

def _ether_pub(topic: Optional[str] = None):
    """Decorator for methods that should publish messages."""
    def decorator(func):
        # Get return type hint if it exists
        sig = inspect.signature(func)
        return_type = sig.return_annotation
        
        # If no return type specified, use dict as default
        if return_type == inspect.Parameter.empty:
            return_type = dict
        # Handle None return type (specified as None or type(None))
        elif return_type in (None, type(None)):
            return_type = dict
        
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Execute the function and get result
            result = func(self, *args, **kwargs)
            try:
                self._logger.debug(f"Inside pub wrapper for {func.__name__}")
                if not hasattr(self, '_pub_socket'):
                    raise RuntimeError("Cannot publish: no publisher socket configured")
                
                # Handle None result
                if result is None:
                    result = {}
                
                # Validate and serialize result
                if isinstance(return_type, type) and issubclass(return_type, BaseModel):
                    validated_result = return_type(**result).model_dump_json()
                else:
                    ResultModel = RootModel[return_type]
                    validated_result = ResultModel(result).model_dump_json()
                
                actual_topic = topic or f"{func.__qualname__}"
                self._logger.debug(f"Publishing to topic: {actual_topic}")
                
                self._pub_socket.send_multipart([
                    actual_topic.encode(),
                    validated_result.encode()
                ])
            except Exception as e:
                # we never want to crash basic operation of the underlying user code
                pass
            
            return result
        
        # Create and attach the metadata
        actual_topic = topic or f"{func.__qualname__}"
        wrapper._pub_metadata = EtherPubMetadata(func, actual_topic)
        
        # Mark the containing class for Ether processing
        frame = inspect.currentframe().f_back
        while frame:
            locals_dict = frame.f_locals
            if '__module__' in locals_dict and '__qualname__' in locals_dict:
                EtherRegistry().mark_for_processing(
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
                EtherRegistry().mark_for_processing(
                    locals_dict['__qualname__'],
                    locals_dict['__module__']
                )
                break
            frame = frame.f_back
        
        return wrapper
    return decorator

def _ether_get(func):
    """Decorator for methods that should handle get requests"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    
    # Create model from function signature
    args_model = _create_model_from_signature(func)
    
    # Create and attach metadata
    wrapper._get_metadata = EtherRequestMetadata(
        func=func,
        service_name=f"{func.__qualname__}.get",
        args_model=args_model
    )
    
    # Mark the containing class for Ether processing
    frame = inspect.currentframe().f_back
    while frame:
        locals_dict = frame.f_locals
        if '__module__' in locals_dict and '__qualname__' in locals_dict:
            EtherRegistry().mark_for_processing(
                locals_dict['__qualname__'],
                locals_dict['__module__']
            )
            break
        frame = frame.f_back
    
    return wrapper

def _ether_save(func):
    """Decorator for methods that handle save requests"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Execute the function and get result
            result = func(*args, **kwargs)
            return {
                "status": "success",
                "result": result,
                "message": "Save operation completed successfully"
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "message": "Save operation failed"
            }
    
    # Create model from function signature
    args_model = _create_model_from_signature(func)
    
    # Create and attach metadata
    wrapper._get_metadata = EtherRequestMetadata(
        func=wrapper,  # Use wrapper to get error handling
        service_name=f"{func.__qualname__}.save",  # Use .save suffix
        args_model=args_model
    )
    
    # Mark the containing class for Ether processing
    frame = inspect.currentframe().f_back
    while frame:
        locals_dict = frame.f_locals
        if '__module__' in locals_dict and '__qualname__' in locals_dict:
            EtherRegistry().mark_for_processing(
                locals_dict['__qualname__'],
                locals_dict['__module__']
            )
            break
        frame = frame.f_back
    
    return wrapper





