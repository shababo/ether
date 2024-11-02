from functools import wraps
from multiprocessing import Event
import inspect
from typing import Dict, Any, Type, Optional
import uuid
import zmq
from pydantic import BaseModel, create_model, RootModel
import signal
import logging
import time

def get_logger(process_name, log_level=logging.INFO):
    """Get or create a logger with a single handler"""
    logger = logging.getLogger(process_name)
    logger.setLevel(log_level)
    logger.propagate = True
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add a single handler
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

class EtherMethodMetadata:
    """Holds metadata about ZMQ-decorated methods"""
    def __init__(self, func, topic: str, args_model: Optional[Type[BaseModel]], method_type: str):
        self.func = func
        self.topic = topic
        self.args_model = args_model
        self.method_type = method_type

class EtherMixin:
    def __init__(self, name: str = None, sub_address: Optional[str] = None, 
                 pub_address: Optional[str] = None, log_level: int = logging.INFO):
        self.id = uuid.uuid4()
        self.name = name or self.id
        self._logger = get_logger(f"{self.__class__.__name__}:{self.name}", log_level)
        self._sub_address = sub_address
        self._pub_address = pub_address
        self._zmq_context = None
        self._sub_socket = None
        self._pub_socket = None
        self._zmq_methods = {}

    def setup_sockets(self):
        self._zmq_context = zmq.Context()
        
        if self._sub_address:
            self._sub_socket = self._zmq_context.socket(zmq.SUB)
            if self._sub_address.startswith("tcp://*:"):
                self._sub_socket.bind(self._sub_address)
            else:
                self._sub_socket.connect(self._sub_address)
        
        if self._pub_address:
            self._pub_socket = self._zmq_context.socket(zmq.PUB)
            if self._pub_address.startswith("tcp://*:"):
                self._pub_socket.bind(self._pub_address)
            else:
                self._pub_socket.connect(self._pub_address)
        
        self._zmq_methods = {}
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if hasattr(attr, '_zmq_metadata'):
                metadata: EtherMethodMetadata = attr._zmq_metadata
                if metadata.method_type == 'sub' and self._sub_socket:
                    topic = metadata.topic
                    self._sub_socket.subscribe(topic.encode())
                    self._logger.info(f"Subscribed to topic: {topic}")
                    self._zmq_methods[topic] = metadata
        
        time.sleep(0.1)

    def run(self, stop_event: Event):
        def handle_signal(signum, frame):
            stop_event.set()
        
        signal.signal(signal.SIGTERM, handle_signal)
        self.setup_sockets()
        
        while not stop_event.is_set():
            try:
                if self._sub_socket:
                    self.receive_single_message()
            except Exception as e:
                self._logger.error(f"Error in run loop: {e}")
                break
        
        self.cleanup()

    def cleanup(self):
        if self._sub_socket:
            self._sub_socket.close()
        if self._pub_socket:
            self._pub_socket.close()
        if self._zmq_context:
            self._zmq_context.term()

    def __del__(self):
        self.cleanup()

    def receive_single_message(self, timeout=1000):
        if self._sub_socket and self._sub_socket.poll(timeout):
            topic = self._sub_socket.recv_string()
            data = self._sub_socket.recv_json()
            
            if topic in self._zmq_methods:
                metadata = self._zmq_methods[topic]
                
                if isinstance(metadata.args_model, type) and issubclass(metadata.args_model, RootModel):
                    args = {'root': metadata.args_model(data).root}
                else:
                    try:
                        model_instance = metadata.args_model(**data)
                        args = model_instance.model_dump()
                    except Exception as e:
                        self._logger.error(f"Error processing data: {e}")
                        raise
                
                metadata.func(self, **args)
            else:
                self._logger.warning(f"Received message for unknown topic: {topic}")

def create_model_from_signature(func) -> Type[BaseModel]:
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

def ether_pub(topic: Optional[str] = None):
    """
    Decorator for methods that should publish ZMQ messages.
    """
    def decorator(func):
        # Get return type hint if it exists
        return_type = inspect.signature(func).return_annotation
        if return_type == inspect.Signature.empty:
            raise TypeError(f"Function {func.__name__} must have a return type hint")
        
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self._pub_socket:
                raise RuntimeError("Cannot publish: no publisher socket configured")
            
            # Execute the function and get result
            result = func(self, *args, **kwargs)
            
            # Validate result against return type
            if isinstance(return_type, type) and issubclass(return_type, BaseModel):
                validated_result = return_type(**result).model_dump_json()
            else:
                ResultModel = RootModel[return_type]
                validated_result = ResultModel(result).model_dump_json()
            
            # Generate topic if not provided
            actual_topic = topic or f"{func.__module__}.{func.__qualname__}"
            
            self._logger.debug(f"Publishing to topic: {actual_topic}")
            
            # Publish the validated result
            self._pub_socket.send_multipart([
                actual_topic.encode(),
                validated_result.encode()
            ])
            
            return result
        
        # Store metadata and log it
        actual_topic = topic or f"{func.__module__}.{func.__qualname__}"
        wrapper._zmq_metadata = EtherMethodMetadata(
            func, 
            actual_topic,
            None,
            'pub'
        )
        
        return wrapper
    return decorator

def ether_sub(topic: Optional[str] = None):
    """
    Decorator for methods that should receive ZMQ messages.
    """
    def decorator(func):
        # Create Pydantic model for arguments
        args_model = create_model_from_signature(func)
        
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        
        # Store metadata
        wrapper._zmq_metadata = EtherMethodMetadata(
            func, 
            topic or f"{func.__module__}.{func.__qualname__}", 
            args_model,
            'sub'
        )
        
        return wrapper
    return decorator


