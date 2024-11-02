from functools import wraps
from multiprocessing import Event
import inspect
from typing import Dict, Any, Type, Optional
import uuid
import zmq
from pydantic import BaseModel, create_model
import signal
import logging
import time

def get_logger(process_name):
    logger = logging.getLogger(process_name)
    logger.setLevel(logging.INFO)
    
    # Create console handler with a custom formatter
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

class ZMQMethodMetadata:
    """Holds metadata about ZMQ-decorated methods"""
    def __init__(self, func, topic: str, args_model: Type[BaseModel]):
        self.func = func
        self.topic = topic
        self.args_model = args_model

class ZMQReceiverMixin:
    """Mixin that handles ZMQ subscription and message dispatching"""
    
    def __init__(self, name: str = None, zmq_address: str = "tcp://localhost:5555"):
        self.id = uuid.uuid4()
        self.name = name or self.id
        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.SUB)
        self._zmq_socket.connect(zmq_address)
        self._logger = get_logger(f"{self.__class__.__name__}:{self.name}")
        
        # Find all ZMQ-decorated methods and subscribe to their topics
        self._zmq_methods: Dict[str, ZMQMethodMetadata] = {}
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if hasattr(attr, '_zmq_metadata'):
                metadata: ZMQMethodMetadata = attr._zmq_metadata
                self._zmq_socket.subscribe(metadata.topic.encode())
                self._logger.info(f"Subscribed to topic: {metadata.topic}")
                self._zmq_methods[metadata.topic] = metadata
        
        # Add a small delay to ensure connection is established
        time.sleep(0.1)

    def run(self, stop_event: Event): #type: ignore
        """Main run loop with graceful shutdown support"""
        def handle_signal(signum, frame):
            stop_event.set()
        
        signal.signal(signal.SIGTERM, handle_signal)
        
        while not stop_event.is_set():
            try:
                self.receive_single_message()
            except Exception as e:
                self._logger.error(f"Error: {e}")
                break

    def receive_single_message(self, timeout=1000):
        """Handle a single message with timeout"""
        # self._logger.info(f"Polling...")
        if self._zmq_socket.poll(timeout):
            topic = self._zmq_socket.recv_string()
            data = self._zmq_socket.recv_json()
            
            self._logger.info(f"Received message - Topic: {topic}, Data: {data}")
            
            if topic in self._zmq_methods:
                metadata = self._zmq_methods[topic]
                args = metadata.args_model(**data)
                metadata.func(self, **args.dict())
            else:
                self._logger.warning(f"Received message for unknown topic: {topic}")

    def __del__(self):
        """Cleanup ZMQ resources"""
        self._zmq_socket.close()
        self._zmq_context.term()

def create_model_from_signature(func) -> Type[BaseModel]:
    """Creates a Pydantic model from a function's signature"""
    sig = inspect.signature(func)
    fields = {}
    
    for name, param in sig.parameters.items():
        if name == 'self':
            continue
        
        annotation = param.annotation if param.annotation != inspect.Parameter.empty else Any
        default = param.default if param.default != inspect.Parameter.empty else ...
        
        fields[name] = (annotation, default)
    
    model_name = f"{func.__name__}Args"
    return create_model(model_name, **fields)

def zmq_method(topic: Optional[str] = None):
    """
    Decorator for methods that should receive ZMQ messages.
    """
    def decorator(func):
        # Create Pydantic model for arguments
        args_model = create_model_from_signature(func)
        
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        
        # Generate topic if not provided
        actual_topic = topic or f"{func.__module__}.{func.__qualname__}"
        
        # Store metadata
        wrapper._zmq_metadata = ZMQMethodMetadata(func, actual_topic, args_model)
        
        return wrapper
    return decorator


