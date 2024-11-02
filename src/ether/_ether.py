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

def get_logger(process_name):
    logger = logging.getLogger(process_name)
    logger.setLevel(logging.DEBUG)
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
        self.method_type = method_type  # 'pub' or 'sub'

class EtherMixin:
    """Mixin that handles both ZMQ publication and subscription"""
    
    def __init__(self, name: str = None, sub_address: Optional[str] = None, pub_address: Optional[str] = None):
        self.id = uuid.uuid4()
        self.name = name or self.id
        self._logger = get_logger(f"{self.__class__.__name__}:{self.name}")
        
        # Store addresses for later use
        self._sub_address = sub_address
        self._pub_address = pub_address
        
        # Initialize these in setup_sockets
        self._zmq_context = None
        self._sub_socket = None
        self._pub_socket = None
        self._zmq_methods = {}

    def setup_sockets(self):
        """Setup ZMQ sockets - called at the start of run()"""
        self._zmq_context = zmq.Context()
        
        # Initialize SUB socket if needed
        if self._sub_address:
            self._sub_socket = self._zmq_context.socket(zmq.SUB)
            self._sub_socket.connect(self._sub_address)
        
        # Initialize PUB socket if needed
        if self._pub_address:
            self._pub_socket = self._zmq_context.socket(zmq.PUB)
            if self._pub_address.startswith("tcp://*:"):
                self._pub_socket.bind(self._pub_address)
            else:
                self._pub_socket.connect(self._pub_address)
        
        # Find all ZMQ-decorated methods
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
        
        # Add a small delay to ensure connections are established
        time.sleep(0.1)

    def run(self, stop_event: Event): #type: ignore
        """Main run loop with graceful shutdown support"""
        def handle_signal(signum, frame):
            stop_event.set()
        
        signal.signal(signal.SIGTERM, handle_signal)
        
        # Setup sockets at the start of the run
        self.setup_sockets()
        
        while not stop_event.is_set():
            try:
                if self._sub_socket:
                    self.receive_single_message()
            except Exception as e:
                self._logger.error(f"Error: {e}")
                break
        
        # Cleanup when done
        self.cleanup()

    def cleanup(self):
        """Cleanup ZMQ resources"""
        if self._sub_socket:
            self._sub_socket.close()
        if self._pub_socket:
            self._pub_socket.close()
        if self._zmq_context:
            self._zmq_context.term()

    def __del__(self):
        """Cleanup ZMQ resources"""
        self.cleanup()

    def receive_single_message(self, timeout=1000):
        """Handle a single message with timeout"""
        if self._sub_socket and self._sub_socket.poll(timeout):
            self._logger.debug(f"Poll returned true for {self.name}")
            topic = self._sub_socket.recv_string()
            self._logger.debug(f"Received topic: {topic}")
            data = self._sub_socket.recv_json()
            
            self._logger.debug(f"Received raw message - Topic: {topic}, Data: {data}")
            
            if topic in self._zmq_methods:
                metadata = self._zmq_methods[topic]
                self._logger.debug(f"Found metadata for topic {topic}: {metadata.__dict__}")
                
                if isinstance(metadata.args_model, type) and issubclass(metadata.args_model, RootModel):
                    # Handle root model case
                    args = {'root': metadata.args_model(data).root}
                    self._logger.debug(f"Processed root model data: {args}")
                else:
                    # Handle regular model case
                    try:
                        model_instance = metadata.args_model(**data)
                        self._logger.debug(f"Created model instance: {model_instance}")
                        args = model_instance.model_dump()
                        self._logger.debug(f"Processed regular model data: {args}")
                    except Exception as e:
                        self._logger.error(f"Error processing data: {e}")
                        raise
                
                metadata.func(self, **args)
            else:
                self._logger.warning(f"Received message for unknown topic: {topic}")
                self._logger.debug(f"Known topics: {list(self._zmq_methods.keys())}")

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
            
            # Debug log the result
            self._logger.debug(f"Publishing function returned: {result}")
            
            # Validate result against return type
            if isinstance(return_type, type) and issubclass(return_type, BaseModel):
                # If return type is already a Pydantic model, use it directly
                validated_result = return_type(**result).model_dump_json()
                self._logger.debug(f"Validated Pydantic model result: {validated_result}")
            else:
                # Create a root model from the return type hint
                ResultModel = RootModel[return_type]
                validated_result = ResultModel(result).model_dump_json()
                self._logger.debug(f"Validated root model result: {validated_result}")
            
            # Generate topic if not provided
            actual_topic = topic or f"{func.__module__}.{func.__qualname__}"
            
            # Debug log before sending
            self._logger.debug(f"Publishing to topic '{actual_topic}': {validated_result}")
            
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


