from functools import wraps
import inspect
from typing import Dict, Any, Type, Optional
import zmq
from pydantic import BaseModel, create_model

class ZMQMethodMetadata:
    """Holds metadata about ZMQ-decorated methods"""
    def __init__(self, func, topic: str, args_model: Type[BaseModel]):
        self.func = func
        self.topic = topic
        self.args_model = args_model

class ZMQReceiverMixin:
    """Mixin that handles ZMQ subscription and message dispatching"""
    
    def __init__(self, zmq_address: str = "tcp://localhost:5555"):
        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.SUB)
        self._zmq_socket.connect(zmq_address)
        
        # Find all ZMQ-decorated methods and subscribe to their topics
        self._zmq_methods: Dict[str, ZMQMethodMetadata] = {}
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if hasattr(attr, '_zmq_metadata'):
                metadata: ZMQMethodMetadata = attr._zmq_metadata
                self._zmq_socket.subscribe(metadata.topic.encode())
                self._zmq_methods[metadata.topic] = metadata

    def receive_messages(self):
        """Main message receiving loop"""
        while True:
            try:
                topic = self._zmq_socket.recv_string()
                data = self._zmq_socket.recv_json()
                
                if topic in self._zmq_methods:
                    metadata = self._zmq_methods[topic]
                    # Parse and validate arguments
                    args = metadata.args_model(**data)
                    # Call the method
                    metadata.func(self, **args.dict())
                    
            except Exception as e:
                print(f"Error processing message: {e}")

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


# Example usage:
class MyService(ZMQReceiverMixin):
    def __init__(self):
        super().__init__(zmq_address="tcp://localhost:5555")
    
    @zmq_method()
    def process_data(self, name: str, count: int = 0):
        print(f"Processing {name} with count {count}")
    
    @zmq_method(topic="custom.topic")
    def custom_process(self, data: dict):
        print(f"Custom processing: {data}")

# Example usage in different processes:

# Process 1 - Service
def run_service():
    service = MyService()
    service.receive_messages()  # Blocks and processes messages

# Process 2 - Publisher
def send_message():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5555")
    
    # Send message to process_data
    socket.send_multipart([
        b"__main__.MyService.process_data",
        b'{"name": "test", "count": 42}'
    ])
    
    # Send message to custom_process
    socket.send_multipart([
        b"custom.topic",
        b'{"data": {"key": "value"}}'
    ])