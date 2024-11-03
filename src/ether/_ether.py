from functools import wraps
from multiprocessing import Event, Process
import inspect
from typing import Any, Type, Optional
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

# Standard ports for Ether communication
_ETHER_SUB_PORT = 5555  # subscribe to this port
_ETHER_PUB_PORT = 5556  # publish to this port

def _get_logger(process_name, log_level=logging.INFO):
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

class _ProxyManager:
    """Singleton manager for the Ether proxy process"""
    _instance = None
    _proxy_process = None
    _stop_event = None
    _pid_file = os.path.join(tempfile.gettempdir(), 'ether_proxy.pid')
    _parent_pid = None  # Add parent pid tracking
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_ProxyManager, cls).__new__(cls)
        return cls._instance
    
    def start_proxy(self):
        """Start proxy if not already running"""
        if self._proxy_process is not None:
            return
        
        # Check if proxy is already running on this machine
        if os.path.exists(self._pid_file):
            with open(self._pid_file, 'r') as f:
                pid = int(f.read())
                try:
                    # Check if process exists
                    os.kill(pid, 0)
                    return  # Proxy is already running
                except OSError:
                    # Process doesn't exist, remove stale pid file
                    os.remove(self._pid_file)
        
        # Start new proxy process
        self._stop_event = Event()
        self._proxy_process = Process(target=self._run_proxy, args=(self._stop_event,))
        self._proxy_process.daemon = True  # Make sure proxy dies with parent
        self._proxy_process.start()
        self._parent_pid = os.getpid()  # Store the creating process's PID
        
        # Save PID
        with open(self._pid_file, 'w') as f:
            f.write(str(self._proxy_process.pid))
    
    def _run_proxy(self, stop_event):
        """Run the proxy process"""
        proxy = _EtherPubSubProxy()
        proxy.run(stop_event)
    
    def stop_proxy(self):
        """Stop proxy if we started it and we're in the creating process"""
        if self._proxy_process is not None and self._parent_pid == os.getpid():
            if self._stop_event is not None:
                self._stop_event.set()
            self._proxy_process.join(timeout=5)
            if self._proxy_process.is_alive():
                self._proxy_process.terminate()
            self._proxy_process = None
            self._stop_event = None
            
            # Remove PID file if it's ours
            if os.path.exists(self._pid_file):
                with open(self._pid_file, 'r') as f:
                    try:
                        if int(f.read()) == self._proxy_process.pid:
                            os.remove(self._pid_file)
                    except (ValueError, AttributeError):
                        pass  # Ignore errors reading pid file
        elif os.path.exists(self._pid_file):
            try:
                with open(self._pid_file, 'r') as f:
                    pid = int(f.read())
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except OSError:
                        pass  # Process might already be gone
                os.remove(self._pid_file)
            except (ValueError, OSError):
                pass  # Ignore errors cleaning up pid file

# Create singleton instance
_proxy_manager = _ProxyManager()

# Register cleanup on exit
@atexit.register
def _cleanup_proxy():
    _proxy_manager.stop_proxy()

class EtherMixin:
    @classmethod
    def ensure_proxy_running(cls):
        """Ensure proxy is running. Call this once at program start."""
        _proxy_manager.start_proxy()
    
    def __init__(self, name: str = None, sub_address: Optional[str] = None, 
                 pub_address: Optional[str] = None, log_level: int = logging.INFO,
                 results_file: Optional[str] = None):
        # Remove proxy start from here
        self.id = uuid.uuid4()
        self.name = name or self.id
        self._logger = _get_logger(f"{self.__class__.__name__}:{self.name}", log_level)
        self._sub_address = sub_address or f"tcp://localhost:{_ETHER_SUB_PORT}"
        self._pub_address = pub_address or f"tcp://localhost:{_ETHER_PUB_PORT}"
        self.results_file = results_file
        
        # Socket handling
        self._zmq_context = None
        self._sub_socket = None
        self._pub_socket = None
        self._zmq_methods = {}
        
        # Message tracking
        self.received_messages = set()
        self.latencies = []
        self.publishers = {}
        self.first_message_time = None
        self.last_message_time = None
        self.subscription_time = None

        self.setup_sockets()

        
    
    def setup_sockets(self):
        self._zmq_context = zmq.Context()
        
        # Setup subscriber socket
        if self._sub_address:
            self._sub_socket = self._zmq_context.socket(zmq.SUB)
            if self._sub_address.startswith("tcp://*:"):
                self._sub_socket.bind(self._sub_address)
            else:
                self._sub_socket.connect(self._sub_address)
            self._sub_socket.setsockopt(zmq.RCVHWM, 1000000)
            self._sub_socket.setsockopt(zmq.RCVBUF, 65536)
            self.subscription_time = time.time()
            
            # Setup subscriptions using sub metadata
            self._zmq_methods = {}
            for attr_name in dir(self):
                attr = getattr(self, attr_name)
                if hasattr(attr, '_sub_metadata'):
                    metadata = attr._sub_metadata
                    topic = metadata.topic
                    self._sub_socket.subscribe(topic.encode())
                    self._logger.debug(f"Subscribed to topic: {topic}")
                    self._zmq_methods[topic] = metadata
        
        # Setup publisher socket
        if self._pub_address:
            self._pub_socket = self._zmq_context.socket(zmq.PUB)
            if self._pub_address.startswith("tcp://*:"):
                self._pub_socket.bind(self._pub_address)
            else:
                self._pub_socket.connect(self._pub_address)
            self._pub_socket.setsockopt(zmq.SNDHWM, 1000000)
            self._pub_socket.setsockopt(zmq.SNDBUF, 65536)
        
        time.sleep(0.1)
    
    def track_message(self, publisher_id: str, sequence: int, timestamp: float):
        """Track message statistics"""
        now = time.time()
        
        # Initialize publisher tracking if needed
        if publisher_id not in self.publishers:
            self.publishers[publisher_id] = {
                "sequences": set(),
                "gaps": [],
                "last_sequence": None,
                "first_time": now
            }
        
        pub_stats = self.publishers[publisher_id]
        
        # Track sequence numbers for this publisher
        if pub_stats["last_sequence"] is not None:
            expected = pub_stats["last_sequence"] + 1
            if sequence > expected:
                gap = sequence - expected
                pub_stats["gaps"].append((expected, sequence, gap))
                self._logger.debug(f"Gap from publisher {publisher_id}: "
                                 f"expected {expected}, got {sequence}")
        
        pub_stats["last_sequence"] = sequence
        pub_stats["sequences"].add(sequence)
        
        # Track timing
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
    
    def run(self, stop_event: Event):
        """Run with result saving support"""
        def handle_signal(signum, frame):
            stop_event.set()
        
        signal.signal(signal.SIGTERM, handle_signal)
        # self.setup_sockets()
        
        while not stop_event.is_set():
            try:
                if self._sub_socket:
                    self.receive_single_message()
            except Exception as e:
                self._logger.error(f"Error in run loop: {e}")
                break
        
        # Save results before cleanup if results_file is set
        self.save_results()
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

class _EtherSubMetadata:
    """Holds metadata about ZMQ subscriber methods"""
    def __init__(self, func, topic: str, args_model: Optional[Type[BaseModel]]):
        self.func = func
        self.topic = topic
        self.args_model = args_model

class _EtherPubMetadata:
    """Holds metadata about ZMQ publisher methods"""
    def __init__(self, func, topic: str):
        self.func = func
        self.topic = topic

def ether_pub(topic: Optional[str] = None):
    """Decorator for methods that should publish ZMQ messages."""
    def decorator(func):
        # Get return type hint if it exists
        return_type = inspect.signature(func).return_annotation
        if return_type == inspect.Signature.empty:
            raise TypeError(f"Function {func.__name__} must have a return type hint")
        
        # Clean up topic name if not explicitly provided
        actual_topic = topic
        if not actual_topic:
            module_name = func.__module__.replace('__mp_main__.', '').replace('__main__.', '')
            actual_topic = f"{module_name}.{func.__qualname__}"
        
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
            
            self._logger.debug(f"Publishing to topic: {actual_topic}")
            
            # Publish the validated result
            self._pub_socket.send_multipart([
                actual_topic.encode(),
                validated_result.encode()
            ])
            
            return result
        
        # Store pub metadata
        wrapper._pub_metadata = _EtherPubMetadata(func, actual_topic)
        return wrapper
    return decorator

def ether_sub(topic: Optional[str] = None):
    """Decorator for methods that should receive ZMQ messages."""
    def decorator(func):
        # Create Pydantic model for arguments
        args_model = _create_model_from_signature(func)
        
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        
        # Clean up topic name if not explicitly provided
        actual_topic = topic
        if not actual_topic:
            actual_topic = f"{func.__qualname__}"
        
        # Store sub metadata
        wrapper._sub_metadata = _EtherSubMetadata(func, actual_topic, args_model)
        return wrapper
    return decorator

# def ether_pubsub(pub_topic: Optional[str] = None, sub_topic: Optional[str] = None):
#     """Combined decorator for methods that both subscribe and publish."""
#     def decorator(func):
#         # Apply sub first, then pub
#         func = ether_sub(sub_topic)(func)
#         func = ether_pub(pub_topic)(func)
#         return func
#     return decorator

class _EtherPubSubProxy(EtherMixin):
    """Proxy that uses XPUB/XSUB sockets for efficient message distribution.
    
    XPUB/XSUB sockets are special versions of PUB/SUB that expose subscriptions
    as messages, allowing for proper subscription forwarding.
    """
    def __init__(self):
        
        self.frontend = None
        self.backend = None

        super().__init__(
            name="Proxy",
            log_level=logging.INFO
        )
        
        self._running = False

    
    def setup_sockets(self):
        """Setup XPUB/XSUB sockets with optimized settings"""
        self._zmq_context = zmq.Context()
        
        # Use standard ports
        self.frontend = self._zmq_context.socket(zmq.XSUB)
        self.frontend.bind(f"tcp://*:{_ETHER_PUB_PORT}")
        self.frontend.setsockopt(zmq.RCVHWM, 1000000)
        self.frontend.setsockopt(zmq.RCVBUF, 65536)
        
        self.backend = self._zmq_context.socket(zmq.XPUB)
        self.backend.bind(f"tcp://*:{_ETHER_SUB_PORT}")
        self.backend.setsockopt(zmq.SNDHWM, 1000000)
        self.backend.setsockopt(zmq.SNDBUF, 65536)
        self.backend.setsockopt(zmq.XPUB_VERBOSE, 1)
        
        # Set TCP keepalive options
        for socket in [self.frontend, self.backend]:
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.IMMEDIATE, 1)
            socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
            socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # Create poller to monitor both sockets
        self._poller = zmq.Poller()
        self._poller.register(self.frontend, zmq.POLLIN)
        self._poller.register(self.backend, zmq.POLLIN)
        
        self._logger.debug(f"Starting proxy with {self.frontend} and {self.backend}")  # Log socket details
            
    
    def run(self, stop_event: Event):
        """Run the proxy with graceful shutdown support"""
        def handle_signal(signum, frame):
            stop_event.set()
        
        signal.signal(signal.SIGTERM, handle_signal)
        
        try:
            
            self._running = True
            # self.setup_sockets()
            
            while self._running and not stop_event.is_set():
                try:
                    events = dict(self._poller.poll(timeout=100))  # 100ms timeout
                    
                    if self.frontend in events:
                        message = self.frontend.recv_multipart()
                        self._logger.debug(f"Proxy forwarding from frontend: {len(message)} parts")
                        self.backend.send_multipart(message)
                    
                    if self.backend in events:
                        message = self.backend.recv_multipart()
                        self._logger.debug(f"Proxy forwarding from backend: {len(message)} parts")
                        self.frontend.send_multipart(message)
                        
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN:  # Timeout, just continue
                        continue
                    else:
                        self._logger.error(f"ZMQ Error in proxy: {e}")
                        raise
                        
        except Exception as e:
            self._logger.error(f"Error in proxy: {e}")
        finally:
            self._running = False
            if self.frontend:
                self.frontend.close()
            if self.backend:
                self.backend.close()
            if self._zmq_context:
                self._zmq_context.term()


