from typing import Set, Dict, Type
import inspect
import zmq
import sys
import logging
import uuid
import time
import signal
from threading import Event
from pydantic import RootModel, BaseModel
import json

from ._utils import _get_logger, _ETHER_SUB_PORT, _ETHER_PUB_PORT

def add_ether_functionality(cls):
    """Adds Ether functionality directly to a class"""
    # Collect Ether methods
    ether_methods = {
        name: method for name, method in cls.__dict__.items()
        if hasattr(method, '_pub_metadata') or hasattr(method, '_sub_metadata')
    }
    
    if not ether_methods:
        return cls
    
    # Store Ether method information
    cls._ether_methods_info = ether_methods
    
    # Add core attributes
    def init_ether_vars(self, name=None, log_level=logging.INFO):
        self.id = uuid.uuid4()
        self.name = name or self.id
        self._logger = _get_logger(f"{self.__class__.__name__}:{self.name}", log_level)
        self._sub_address = f"tcp://localhost:{_ETHER_SUB_PORT}"
        self._pub_address = f"tcp://localhost:{_ETHER_PUB_PORT}"
        
        # Socket handling
        self._zmq_context = zmq.Context()
        self._sub_socket = None
        self._pub_socket = None
        
        # Message tracking
        self.received_messages = set()
        self.latencies = []
        self.publishers = {}
        self.first_message_time = None
        self.last_message_time = None
        self.subscription_time = None
        self.results_file = None
    
    def setup_sockets(self):
        print("SETUP SOCKETS")
        print(f"Setting up sockets for {self.name}")

        has_sub_method = False
        has_pub_method = False
        for method in self._ether_methods_info.values():
            if hasattr(method, '_sub_metadata'):
                has_sub_method = True
            if hasattr(method, '_pub_metadata'):
                has_pub_method = True

        if hasattr(self, '_sub_address') and has_sub_method:
            print("SUB ADDRESS")
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
                    topic = method._sub_metadata.topic
                    self._sub_socket.subscribe(topic.encode())
                    print(f"Subscribed to topic: {topic}")
        
        if hasattr(self, '_pub_address') and has_pub_method:
            print("PUB ADDRESS")
            self._pub_socket = self._zmq_context.socket(zmq.PUB)
            if self._pub_address.startswith("tcp://*:"):
                self._pub_socket.bind(self._pub_address)
            else:
                self._pub_socket.connect(self._pub_address)
            self._pub_socket.setsockopt(zmq.SNDHWM, 1000000)
            self._pub_socket.setsockopt(zmq.SNDBUF, 65536)
        
        time.sleep(1.0)
    
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
                        print(f"Error processing data: {e}")
                        raise
                
                metadata.func(self, **args)
            else:
                print(f"Received message for unknown topic: {topic}")

    
    # Add run method
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
                print(f"Error in run loop: {e}")
                break
        
        self.save_results()
        self.cleanup()
    
    # Add cleanup
    def cleanup(self):
        if hasattr(self, '_sub_socket'):
            self._sub_socket.close()
        if hasattr(self, '_pub_socket'):
            self._pub_socket.close()
        if hasattr(self, '_zmq_context'):
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
        print("NEW INIT")
        self.init_ether(
            name=kwargs.pop('name', None),
            log_level=kwargs.pop('log_level', logging.INFO)
        )
        original_init(self, *args, **kwargs)
        self.setup_sockets()
    cls.__init__ = new_init
    
    # Add cleanup on deletion
    def new_del(self):
        self.cleanup()
    cls.__del__ = new_del
    
    return cls

class EtherRegistry:
    """Registry to track and process classes with Ether methods"""
    _pending_classes: Dict[str, str] = {}  # qualname -> module_name
    _processed_classes: Set[str] = set()
    
    @classmethod
    def mark_for_processing(cls, class_qualname: str, module_name: str):
        cls._pending_classes[class_qualname] = module_name
    
    @classmethod
    def process_pending_classes(cls):
        logger = logging.getLogger("EtherRegistry")
        logger.info("Processing pending classes...")
        
        for qualname, module_name in cls._pending_classes.items():
            if qualname in cls._processed_classes:
                logger.debug(f"Class {qualname} already processed, skipping")
                continue
                
            # Import the module that contains the class
            module = sys.modules.get(module_name)
            if module and hasattr(module, qualname):
                class_obj = getattr(module, qualname)
                logger.info(f"Adding Ether functionality to {qualname}")
                add_ether_functionality(class_obj)
                cls._processed_classes.add(qualname)
                logger.info(f"Successfully processed {qualname}")
            else:
                logger.warning(f"Could not find class {qualname} in module {module_name}")