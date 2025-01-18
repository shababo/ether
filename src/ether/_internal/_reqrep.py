import zmq
import uuid
import json
from typing import Dict, Optional
import time

from ..utils import _get_logger

# Constants for MDP protocol
WORKER_READY = b"\x01"  # Worker tells broker it's ready
REQUEST = b"\x02"       # Client request to broker
REPLY = b"\x03"        # Worker reply to broker

class Service:
    """Represents a service and its associated workers"""
    def __init__(self, name: str):
        self.name = name
        self.workers: Dict[str, 'Worker'] = {}  # worker_id -> Worker
        self.requests = []  # List of pending requests
        
class Worker:
    """Represents a worker instance"""
    def __init__(self, id: str, service: str, address: bytes):
        self.id = id
        self.service = service
        self.address = address
        self.expiry = time.time() + 5  # 5 second timeout

class EtherReqRepBroker:
    """Broker implementing Majordomo pattern for request-reply messaging"""
    def __init__(self, frontend_port: int = 5559, backend_port: int = 5560):
        self.id = str(uuid.uuid4())
        self._logger = _get_logger("EtherReqRepBroker")
        self._logger.debug("Initializing ReqRep broker")
        
        self.frontend_port = frontend_port
        self.backend_port = backend_port
        self.services: Dict[str, Service] = {}  # service_name -> Service
        self.workers: Dict[str, Worker] = {}    # worker_id -> Worker
        self._setup_sockets()
        
    def _setup_sockets(self):
        """Setup ROUTER sockets for both frontend and backend"""
        self._logger.debug("Setting up sockets")
        self.context = zmq.Context()
        
        # Socket for clients
        self.frontend = self.context.socket(zmq.ROUTER)
        self.frontend.bind(f"tcp://*:{self.frontend_port}")
        
        # Socket for workers
        self.backend = self.context.socket(zmq.ROUTER)
        self.backend.bind(f"tcp://*:{self.backend_port}")
        
        self.poller = zmq.Poller()
        self.poller.register(self.frontend, zmq.POLLIN)
        self.poller.register(self.backend, zmq.POLLIN)
        
    def _process_client(self, sender: bytes, empty: bytes, msg: list):
        """Process a client request"""
        assert empty == b''
        service_name = msg[0].decode()
        request = msg[1]
        
        service = self.services.get(service_name)
        if not service:
            service = Service(service_name)
            self.services[service_name] = service
            
        # Queue the request
        service.requests.append((sender, request))
        self._logger.debug(f"Client request for service: {service_name}")
        self._dispatch_requests(service)
        
    def _process_worker(self, sender: bytes, empty: bytes, msg: list):
        """Process a worker message"""
        assert empty == b''
        command = msg[0]
        service_name = msg[1].decode()
        
        worker_id = sender.hex()
        service = self.services.get(service_name)
        
        if command == WORKER_READY:
            # New worker registration
            if not service:
                service = Service(service_name)
                self.services[service_name] = service
                
            worker = Worker(worker_id, service_name, sender)
            self.workers[worker_id] = worker
            service.workers[worker_id] = worker
            self._logger.debug(f"Worker registered for service: {service_name}")
            
        elif command == REPLY:
            # Worker reply to previous request
            if worker_id in self.workers:
                reply = msg[2]
                client = msg[3]  # Original client address
                self.frontend.send_multipart([client, b'', reply])
                self._logger.debug(f"Worker reply sent to client for service: {service_name}")
                self._dispatch_requests(service)  # Process any pending requests
                
    def _dispatch_requests(self, service: Service):
        """Attempt to dispatch pending requests to available workers"""
        if not service.requests:
            return
            
        # Find available workers
        available_workers = [w for w in service.workers.values() 
                           if time.time() < w.expiry]
        
        while service.requests and available_workers:
            client, request = service.requests.pop(0)
            worker = available_workers.pop(0)
            
            # Update worker expiry
            worker.expiry = time.time() + 5
            
            # Forward request to worker
            self.backend.send_multipart([
                worker.address,
                b'',
                REQUEST,
                client,
                request
            ])
            self._logger.debug(f"Request dispatched to worker for service: {service.name}")
            
    def run(self):
        """Main broker loop"""
        self._logger.info("Starting broker loop")
        try:
            while True:
                events = dict(self.poller.poll(timeout=1000))
                
                # Process client requests
                if self.frontend in events:
                    msg = self.frontend.recv_multipart()
                    self._process_client(msg[0], msg[1], msg[2:])
                    
                # Process worker messages
                if self.backend in events:
                    msg = self.backend.recv_multipart()
                    self._process_worker(msg[0], msg[1], msg[2:])
                    
                # Clean up expired workers
                now = time.time()
                for service in self.services.values():
                    expired = [wid for wid, w in service.workers.items() 
                             if w.expiry < now]
                    for wid in expired:
                        worker = service.workers.pop(wid)
                        self.workers.pop(worker.id, None)
                        self._logger.debug(f"Removed expired worker from service: {service.name}")
                        
        except Exception as e:
            self._logger.error(f"Broker error: {e}")
        finally:
            self.cleanup()
            
    def cleanup(self):
        """Clean up broker resources"""
        self._logger.debug("Cleaning up broker")
        if hasattr(self, 'frontend'):
            self.frontend.close()
        if hasattr(self, 'backend'):
            self.backend.close()
        if hasattr(self, 'context'):
            self.context.term() 