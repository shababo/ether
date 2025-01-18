import zmq
import time
import uuid
from typing import Dict, Optional
from ..utils import _get_logger

# MDP protocol constants
MDPW_WORKER = b"MDPW01"  # MDP/Worker v0.1
MDPC_CLIENT = b"MDPC01"  # MDP/Client v0.1

# MDP command constants
W_READY = b"\x01"      # Worker ready
W_REQUEST = b"\x02"    # Worker request
W_REPLY = b"\x03"      # Worker reply
W_HEARTBEAT = b"\x04"  # Worker heartbeat
W_DISCONNECT = b"\x05" # Worker disconnect

class Service:
    """Represents a service and its associated workers"""
    def __init__(self, name: str):
        self.name = name
        self.waiting = []  # Available workers for this service
        self.requests = [] # Pending requests for this service

class Worker:
    """Represents a worker instance"""
    def __init__(self, id: str, service: str, address: bytes):
        self.id = id
        self.service = service  # Service this worker provides
        self.address = address
        self.expiry = time.time() + 5  # 5 second timeout
        self.liveness = 3    # 3 heartbeats missed before death

class EtherReqRepBroker:
    """Broker implementing Majordomo Protocol v0.1"""
    
    HEARTBEAT_INTERVAL = 2500  # msecs
    HEARTBEAT_LIVENESS = 3    # 3-5 is reasonable

    def __init__(self, frontend_port: int = 5559, backend_port: int = 5560):
        self.id = str(uuid.uuid4())
        self._logger = _get_logger("EtherReqRepBroker")
        self._logger.debug("Initializing MDP broker")
        
        self.frontend_port = frontend_port
        self.backend_port = backend_port
        self.services: Dict[str, Service] = {}  # service_name -> Service
        self.workers: Dict[str, Worker] = {}    # worker_id -> Worker
        self.heartbeat_at = time.time() + 0.001 * self.HEARTBEAT_INTERVAL
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
        """Process a client request using MDP"""
        assert empty == b''
        assert msg.pop(0) == MDPC_CLIENT  # Protocol header
        service_name = msg.pop(0).decode()
        request = msg  # Remaining frames are request
        
        service = self.services.get(service_name)
        if not service:
            self._logger.warning(f"Service {service_name} not found")
            return
            
        # Queue the request
        service.requests.append((sender, request))
        self._logger.debug(f"Client request queued for service: {service_name}")
        self._dispatch_requests(service)

    def _process_worker(self, sender: bytes, empty: bytes, msg: list):
        """Process worker message using MDP"""
        assert empty == b''
        assert msg.pop(0) == MDPW_WORKER  # Protocol header
        
        command = msg.pop(0)
        worker_id = sender.hex()
        service_name = msg.pop(0).decode()
        
        # Get or create the service
        service = self.services.get(service_name)
        if service is None and command == W_READY:
            # Only create service on worker registration
            service = Service(service_name)
            self.services[service_name] = service
            self._logger.debug(f"Created new service: {service_name}")

        if not service:
            self._logger.warning(f"Service {service_name} not found")
            return

        if command == W_READY:
            # Worker registering for first time
            worker = Worker(worker_id, service_name, sender)
            self.workers[worker_id] = worker
            service.waiting.append(worker)
            self._logger.debug(f"Worker registered for service: {service_name}")

        elif command == W_REPLY:
            if worker_id in self.workers:
                worker = self.workers[worker_id]
                client = msg.pop(0)  # Original client address
                reply = msg  # Remaining frames are reply
                
                # Send reply to client with MDP envelope
                self.frontend.send_multipart([
                    client, 
                    b'', 
                    MDPC_CLIENT,
                    service_name.encode(),
                    *reply
                ])
                
                # Worker is now available again
                worker.expiry = time.time() + 5
                service.waiting.append(worker)
                self._dispatch_requests(service)

        elif command == W_HEARTBEAT:
            if worker_id in self.workers:
                worker = self.workers[worker_id]
                worker.expiry = time.time() + 5
                worker.liveness = self.HEARTBEAT_LIVENESS

        elif command == W_DISCONNECT:
            self._delete_worker(worker_id)

    def _delete_worker(self, worker_id: str):
        """Delete worker from all data structures"""
        if worker_id in self.workers:
            worker = self.workers[worker_id]
            service = self.services.get(worker.service)
            if service:
                if worker in service.waiting:
                    service.waiting.remove(worker)
            del self.workers[worker_id]

    def _purge_workers(self):
        """Look for and kill expired workers"""
        now = time.time()
        for worker_id, worker in list(self.workers.items()):
            if now > worker.expiry:
                self._logger.debug(f"Deleting expired worker: {worker_id}")
                self._delete_worker(worker_id)

    def _send_heartbeats(self):
        """Send heartbeats to waiting workers if it's time"""
        if time.time() > self.heartbeat_at:
            for worker in self.workers.values():
                self.backend.send_multipart([
                    worker.address,
                    b'',
                    MDPW_WORKER,
                    W_HEARTBEAT
                ])
            self.heartbeat_at = time.time() + 0.001 * self.HEARTBEAT_INTERVAL

    def _dispatch_requests(self, service: Service):
        """Attempt to dispatch pending requests to available workers"""
        while service.requests and service.waiting:
            client, request = service.requests.pop(0)
            worker = service.waiting.pop(0)
            
            # Forward request to worker with MDP envelope
            self.backend.send_multipart([
                worker.address,
                b'',
                MDPW_WORKER,
                W_REQUEST,
                client,
                *request
            ])
            self._logger.debug(f"Request dispatched to worker for service: {service.name}")

    def run(self):
        """Main broker loop"""
        self._logger.info("Starting broker loop")
        try:
            while True:
                events = dict(self.poller.poll(self.HEARTBEAT_INTERVAL))
                
                if self.frontend in events:
                    msg = self.frontend.recv_multipart()
                    self._process_client(msg[0], msg[1], msg[2:])
                    
                if self.backend in events:
                    msg = self.backend.recv_multipart()
                    self._process_worker(msg[0], msg[1], msg[2:])
                
                self._purge_workers()
                self._send_heartbeats()
                    
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