import zmq
import time
import uuid
import json
from typing import Dict, Optional
from ..utils import get_ether_logger

# MDP protocol constants
MDPW_WORKER = b"MDPW01"  # MDP/Worker v0.1
MDPC_CLIENT = b"MDPC01"  # MDP/Client v0.1

# MDP command constants
W_READY = b"\x01"      # Worker ready
W_REQUEST = b"\x02"    # Worker request
W_REPLY = b"\x03"      # Worker reply
W_HEARTBEAT = b"\x04"  # Worker heartbeat
W_DISCONNECT = b"\x05" # Worker disconnect


REQUEST_WORKER_INDEX = 1
REQUEST_COMMAND_INDEX = 2
REQUEST_CLIENT_ID_INDEX = 3
REQUEST_SERVICE_INDEX = 4
REQUEST_DATA_INDEX = 5

REPLY_CLIENT_INDEX = 1
REPLY_SERVICE_INDEX = 2
REPLY_DATA_INDEX = 3

class Service:
    """Represents a service and its associated workers"""
    def __init__(self, name: str):
        self.name = name
        self.workers = []  # Available workers for this service (renamed from waiting)
        self.requests = [] # Pending requests for this service

class Worker:
    """Represents a worker instance"""
    def __init__(self, id: str, service: str, address: bytes, use_expiry: bool = False):
        self.id = id
        self.service = service  # Service this worker provides
        self.address = address
        self.use_expiry = use_expiry
        self.expiry = time.time() + 10 if use_expiry else None
        self.liveness = 3 if use_expiry else None

class EtherReqRepBroker:
    """Broker implementing Majordomo Protocol v0.1"""
    
    HEARTBEAT_INTERVAL = 2500  # msecs
    HEARTBEAT_LIVENESS = 3    # 3-5 is reasonable

    def __init__(self, frontend_port: int = 5559, backend_port: int = 5560):
        self.id = str(uuid.uuid4())
        self._logger = get_ether_logger("EtherReqRepBroker")
        self._logger.debug("Initializing MDP broker")
        
        self.frontend_port = frontend_port
        self.backend_port = backend_port
        self.services: Dict[str, Service] = {}  # service_name -> Service
        self.workers: Dict[str, Worker] = {}    # worker_id -> Worker
        self.heartbeat_at = time.time() + 0.001 * self.HEARTBEAT_INTERVAL
        self._setup_sockets()

    def _setup_sockets(self):
        """Setup REP/ROUTER sockets with proper options"""
        self._logger.debug("Setting up sockets")
        self.context = zmq.Context()
        
        # Socket for clients - now using REP/REQ pattern
        self.frontend = self.context.socket(zmq.REP)
        self.frontend.setsockopt(zmq.LINGER, 0)
        self.frontend.bind(f"tcp://*:{self.frontend_port}")
        
        # Socket for workers
        self.backend = self.context.socket(zmq.ROUTER)
        self.backend.setsockopt(zmq.LINGER, 0)
        self.backend.bind(f"tcp://*:{self.backend_port}")
        
        self.poller = zmq.Poller()
        self.poller.register(self.frontend, zmq.POLLIN)
        self.poller.register(self.backend, zmq.POLLIN)

    def _process_client(self, msg: list):
        """Process a client request using REP socket"""
        assert msg[0] == MDPC_CLIENT  # Verify client protocol
        service_name = msg[1].decode()  # Service name comes after protocol
        request = msg[2]  # Request data is third frame
        
        self._logger.debug(f"Processing client message: protocol={msg[0]}, service={service_name}, request={request}")
        
        # Handle disconnect command
        if W_DISCONNECT in request:
            self._logger.info(f"Processing disconnect request for service: {service_name}")
            worker_count = 0
            # Find all workers for this service and delete them
            for worker_id, worker in list(self.workers.items()):
                if worker.service == service_name:
                    self._logger.debug(f"Disconnecting worker {worker_id} for service {service_name}")
                    self._delete_worker(worker_id)
                    worker_count += 1
            
            self._logger.info(f"Disconnected {worker_count} workers for service {service_name}")
            
            # Send success response
            response = {
                "status": "success",
                "result": {
                    "status": "disconnected",
                    "workers_disconnected": worker_count
                }
            }
            self._logger.debug(f"Sending disconnect response: {response}")
            self.frontend.send_multipart([
                MDPC_CLIENT,
                service_name.encode(),
                json.dumps(response).encode()
            ])
            return
        
        service = self.services.get(service_name)
        if not service:
            service = Service(service_name)
            self.services[service_name] = service
            
        # Queue the request with proper MDP framing for worker
        service.requests.append((self.frontend, [
            MDPW_WORKER,
            W_REQUEST,
            service_name.encode(),
            request
        ]))
        self._logger.debug(f"Client request queued for service: {service_name}")
        self._dispatch_requests(service)

    def _process_worker(self, sender: bytes, empty: bytes, msg: list):
        """Process worker message using MDP"""
        assert empty == b''
        assert msg.pop(0) == MDPW_WORKER  # Protocol header
        
        command = msg.pop(0)
        worker_id = sender.hex()
        
        self._logger.debug(f"Processing worker message: worker_id={worker_id}, command={command}")

        if command == W_HEARTBEAT:
            if worker_id in self.workers:
                worker = self.workers[worker_id]
                worker.expiry = time.time() + 5
                worker.liveness = self.HEARTBEAT_LIVENESS
                self._logger.debug(f"Updated heartbeat for worker {worker_id}")
                return

        elif command == W_DISCONNECT:
            self._logger.info(f"Processing worker disconnect command from {worker_id}")
            if worker_id in self.workers:
                # Send acknowledgment before disconnecting
                self._logger.debug(f"Acknowledging disconnect from worker {worker_id}")
                service_name = msg.pop(0).decode() if msg else None
                if service_name:
                    response = {
                        "status": "success",
                        "result": {"status": "disconnected", "worker_id": worker_id}
                    }
                    self._logger.debug(f"Sending disconnect acknowledgment: {response}")
                    self.frontend.send_multipart([
                        MDPC_CLIENT,
                        service_name.encode(),
                        json.dumps(response).encode()
                    ])
                self._delete_worker(worker_id)
                self._logger.info(f"Worker {worker_id} disconnected")
                return
            else:
                self._logger.warning(f"Disconnect request from unknown worker {worker_id}")

        full_service_name = msg.pop(0).decode()
        
        service = self.services.get(full_service_name)
        if service is None and command == W_READY:
            service = Service(full_service_name)
            self.services[full_service_name] = service
            self._logger.debug(f"Created new service: {full_service_name}")

        if not service:
            self._logger.warning(f"Service {full_service_name} not found")
            return

        if command == W_READY:
            worker = Worker(worker_id, full_service_name, sender)
            self.workers[worker_id] = worker
            service.workers.append(worker)
            self._logger.debug(f"Worker registered for service: {full_service_name}")

        elif command == W_REPLY:
            if worker_id in self.workers:
                worker = self.workers[worker_id]
                client = msg.pop(0)
                reply = msg
                
                # Send reply to client with proper MDP framing
                self.frontend.send_multipart([
                    MDPC_CLIENT,           # Protocol
                    service.name.encode(), # Service name
                    reply[0]               # Reply data
                ])
                
                # Worker is now available again
                if worker.use_expiry:
                    worker.expiry = time.time() + 10
                service.workers.append(worker)
                self._dispatch_requests(service)

    def _delete_worker(self, worker_id: str):
        """Delete worker from all data structures"""
        if worker_id in self.workers:
            worker = self.workers[worker_id]
            service = self.services.get(worker.service)
            if service:
                if worker in service.workers:  # Changed from waiting to workers
                    service.workers.remove(worker)
            del self.workers[worker_id]

    def _purge_workers(self):
        """Look for and kill expired workers"""
        now = time.time()
        for worker_id, worker in list(self.workers.items()):
            if worker.use_expiry and worker.expiry and now > worker.expiry:
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
        """Dispatch requests with improved error handling"""
        while service.requests and service.workers:
        
            client_socket, request = service.requests.pop(0)
            worker = service.workers.pop(0)
            
            try:
                # Forward request to worker with proper MDP framing
                self.backend.send_multipart([
                    worker.address,
                    b'',
                    MDPW_WORKER,
                    W_REQUEST,
                    client_socket.getsockopt(zmq.IDENTITY),
                    service.name.encode(),
                    request[-1]  # The actual request data
                ])
                self._logger.debug(f"Request dispatched to worker for service: {service.name}")
                
            except zmq.error.Again:
                # Put request and worker back if send fails
                service.requests.insert(0, (client_socket, request))
                service.workers.append(worker)
                self._logger.warning("Failed to dispatch request, will retry")
                
        # If we have requests but no workers, send error to clients
        while service.requests and not service.workers:
            client_socket, request = service.requests.pop(0)
            error_reply = {
                "status": "error",
                "error": "Service not available - no workers",
                "service": service.name
            }
            try:
                client_socket.send_multipart([
                    MDPC_CLIENT,
                    service.name.encode(),
                    json.dumps(error_reply).encode()
                ])
                self._logger.warning(f"Sent service unavailable error to client for {service.name}")
            except Exception as e:
                self._logger.error(f"Failed to send error reply to client: {e}")

    def run(self):
        """Run the broker loop"""
        self._logger.info("Starting broker loop")
        
        try:
            while True:
                sockets = dict(self.poller.poll())
                self._logger.debug(f"Active sockets: {sockets}")
                events = dict(self.poller.poll(self.HEARTBEAT_INTERVAL))
                
                if self.frontend in events:
                    self._logger.debug(f"Frontend in events: {self.frontend}")
                    msg = self.frontend.recv_multipart()
                    self._logger.debug(f"Frontend resply msg: {msg}")
                    self._process_client(msg)
                    
                if self.backend in events:
                    self._logger.debug(f"Backend in events: {self.backend}")
                    msg = self.backend.recv_multipart()
                    self._logger.debug(f"Backend reply msg: {msg}")
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