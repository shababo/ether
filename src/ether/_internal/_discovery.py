import zmq
import uuid
import time
import os
from typing import Optional, Dict, Any
import atexit
import errno
from time import sleep

from ether.utils import get_ether_logger, get_ip_address
from ether.config import EtherSessionConfig

class _EtherDiscovery:
    DISCOVERY_PUB_PORT = 301309
    QUERY_PORT = 301310

    def __init__(self, ether_id: str = "default", session_config: Optional[EtherSessionConfig] = None):
        self.session_id = str(uuid.uuid4())
        self.ether_id = ether_id
        self.context = zmq.Context()
        self.is_host = False
        self.running = False
        self._logger = get_ether_logger(process_name="EtherDiscovery")
        
        # Use provided network config or defaults
        self.network = session_config or EtherSessionConfig()
        
        self.metadata = {
            "session_id": self.session_id,
            "ether_id": ether_id,
            "start_time": time.time(),
            "pid": os.getpid(),
            "public_ip": get_ip_address(),
            "config": self.network.model_dump()  # Add network config to metadata
        }
        
    def start(self):
        try:
            if not self._connect_to_discovery():
                self._logger.debug(f"Process {os.getpid()}: No existing service found, attempting to start one...")
                self._start_discovery_service()
        except Exception as e:
            self.cleanup()
            raise e

        atexit.register(self.cleanup)

    def _setup_sockets(self):
        self.pub_socket = self.context.socket(zmq.PUB)
        # Bind to all interfaces for remote connections
        self.pub_socket.bind(f"tcp://*:{self.network.session_discovery_port}")
        
        self.rep_socket = self.context.socket(zmq.REP)
        self.rep_socket.bind(f"tcp://*:{self.network.session_query_port}")
        
    def _start_discovery_service(self):

        if self.running:
            self._logger.debug(f"Process {os.getpid()}: Discovery service already running, skipping start")
            return
        
        try:
            self._setup_sockets()
            
            self.is_host = True
            self.running = True
            
            self._run_discovery_service()
            
        except zmq.ZMQError as e:
            if e.errno == errno.EADDRINUSE:
                self._logger.debug(f"Process {os.getpid()}: Another process just started the discovery service, attempting to connect...")
                
            else:
                self._logger.debug(f"Process {os.getpid()}: Failed to start discovery service: {e}")
            self.cleanup()

    def _run_discovery_service(self):

        self._logger.debug(f"Started discovery service on ports {self.network.session_discovery_port}, {self.network.session_query_port}")

        poller = zmq.Poller()
        poller.register(self.rep_socket, zmq.POLLIN)

        self._logger.debug(f"Running discovery service, ether_id: {self.ether_id}, metadata: {self.metadata}")
        while self.running:
            try:
                # Announce metadata
                self._logger.debug(f"Broadcasting session announcement with metadata: {self.metadata}")
                self.pub_socket.send_json({
                    "type": "announcement",
                    "data": self.metadata
                })

                # Handle queries
                events = dict(poller.poll(1000))
                if self.rep_socket in events:
                    self._logger.debug("Received query request")
                    msg = self.rep_socket.recv_json()
                    if msg.get("type") == "query":
                        self._logger.debug(f"Sending response with metadata: {self.metadata}")
                        self.rep_socket.send_json({
                            "type": "response",
                            "data": self.metadata
                        })
                    else:
                        self._logger.warning(f"Received unknown message type: {msg}")
                else:
                    self._logger.debug("No query requests received in this cycle")

            except Exception as e:
                self._logger.error(f"Error in discovery service: {e}", exc_info=True)
                break

        self._logger.debug("Discovery service loop ended")
        self.running = False  

    def _connect_to_discovery(self) -> bool:
        """Connect to an existing discovery service"""
        try:
            # Create subscriber socket
            self.sub_socket = self.context.socket(zmq.SUB)
            # Should use network.host here
            self.sub_socket.connect(f"tcp://{self.network.host}:{self.network.session_discovery_port}")
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            
            # Create request socket
            self.req_socket = self.context.socket(zmq.REQ)
            # And here
            self.req_socket.connect(f"tcp://{self.network.host}:{self.network.session_query_port}")
            
            poller = zmq.Poller()
            poller.register(self.sub_socket, zmq.POLLIN)
            
            # print(f"Process {os.getpid()}: Checking for announcements...")
            events = dict(poller.poll(200))  # reduced timeout
            if self.sub_socket in events:
                # print(f"Process {os.getpid()}: Found existing service via SUB")
                return True

            # print(f"Process {os.getpid()}: Trying direct query...")
            self.req_socket.send_json({"type": "query"})
            poller = zmq.Poller()
            poller.register(self.req_socket, zmq.POLLIN)
            events = dict(poller.poll(200))  # reduced timeout
            if self.req_socket in events:
                # print(f"Process {os.getpid()}: Found existing service via REQ")
                return True

            self._logger.debug(f"Process {os.getpid()}: No existing service found")
            return False

        except Exception as e:
            # print(f"Process {os.getpid()}: Error in connect_to_discovery: {e}")
            return False
        finally:
            self.sub_socket.close()
            self.req_socket.close()

    def cleanup(self):
        
        # if hasattr(self, 'service_thread') and self.service_thread.is_alive():
        #     self.service_thread.join(timeout=1.0)
        if hasattr(self, 'pub_socket'):
            self.pub_socket.close()
        if hasattr(self, 'rep_socket'):
            self.rep_socket.close()
        if hasattr(self, 'context'):
            self.context.term()
        self.running = False

def session_discovery_launcher(ether_id: str, session_config: Optional[EtherSessionConfig] = None):
    """Launch an Ether session process
    
    Args:
        process_id: ID for the process/session
        session_config: Optional network configuration
    """
    try:

        session = _EtherDiscovery(
            ether_id=ether_id,
            session_config=session_config
        )
        session.start()

    except Exception as e:
        pass
