import zmq
import uuid
import time
import threading
import os
from typing import Optional, Dict, Any
import atexit
import multiprocessing
import errno
from time import sleep

from ether.utils import get_ether_logger, get_ip_address
from ether._internal._config import EtherNetworkConfig

class EtherSession: 
    DISCOVERY_PUB_PORT = 301309
    QUERY_PORT = 301310

    def __init__(self, ether_id: str = "default", network_config: Optional[EtherNetworkConfig] = None):
        self.session_id = str(uuid.uuid4())
        self.ether_id = ether_id
        self.context = zmq.Context()
        self.is_discovery_service = False
        self.running = False
        self._logger = get_ether_logger(process_name="EtherSession")
        
        # Use provided network config or defaults
        self.network = network_config or EtherNetworkConfig()
        
        self.metadata = {
            "session_id": self.session_id,
            "ether_id": ether_id,
            "start_time": time.time(),
            "pid": os.getpid(),
            "public_ip": get_ip_address(),
            "network": self.network.model_dump()  # Add network config to metadata
        }
        
        try:
            if not self._connect_to_discovery():
                self._logger.debug(f"Process {os.getpid()}: No existing service found, attempting to start one...")
                self._start_discovery_service()
        except Exception as e:
            self.cleanup()
            raise e

        atexit.register(self.cleanup)

    def _start_discovery_service(self):
        try:
            self.pub_socket = self.context.socket(zmq.PUB)
            # Bind to all interfaces for remote connections
            self.pub_socket.bind(f"tcp://*:{self.network.session_discovery_port}")
            
            self.rep_socket = self.context.socket(zmq.REP)
            self.rep_socket.bind(f"tcp://*:{self.network.session_query_port}")
            
            self.is_discovery_service = True
            self.running = True
            
            self.service_thread = threading.Thread(target=self._run_discovery_service)
            self.service_thread.daemon = True
            self.service_thread.start()
            
            self._logger.debug(f"Started discovery service on ports {self.network.session_discovery_port}, {self.network.session_query_port}")
            
        except zmq.ZMQError as e:
            if e.errno == errno.EADDRINUSE:
                self._logger.debug(f"Process {os.getpid()}: Another process just started the discovery service, attempting to connect...")
                # Clean up our failed binding attempts
                if hasattr(self, 'pub_socket'):
                    self.pub_socket.close()
                if hasattr(self, 'rep_socket'):
                    self.rep_socket.close()
                
                # Give the other process a moment to fully initialize
                sleep(0.1)
                
                # Try to connect again
                if self._connect_to_discovery():
                    self._logger.debug(f"Process {os.getpid()}: Successfully connected to existing service")
                    return
                else:
                    raise RuntimeError("Failed to connect to existing service after bind failure")
            else:
                self._logger.debug(f"Process {os.getpid()}: Failed to start discovery service: {e}")
                self.cleanup()
                raise

    def _run_discovery_service(self):
        poller = zmq.Poller()
        poller.register(self.rep_socket, zmq.POLLIN)

        self._logger.debug(f"Running discovery service, ether_id: {self.ether_id}")
        while self.running:
            try:
                self.pub_socket.send_json({
                    "type": "announcement",
                    "data": self.metadata
                })

                events = dict(poller.poll(1000))
                if self.rep_socket in events:
                    msg = self.rep_socket.recv_json()
                    if msg.get("type") == "query":
                        self.rep_socket.send_json({
                            "type": "response",
                            "data": self.metadata
                        })
            except Exception as e:
                self._logger.debug(f"Process {os.getpid()}: Service error: {e}")
                break

    @classmethod
    def get_current_session(cls, timeout: int = 200, network_config: Optional[EtherNetworkConfig] = None) -> Optional[Dict[str, Any]]:  # reduced timeout
        # print(f"Process {os.getpid()}: Attempting to get current session...")
        context = zmq.Context()
        req_socket = context.socket(zmq.REQ)

        # Use provided network config or defaults
        network = network_config or EtherNetworkConfig()
        
        try:
            req_socket.setsockopt(zmq.LINGER, 0)  # Don't wait on close
            req_socket.connect(f"tcp://{network.host}:{network.session_query_port}")
            # print(f"Process {os.getpid()}: Connected to query port")
            
            req_socket.send_json({"type": "query"})
            # print(f"Process {os.getpid()}: Sent query")
            
            poller = zmq.Poller()
            poller.register(req_socket, zmq.POLLIN)
            
            # print(f"Process {os.getpid()}: Waiting for response...")
            events = dict(poller.poll(timeout))
            if req_socket in events:
                response = req_socket.recv_json()
                # print(f"Process {os.getpid()}: Received response")
                if response.get("type") == "response":
                    return response["data"]
            # print(f"Process {os.getpid()}: No response received")
            return None
            
        except Exception as e:
            # print(f"Process {os.getpid()}: Error in get_current_session: {e}")
            return None
        finally:
            req_socket.close()
            context.term()

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
        self.running = False
        if hasattr(self, 'service_thread') and self.service_thread.is_alive():
            self.service_thread.join(timeout=1.0)
        if hasattr(self, 'pub_socket'):
            self.pub_socket.close()
        if hasattr(self, 'rep_socket'):
            self.rep_socket.close()
        if hasattr(self, 'context'):
            self.context.term()

def session_discovery_launcher(process_id: str, network_config: Optional[EtherNetworkConfig] = None):
    """Launch an Ether session process
    
    Args:
        process_id: ID for the process/session
        network_config: Optional network configuration
    """
    try:
        # First try to find existing session
        # print(f"Process {process_id}: Checking for existing session...")
        current_session = EtherSession.get_current_session(timeout=200)
        
        if current_session is None:
            # print(f"Process {process_id}: No session found, attempting to create new one...")
            try:
                session_mgr = EtherSession(
                    ether_id=process_id,
                    network_config=network_config
                )
                while True:
                    time.sleep(1.0)
            except zmq.ZMQError as e:
                if e.errno == errno.EADDRINUSE:
                    # Someone else created it just before us, try to get their session
                    # print(f"Process {process_id}: Another process created session first, connecting...")
                    current_session = EtherSession.get_current_session(timeout=200)
                else:
                    raise
        else:
            # print(f"Process {process_id}: Found existing session {current_session['session_id']}")
            pass
            
    except Exception as e:
        # print(f"Process {process_id}: Error: {e}")
        pass
    finally:
        # print(f"Process {process_id}: Shutting down")
        pass

# def main():
#     processes = []
#     for i in range(4):
#         p = multiprocessing.Process(target=session_discovery_launcher, args=(i,))
#         processes.append(p)
#         p.start()
#         time.sleep(0.1)  # Small delay between launches

#     for p in processes:
#         p.join()

# if __name__ == "__main__":
#     main()