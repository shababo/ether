import zmq
import uuid
from typing import Optional

from ..utils import get_ether_logger
from ._config import EtherNetworkConfig
from ._session import EtherSession


class _EtherPubSubProxy:
    """Proxy that uses XPUB/XSUB sockets for efficient message distribution.
    
    XPUB/XSUB sockets are special versions of PUB/SUB that expose subscriptions
    as messages, allowing for proper subscription forwarding.
    """
    def __init__(self, network_config: Optional[EtherNetworkConfig] = None):
        self.id = uuid.uuid4()
        self.name = f"EtherPubSubProxy_{self.id}"
        self._logger = get_ether_logger("EtherPubSubProxy")
        self._logger.debug("Initializing PubSub proxy")
        
        # Use provided network config or get from session
        if network_config is None:
            session_data = EtherSession.get_current_session()
            if session_data and "network" in session_data:
                network_config = EtherNetworkConfig.model_validate(session_data["network"])
            else:
                network_config = EtherNetworkConfig()
        self.network = network_config
        
        self.capture_socket = None
        self.broadcast_socket = None
        self._running = False
        self.setup_sockets()

    def setup_sockets(self):
        """Setup XPUB/XSUB sockets with optimized settings"""
        self._logger.debug("Setting up ZMQ sockets")
        
        self._zmq_context = zmq.Context()
        
        # Setup capture (XSUB) socket
        self._logger.debug(f"Setting up XSUB socket on port {self.network.pubsub_backend_port}")
        self.capture_socket = self._zmq_context.socket(zmq.XSUB)
        self.capture_socket.bind(f"tcp://*:{self.network.pubsub_backend_port}")
        self.capture_socket.setsockopt(zmq.RCVHWM, 1000000)
        self.capture_socket.setsockopt(zmq.RCVBUF, 65536)
        
        # Setup broadcast (XPUB) socket
        self._logger.debug(f"Setting up XPUB socket on port {self.network.pubsub_frontend_port}")
        self.broadcast_socket = self._zmq_context.socket(zmq.XPUB)
        self.broadcast_socket.bind(f"tcp://*:{self.network.pubsub_frontend_port}")
        self.broadcast_socket.setsockopt(zmq.SNDHWM, 1000000)
        self.broadcast_socket.setsockopt(zmq.SNDBUF, 65536)
        self.broadcast_socket.setsockopt(zmq.XPUB_VERBOSE, 1)
        
        # Set TCP keepalive options
        self._logger.debug("Configuring socket keepalive options")
        for socket in [self.capture_socket, self.broadcast_socket]:
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
            socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # Create poller
        self._logger.debug("Setting up ZMQ poller")
        self._poller = zmq.Poller()
        self._poller.register(self.capture_socket, zmq.POLLIN)
        self._poller.register(self.broadcast_socket, zmq.POLLIN)
        
        self._logger.debug("PubSub proxy sockets initialized")
    
    def run(self):
        """Run the proxy with graceful shutdown support"""
        self._logger.debug("Starting PubSub proxy event loop")
        try:
            self._running = True
            
            while self._running:
                try:
                    events = dict(self._poller.poll(timeout=100))  # 100ms timeout
                    
                    if self.capture_socket in events:
                        message = self.capture_socket.recv_multipart()
                        topic = message[0].decode()
                        self._logger.debug(f"Forwarding from publisher: Topic={topic}")
                        self.broadcast_socket.send_multipart(message)
                    
                    if self.broadcast_socket in events:
                        message = self.broadcast_socket.recv_multipart()
                        # First byte indicates subscription: 1=subscribe, 0=unsubscribe
                        is_subscribe = message[0][0] == 1
                        topic = message[0][1:].decode()  # Topic follows the first byte
                        self._logger.debug(
                            f"{'Subscription' if is_subscribe else 'Unsubscription'} "
                            f"received for topic: {topic}"
                        )
                        self.capture_socket.send_multipart(message)
                        
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN:  # Timeout, just continue
                        continue
                    else:
                        self._logger.error(f"ZMQ Error in proxy: {e}")
                        raise
                        
        except Exception as e:
            self._logger.error(f"Error in proxy event loop: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        self._logger.debug("Cleaning up PubSub proxy")
        self._running = False
        
        if self.capture_socket:
            self._logger.debug("Closing capture socket")
            self.capture_socket.close()
            
        if self.broadcast_socket:
            self._logger.debug("Closing broadcast socket")
            self.broadcast_socket.close()
            
        if self._zmq_context:
            self._logger.debug("Terminating ZMQ context")
            self._zmq_context.term()
            
        self._logger.debug("PubSub proxy cleanup complete")

    def __del__(self):
        self.cleanup()