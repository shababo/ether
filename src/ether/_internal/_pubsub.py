import zmq
import logging
import signal
import uuid
from multiprocessing import Process
import os
import tempfile
import atexit
import time

from ._utils import _ETHER_SUB_PORT, _ETHER_PUB_PORT, _get_logger

class _EtherPubSubProxy:
    """Proxy that uses XPUB/XSUB sockets for efficient message distribution.
    
    XPUB/XSUB sockets are special versions of PUB/SUB that expose subscriptions
    as messages, allowing for proper subscription forwarding.
    """
    def __init__(self):
        self.capture_socket = None
        self.broadcast_socket = None
        self._running = False
        self.setup_sockets()

    def setup_sockets(self):
        """Setup XPUB/XSUB sockets with optimized settings"""
        self.id = uuid.uuid4()
        self.name = f"EtherPubSubProxy_{self.id}"
        self._logger = _get_logger("Proxy", log_level=logging.INFO)

        self._zmq_context = zmq.Context()
        
        # Use standard ports
        self.capture_socket = self._zmq_context.socket(zmq.XSUB)
        self.capture_socket.bind(f"tcp://*:{_ETHER_PUB_PORT}")
        self.capture_socket.setsockopt(zmq.RCVHWM, 1000000)
        self.capture_socket.setsockopt(zmq.RCVBUF, 65536)
        
        self.broadcast_socket = self._zmq_context.socket(zmq.XPUB)
        self.broadcast_socket.bind(f"tcp://*:{_ETHER_SUB_PORT}")
        self.broadcast_socket.setsockopt(zmq.SNDHWM, 1000000)
        self.broadcast_socket.setsockopt(zmq.SNDBUF, 65536)
        self.broadcast_socket.setsockopt(zmq.XPUB_VERBOSE, 1)
        
        # Set TCP keepalive options
        for socket in [self.capture_socket, self.broadcast_socket]:
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
            socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # Create poller to monitor both sockets
        self._poller = zmq.Poller()
        self._poller.register(self.capture_socket, zmq.POLLIN)
        self._poller.register(self.broadcast_socket, zmq.POLLIN)
        
        self._logger.debug(f"Starting proxy with {self.capture_socket} and {self.broadcast_socket}")
    
    def run(self):
        """Run the proxy with graceful shutdown support"""
        try:
            self._running = True
            
            while self._running:
                try:
                    events = dict(self._poller.poll(timeout=100))  # 100ms timeout
                    
                    if self.capture_socket in events:
                        message = self.capture_socket.recv_multipart()
                        self._logger.debug(f"Proxy forwarding from capture_socket: {len(message)} parts")
                        self._logger.debug(f"Topic: {message[0]}")
                        self.broadcast_socket.send_multipart(message)
                    
                    if self.broadcast_socket in events:
                        message = self.broadcast_socket.recv_multipart()
                        self._logger.debug(f"Proxy forwarding from broadcast_socket: {len(message)} parts")
                        self._logger.debug(f"Topic: {message[0]}")
                        self.capture_socket.send_multipart(message)
                        
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN:  # Timeout, just continue
                        continue
                    else:
                        self._logger.error(f"ZMQ Error in proxy: {e}")
                        raise
                        
        except Exception as e:
            self._logger.error(f"Error in proxy: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        self._running = False
        if self.capture_socket:
            self.capture_socket.close()
        if self.broadcast_socket:
            self.broadcast_socket.close()
        if self._zmq_context:
            self._zmq_context.term()

    def __del__(self):
        self.cleanup()