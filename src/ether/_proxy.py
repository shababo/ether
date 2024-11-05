import zmq
import logging
import signal
import uuid
from multiprocessing import Event, Process
import os
import tempfile
import atexit
import time

from ._utils import _ETHER_SUB_PORT, _ETHER_PUB_PORT

class _EtherPubSubProxy:
    """Proxy that uses XPUB/XSUB sockets for efficient message distribution.
    
    XPUB/XSUB sockets are special versions of PUB/SUB that expose subscriptions
    as messages, allowing for proper subscription forwarding.
    """
    def __init__(self):
        self.frontend = None
        self.backend = None
        self._running = False
        self.setup_sockets()

    def setup_sockets(self):
        """Setup XPUB/XSUB sockets with optimized settings"""
        self.id = uuid.uuid4()
        self.name = f"EtherPubSubProxy_{self.id}"
        self._logger = logging.getLogger("Proxy")

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
            socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
            socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # Create poller to monitor both sockets
        self._poller = zmq.Poller()
        self._poller.register(self.frontend, zmq.POLLIN)
        self._poller.register(self.backend, zmq.POLLIN)
        
        self._logger.info(f"Starting proxy with {self.frontend} and {self.backend}")
    
    def run(self, stop_event: Event):
        """Run the proxy with graceful shutdown support"""
        def handle_signal(signum, frame):
            stop_event.set()
        
        signal.signal(signal.SIGTERM, handle_signal)
        
        try:
            self._running = True
            
            while self._running and not stop_event.is_set():
                try:
                    events = dict(self._poller.poll(timeout=100))  # 100ms timeout
                    
                    if self.frontend in events:
                        message = self.frontend.recv_multipart()
                        self._logger.info(f"Proxy forwarding from frontend: {len(message)} parts")
                        self._logger.info(f"Message: {message}")
                        self.backend.send_multipart(message)
                    
                    if self.backend in events:
                        message = self.backend.recv_multipart()
                        self._logger.info(f"Proxy forwarding from backend: {len(message)} parts")
                        self._logger.info(f"Message: {message}")
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
            self.cleanup()

    def cleanup(self):
        self._running = False
        if self.frontend:
            self.frontend.close()
        if self.backend:
            self.backend.close()
        if self._zmq_context:
            self._zmq_context.term()

    def __del__(self):
        self.cleanup()

class _ProxyManager:
    """Singleton manager for the Ether proxy process"""
    _instance = None
    _proxy_process = None
    _stop_event = None
    _pid_file = os.path.join(tempfile.gettempdir(), 'ether_proxy.pid')
    _parent_pid = None
    _logger = logging.getLogger("ProxyManager")
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_ProxyManager, cls).__new__(cls)
        return cls._instance
    
    def start_proxy(self):
        """Start proxy if not already running. Safe to call multiple times."""
        if self._proxy_process is not None:
            return
        
        # Check if proxy is already running on this machine
        self._logger.info(f"PID FILE: {self._pid_file}")
        if os.path.exists(self._pid_file):
            self._logger.info("Already Proxy Running")
            with open(self._pid_file, 'r') as f:
                pid = int(f.read())
                try:
                    os.kill(pid, 0)  # Just check if process exists
                    return  # Proxy is already running
                except OSError:
                    os.remove(self._pid_file)
        
        # Start new proxy process
        self._logger.info("Starting new proxy process")
        self._stop_event = Event()
        self._proxy_process = Process(target=self._run_proxy, args=(self._stop_event,))
        self._proxy_process.daemon = True
        self._proxy_process.start()
        self._parent_pid = os.getpid()
        
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
            self._logger.info("Stopping proxy process")
            if self._stop_event is not None:
                self._stop_event.set()
            self._proxy_process.join(timeout=5)
            if self._proxy_process.is_alive():
                self._proxy_process.terminate()
            self._proxy_process = None
            self._stop_event = None
            
            # Force context termination to clear all buffers
            self._logger.info("Terminating ZMQ context")
            zmq.Context.instance().term()
            if os.path.exists(self._pid_file):
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

    def __del__(self):
        self.stop_proxy()

# Create singleton instance
_proxy_manager = _ProxyManager()

# Register cleanup on exit
@atexit.register
def _cleanup_proxy():
    _proxy_manager.stop_proxy() 
