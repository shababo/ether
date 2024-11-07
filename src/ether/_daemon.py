# Modify existing _proxy.py to include daemon functionality
import subprocess
import socket
import time
from pathlib import Path
import tempfile
import redis
import os
import atexit
import logging
from multiprocessing import Process
import zmq

from ._utils import _get_logger
from ._pubsub import _EtherPubSubProxy
from ._instance_tracker import EtherInstanceLiaison
from ._decorators import ether_cleanup
from ._ether import EtherRegistry
# Constants
CULL_INTERVAL = 10  # seconds between culling checks

def _run_pubsub():
    """Standalone function to run PubSub proxy"""
    proxy = _EtherPubSubProxy()
    proxy.run()

def _run_monitor():
    """Standalone function to run instance monitoring"""
    logger = _get_logger("EtherMonitor", log_level=logging.INFO)
    tracker = EtherInstanceLiaison()
    
    while True:
        try:
            # Cull dead processes first
            culled = tracker.cull_dead_processes()
            if culled:
                logger.debug(f"Culled {culled} dead instances")
            
            # Get remaining active instances
            instances = tracker.get_active_instances()
            logger.debug(f"Active instances: {instances}")
            time.sleep(CULL_INTERVAL)  # Check every CULL_INTERVAL seconds
        except Exception as e:
            logger.error(f"Error monitoring instances: {e}")
            time.sleep(1)

class _EtherDaemon:
    """Manages Redis and PubSub services for Ether"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_EtherDaemon, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        self._logger = _get_logger("EtherDaemon", log_level=logging.INFO)
        self._redis_process = None
        self._redis_port = 6379
        self._redis_pidfile = Path(tempfile.gettempdir()) / 'ether_redis.pid'
        self._pubsub_process = None
        self._monitor_process = None
        self._started = False
    
    def start(self):
        """Start all daemon services"""
        if self._started:
            return
        
        # Start services
        if not self._ensure_redis_running():
            raise RuntimeError("Redis server failed to start")
        if not self._ensure_pubsub_running():
            raise RuntimeError("PubSub proxy failed to start")
        
        # Start monitoring in separate process
        self._monitor_process = Process(target=_run_monitor)
        self._monitor_process.daemon = True
        self._monitor_process.start()
        
        self._started = True
    
    def _ensure_redis_running(self) -> bool:
        """Ensure Redis server is running, start if not"""
        if self._redis_pidfile.exists():
            with open(self._redis_pidfile) as f:
                pid = int(f.read().strip())
            try:
                os.kill(pid, 0)
                return self._test_redis_connection()
            except (OSError, redis.ConnectionError):
                self._redis_pidfile.unlink()
        
        self._start_redis_server()
        return self._test_redis_connection()
    
    def _ensure_pubsub_running(self) -> bool:
        """Ensure PubSub proxy is running"""
        # Clean up any existing ZMQ contexts
        zmq.Context.instance().term()
        if self._pubsub_process is None:
            self._pubsub_process = Process(target=_run_pubsub)
            self._pubsub_process.daemon = True
            self._pubsub_process.start()

        return self._test_pubsub_connection()

    
    def _test_redis_connection(self) -> bool:
        """Test Redis connection"""
        try:
            r = redis.Redis(port=self._redis_port)
            r.ping()
            r.close()
            return True
        except redis.ConnectionError:
            return False
        
    def _test_pubsub_connection(self) -> bool:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        for _ in range(10):  # Try for 5 seconds
            try:
                socket.connect(f"tcp://localhost:5555")
                socket.close()
                context.term()
                return True
            except zmq.error.ZMQError:
                time.sleep(0.5)
        return False
    
    def _start_redis_server(self):
        """Start Redis server process"""
        self._redis_process = subprocess.Popen(
            [
                'redis-server',
                '--port', str(self._redis_port),
                '--dir', tempfile.gettempdir()  # Use temp dir for dump.rdb
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Save PID
        with open(self._redis_pidfile, 'w') as f:
            f.write(str(self._redis_process.pid))
        
        # Wait for Redis to be ready
        max_attempts = 10
        for _ in range(max_attempts):
            try:
                self._test_redis_connection()
                break
            except redis.ConnectionError:
                time.sleep(0.5)
        else:
            raise RuntimeError("Redis server failed to start")
    
    def _stop_all_instances(self):
        """Stop all instances"""
        tracker = EtherInstanceLiaison()
        tracker.stop_all_instances()

    def shutdown(self):
        """Shutdown all services"""

        self._stop_all_instances()
        try:
            if self._pubsub_process:
                self._logger.debug("Shutting down PubSub proxy")
                self._pubsub_process.terminate()
            if self._monitor_process:
                self._logger.debug("Shutting down monitor")
                self._monitor_process.terminate()
            if self._redis_process:
                self._logger.debug("Shutting down Redis server")
                self._redis_process.terminate()
                self._redis_process.wait(timeout=5)
                if self._redis_pidfile.exists():
                    self._redis_pidfile.unlink()
            # self.cleanup()
        finally:
            # Clean up logger
            if hasattr(self, '_logger'):
                for handler in self._logger.handlers[:]:
                    handler.close()
                    self._logger.removeHandler(handler)

# Create singleton instance but don't start it
# Process any pending classes
EtherRegistry.process_pending_classes()
daemon_manager = _EtherDaemon()

# # Register cleanup
# @atexit.register
# def _cleanup_daemon():
#     daemon_manager.shutdown()