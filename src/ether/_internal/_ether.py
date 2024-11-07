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
from typing import Union, Dict
from pydantic import BaseModel
import json

from ._utils import _ETHER_SUB_PORT, _ETHER_PUB_PORT, _get_logger
from ._pubsub import _EtherPubSubProxy
from ._instances._liaison import EtherInstanceLiaison 
from ._instances._manager import _EtherInstanceManager
from ._config import EtherConfig
from ._registry import EtherRegistry

# Constants
CULL_INTERVAL = 10  # seconds between culling checks

def _run_pubsub():
    """Standalone function to run PubSub proxy"""
    proxy = _EtherPubSubProxy()
    proxy.run()

def _run_monitor():
    """Standalone function to run instance monitoring"""
    logger = _get_logger("EtherMonitor", log_level=logging.INFO)
    liaison = EtherInstanceLiaison()
    
    while True:
        try:
            # Cull dead processes first
            culled = liaison.cull_dead_processes()
            if culled:
                logger.debug(f"Culled {culled} dead instances")
            
            # Get remaining active instances
            instances = liaison.get_active_instances()
            logger.debug(f"Active instances: {instances}")
            time.sleep(CULL_INTERVAL)  # Check every CULL_INTERVAL seconds
        except Exception as e:
            logger.error(f"Error monitoring instances: {e}")
            time.sleep(1)

class _Ether:
    """Singleton to manage Ether services behind the scenes."""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_Ether, cls).__new__(cls)
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
        self._instance_manager = None
        self._started = False
        
        # Add ZMQ publishing setup
        
        self._pub_socket = None
    
    def _setup_publisher(self):
        """Set up the ZMQ publisher socket"""
        if self._pub_socket is None:
            self._zmq_context = zmq.Context()
            self._pub_socket = self._zmq_context.socket(zmq.PUB)
            self._pub_socket.connect(f"tcp://localhost:{_ETHER_PUB_PORT}")
    
    def publish(self, data: Union[Dict, BaseModel], topic: str) -> None:
        """Publish data to a topic
        
        Args:
            data: Data to publish (dict or Pydantic model)
            topic: Topic to publish to
        """
        if not self._started:
            raise RuntimeError("Cannot publish: Ether system not started")
            
        if self._pub_socket is None:
            self._setup_publisher()
            
        # Convert data to JSON
        if isinstance(data, BaseModel):
            json_data = data.model_dump_json()
        elif isinstance(data, dict):
            json_data = json.dumps(data)
        else:
            raise TypeError("Data must be a dict or Pydantic model")
            
        # Publish message
        self._pub_socket.send_multipart([
            topic.encode(),
            json_data.encode()
        ])
        self._logger.debug(f"Published to {topic}: {json_data}")
    
    def start(self, config = None, restart: bool = False):
        """Start all daemon services"""

        if self._started:
            if restart:
                self.shutdown()
            else:
                return
        
        # Process any pending classes
        EtherRegistry.process_pending_classes()
        
        # Start Redis
        if not self._ensure_redis_running():
            raise RuntimeError("Redis server failed to start")
        
        # Start Messaging
        if not self._ensure_pubsub_running():
            raise RuntimeError("PubSub proxy failed to start")
        
        # Start instance process monitoring (DO WE NEED THIS?)
        self._monitor_process = Process(target=_run_monitor)
        self._monitor_process.daemon = True
        self._monitor_process.start()

        if config:
            self._start_instances(config)
        
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
    
    def _start_instances(self, config: EtherConfig = None):
        """Start instances from configuration"""
        
        self._logger.debug("Starting instances")
        if not self._instance_manager:
            self._instance_manager = _EtherInstanceManager(config=config)
        else:
            self._instance_manager.launch_instances(config)
        # Wait for instances to be ready
        time.sleep(1.0)


    def shutdown(self):
        """Shutdown all services"""

        try:
            # stop all instances
            if self._instance_manager:
                self._instance_manager.stop_all_instances()
            # close publishing socket and context
            if self._pub_socket:
                self._pub_socket.close()
                self._pub_socket = None 
            if self._zmq_context:
                self._zmq_context.term()
                self._zmq_context = None
            # Terminate pubsub proxy process
            if self._pubsub_process:
                self._logger.debug("Shutting down PubSub proxy")
                self._pubsub_process.terminate()
            # Terminate instance monitoring process
            if self._monitor_process:
                self._logger.debug("Shutting down monitor")
                self._monitor_process.terminate()
            # Terminate Redis server
            if self._redis_process:
                self._logger.debug("Shutting down Redis server")
                self._redis_process.terminate()
                self._redis_process.wait(timeout=5)
                if self._redis_pidfile.exists():
                    self._redis_pidfile.unlink()
        except Exception as e:
            self._logger.error(f"Error shutting down services: {e}")
        finally:
            # Clean up logger
            if hasattr(self, '_logger'):
                for handler in self._logger.handlers[:]:
                    handler.close()
                    self._logger.removeHandler(handler)

# Create singleton instance but don't start it
_ether = _Ether()

# # Register cleanup
# @atexit.register
# def _cleanup_daemon():
#     daemon_manager.shutdown()