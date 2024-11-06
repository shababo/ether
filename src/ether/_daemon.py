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

from ._utils import _get_logger
from ._pubsub import _EtherPubSubProxy
from ._instance_tracker import EtherInstanceTracker

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
        self._logger = _get_logger("EtherDaemon")
        self._redis_process = None
        self._redis_port = 6379
        self._redis_pidfile = Path(tempfile.gettempdir()) / 'ether_redis.pid'
        self._pubsub_proxy = None
        
        # Start services
        self._ensure_redis_running()
        self._ensure_pubsub_running()
        
        self._instance_tracker = EtherInstanceTracker()
        
        # Start monitoring in separate process
        self._monitor_process = Process(target=self.monitor_instances)
        self._monitor_process.daemon = True
        self._monitor_process.start()
    
    def _ensure_redis_running(self):
        """Ensure Redis server is running, start if not"""
        if self._redis_pidfile.exists():
            with open(self._redis_pidfile) as f:
                pid = int(f.read().strip())
            try:
                os.kill(pid, 0)
                self._test_redis_connection()
                return
            except (OSError, redis.ConnectionError):
                self._redis_pidfile.unlink()
        
        self._start_redis_server()
    
    def _ensure_pubsub_running(self):
        """Ensure PubSub proxy is running"""
        if self._pubsub_proxy is None:
            self._pubsub_proxy = _EtherPubSubProxy()
            self._pubsub_process = Process(target=self._pubsub_proxy.run)
            self._pubsub_process.daemon = True
            self._pubsub_process.start()
    
    def _test_redis_connection(self):
        """Test Redis connection"""
        r = redis.Redis(port=self._redis_port)
        r.ping()
        r.close()
    
    def _start_redis_server(self):
        """Start Redis server process"""
        self._redis_process = subprocess.Popen(
            ['redis-server', '--port', str(self._redis_port)],
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
    
    def monitor_instances(self):
        """Monitor registered instances and their health"""
        while True:
            try:
                instances = self._instance_tracker.get_active_instances()
                self._logger.debug(f"Active instances: {instances}")
                time.sleep(10)  # Check every 10 seconds
            except Exception as e:
                self._logger.error(f"Error monitoring instances: {e}")
                time.sleep(1)
    
    def shutdown(self):
        """Shutdown all services"""
        if self._pubsub_proxy:
            self._pubsub_proxy.cleanup()
        if self._monitor_process:
            self._monitor_process.terminate()
        if self._redis_process:
            self._redis_process.terminate()
            self._redis_process.wait(timeout=5)
            if self._redis_pidfile.exists():
                self._redis_pidfile.unlink()

# Create singleton instance
daemon_manager = _EtherDaemon()

# Register cleanup
@atexit.register
def _cleanup_daemon():
    daemon_manager.shutdown()