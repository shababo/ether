import logging
import zmq
import time
from typing import Optional, Union, List, Dict
from multiprocessing import Process
import signal
import sys
from ._ether import (
    _Ether, EtherRegistry,    
)
from ._utils import _get_logger
from ._daemon import daemon_manager
from ._instance_tracker import EtherInstanceTracker
from ._config import EtherConfig
import atexit

_ether_initialized = False
_instance_processes: Dict[str, Process] = {}
_logger = None  # Initialize later

def _init_logger(log_level: int = logging.INFO):
    """Initialize logger with proper cleanup"""
    global _logger
    if _logger is None:
        _logger = _get_logger("EtherMain", log_level=log_level)

def _cleanup_logger():
    """Properly close logger handlers"""
    global _logger
    if _logger:
        for handler in _logger.handlers[:]:
            handler.close()
            _logger.removeHandler(handler)

def _wait_for_pubsub():
    """Wait for PubSub proxy to be ready"""
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

def _wait_for_redis():
    """Wait for Redis to be ready"""
    from ._instance_tracker import EtherInstanceTracker
    for _ in range(10):  # Try for 5 seconds
        try:
            tracker = EtherInstanceTracker()
            tracker.redis.ping()
            return True
        except Exception:
            time.sleep(0.5)
    return False

def _cleanup_handler():
    """Combined cleanup handler for both resources and logger"""
    _init_logger()
    _logger.debug("Starting cleanup...")
    
    try:
        # Stop all instances
        stop_all_instances()
        
        # Wait for instances to finish
        _logger.debug(f"Waiting for {len(_instance_processes)} instances to finish")
        for process in _instance_processes.values():
            process.join(timeout=5)
            if process.is_alive():
                _logger.warning(f"Process {process.name} didn't stop gracefully, terminating")
                process.terminate()
                process.join(timeout=1)
        
        # Shutdown daemon services
        daemon_manager.shutdown()
        
        _logger.info("Cleanup complete")
    finally:
        # Clean up logger last
        _cleanup_logger()

def ether_init(config: Optional[Union[str, dict, EtherConfig]] = None, force_reinit: bool = False):
    """Initialize the Ether messaging system.
    
    Args:
        config: Optional configuration for instances
        force_reinit: If True, will cleanup and reinitialize even if already initialized
    """
    global _ether_initialized, _instance_processes
    
    if _ether_initialized and force_reinit:
        # Clean up existing system
        _init_logger()
        _logger.debug("Force reinitializing Ether system...")
        _cleanup_handler()
        _ether_initialized = False
        
    if not _ether_initialized:
        # Initialize logger
        _init_logger()
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, _cleanup_handler)
        signal.signal(signal.SIGTERM, _cleanup_handler)
        
        # Clean up any existing ZMQ contexts
        zmq.Context.instance().term()
        
        # Start daemon (which manages Redis and PubSub)
        daemon_manager.start()
        
        # Wait for services to be ready
        if not _wait_for_redis():
            raise RuntimeError("Redis failed to start")
        if not _wait_for_pubsub():
            raise RuntimeError("PubSub proxy failed to start")
            
        # Check for existing instances
        tracker = EtherInstanceTracker()
        existing = tracker.get_active_instances()
        if existing:
            _logger.warning(f"Found {len(existing)} existing instances in Redis:")
            for instance_id, info in existing.items():
                _logger.warning(f"  {instance_id}: {info.get('name')} ({info.get('class')})")
        tracker.cull_dead_processes()
        existing_after_cull = tracker.get_active_instances()
        if existing_after_cull:
            _logger.warning(f"Found {len(existing_after_cull)} existing instances in Redis AFTER CULL:")
            for instance_id, info in existing.items():
                _logger.warning(f"  {instance_id}: {info.get('name')} ({info.get('class')})")
        elif existing:
            _logger.debug("No existing instances found after cull")
        
        # Process any pending classes
        EtherRegistry.process_pending_classes()
        
        # Mark as initialized
        _ether_initialized = True
        _logger.info("Ether system initialized")
        
        # Register single cleanup handler
        atexit.register(_cleanup_handler)
        
        # Handle configuration if provided - do this last after classes are processed
        if config is not None:
            if isinstance(config, str):
                config = EtherConfig.from_yaml(config)
            elif isinstance(config, dict):
                config = EtherConfig.model_validate(config)
            
            _instance_processes = config.launch_instances()
            
            # Wait for instances to be ready
            time.sleep(1.0)
            
            return config  # Return the config object for manual launching if needed

def stop_instance(instance_id: str):
    """Stop a specific instance"""
    if instance_id in _instance_processes:
        process = _instance_processes[instance_id]
        _logger.debug(f"Stopping instance {instance_id}")
        
        # Get instance info before stopping
        tracker = EtherInstanceTracker()
        instances = tracker.get_active_instances()
        
        # Find the Redis ID for this process
        redis_id = None
        for rid, info in instances.items():
            if info.get('process_name') == instance_id:
                redis_id = rid
                break
        
        # Stop the process
        process.terminate()
        process.join(timeout=5)
        if process.is_alive():
            _logger.warning(f"Instance {instance_id} didn't stop gracefully, killing")
            process.kill()
            process.join(timeout=1)
            
        # Deregister from Redis if we found the ID
        if redis_id:
            _logger.debug(f"Deregistering instance {instance_id} (Redis ID: {redis_id})")
            tracker.deregister_instance(redis_id)
        else:
            _logger.warning(f"Could not find Redis ID for instance {instance_id}")
            
        del _instance_processes[instance_id]

def stop_all_instances():
    """Stop all running instances"""
    for instance_id in list(_instance_processes.keys()):
        stop_instance(instance_id)


# ether = _Ether()
ether_pub = _Ether.ether_pub
ether_sub = _Ether.ether_sub

# Export public interface
__all__ = ['ether_pub', 'ether_sub', 'ether_init', 'stop_instance', 'stop_all_instances']

