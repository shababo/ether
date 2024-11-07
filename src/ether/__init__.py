import functools
import logging
import zmq
import time
from typing import Optional, Union, List, Dict
from multiprocessing import Process
import signal
import sys
from pydantic import BaseModel
from ._registry import (
    EtherRegistry
)
from ._decorators import ether_pub, ether_sub, ether_save, ether_cleanup
from ._utils import _get_logger
from ._ether import _ether
from ._instances import EtherInstanceLiaison, EtherInstanceManager
from ._config import EtherConfig
import atexit

_ether_initialized = False
_instance_manager = EtherInstanceManager()
_logger = None  # Initialize later

def pub(data: Union[Dict, BaseModel], topic: str):
    """Publish data to a topic
    
    Args:
        data: Data to publish (dict or Pydantic model)
        topic: Topic to publish to
    """
    liaison = EtherInstanceLiaison()
    liaison.publish(data, topic)

# Ether interaction methods
class Ether:
    pass

Ether.save = functools.partial(pub, {}, topic="Ether.save")
Ether.cleanup_all = functools.partial(pub, {}, topic="Ether.cleanup")

def _init_logger(log_level: int = logging.DEBUG):
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

def _wait_for_redis():
    """Wait for Redis to be ready"""
    for _ in range(10):  # Try for 5 seconds
        try:
            tracker = EtherInstanceLiaison()
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
        _instance_manager.stop_all_instances()
        Ether.cleanup_all()
        
        _logger.info("Cleanup complete")
    finally:
        # Clean up logger last
        _cleanup_logger()

def ether_init(config: Optional[Union[str, dict, EtherConfig]] = None, force_reinit: bool = False):
    """Initialize the Ether messaging system."""
    global _ether_initialized
    
    if _ether_initialized and force_reinit:
        # Clean up existing system
        _init_logger()
        _logger.debug("Force reinitializing Ether system...")
        _ether.shutdown()
        _ether_initialized = False
        
    if not _ether_initialized:
        # Initialize logger
        _init_logger()

        # Process any pending classes
        EtherRegistry.process_pending_classes()
        
        # Start daemon
        if not _ether._initialized:
            _ether.start()
            
        # Check for existing instances
        tracker = EtherInstanceLiaison()
        existing = tracker.get_active_instances()
        if existing:
            _logger.warning(f"Found {len(existing)} existing instances in Redis:")
            for instance_id, info in existing.items():
                _logger.warning(f"  {instance_id}: {info.get('name')} ({info.get('class')})")
        tracker.cull_dead_processes()
        
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
            
            processes = config.launch_instances()
            for name, process in processes.items():
                _instance_manager.add_instance(name, process)
            
            # Wait for instances to be ready
            time.sleep(1.0)
            
            return config  # Return the config object for manual launching if needed

# Export public interface
__all__ = ['ether_pub', 'ether_sub', 'ether_save', 'ether_init', 'Ether', 'ether_cleanup']

