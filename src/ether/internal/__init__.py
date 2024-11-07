import functools
import logging
import zmq
import time
from typing import Optional, Union, List, Dict
from multiprocessing import Process
import signal
import sys
from pydantic import BaseModel
from ._decorators import ether_pub, ether_sub, ether_save, ether_cleanup
from ._utils import _get_logger
from ._ether import _ether
from ._config import EtherConfig
import atexit

# _ether_initialized = False
_logger = None  # Initialize later

def pub(data: Union[Dict, BaseModel], topic: str):
    """Publish data to a topic
    
    Args:
        data: Data to publish (dict or Pydantic model)
        topic: Topic to publish to
    """

    _ether.publish(data, topic)

# Public singleton instance of Ether API
class Ether:
    save = functools.partial(pub, {}, topic="Ether.save")
    cleanup_all = functools.partial(pub, {}, topic="Ether.cleanup")
    shutdown = functools.partial(pub, {}, topic="Ether.shutdown")
    _initialized = False
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Ether, cls).__new__(cls)
        return cls._instance

    def init(self,config: Optional[Union[str, dict, EtherConfig]] = None, force_reinit: bool = False):
        """Initialize the Ether messaging system."""
        # global _ether_initialized
        
        if cls._ether_initialized and force_reinit:
            # Clean up existing system
            _init_logger()
            _logger.debug("Force reinitializing Ether system...")
            _ether.shutdown()
            _ether_initialized = False
            
        if not _ether_initialized:
            # Initialize logger
            _init_logger()

            # Start ether
            _ether.start(config=config)
        
            # Mark as initialized
            _ether_initialized = True
            _logger.info("Ether system initialized")
            
            # Register single cleanup handler
            atexit.register(_ether.shutdown)

def _init_logger(log_level: int = logging.DEBUG):
    """Initialize logger with proper cleanup"""
    global _logger
    if _logger is None:
        _logger = _get_logger("EtherMain", log_level=log_level)

# def _cleanup_logger():
#     """Properly close logger handlers"""
#     global _logger
#     if _logger:
#         for handler in _logger.handlers[:]:
#             handler.close()
#             _logger.removeHandler(handler)



        

# Export public interface

__all__ = ['Ether', 'ether_init', 'ether_pub', 'ether_sub', 'ether_save', 'ether_cleanup']

