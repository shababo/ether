import functools
import logging
from typing import Optional, Union, Dict
from pydantic import BaseModel
from .decorators import ether_pub, ether_sub, ether_save, ether_cleanup
from ._internal._utils import _get_logger
from ._internal._ether import _ether
from ._internal._config import EtherConfig
import atexit

# _ether_initialized = False
_logger = None  # Initialize later

def _pub(data: Union[Dict, BaseModel], topic: str):
    """Publish data to a topic
    
    Args:
        data: Data to publish (dict or Pydantic model)
        topic: Topic to publish to
    """

    _ether.publish(data, topic)

# Public singleton instance of Ether API
class Ether:
    pub = staticmethod(_pub)
    save = staticmethod(functools.partial(_pub, {}, topic="Ether.save"))
    cleanup_all = staticmethod(functools.partial(_pub, {}, topic="Ether.cleanup"))
    shutdown = staticmethod(functools.partial(_pub, {}, topic="Ether.shutdown"))
    _initialized = False
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Ether, cls).__new__(cls)
        return cls._instance

    def init(self,config: Optional[Union[str, dict, EtherConfig]] = None, restart: bool = False):
        """Initialize the Ether messaging system."""
        # global _ether_initialized
        
        if self._initialized and restart:
            # Clean up existing system
            _init_logger()
            _logger.debug("Force reinitializing Ether system...")
            _ether.shutdown()
            self._initialized = False
            
        if not self._initialized:
            # Initialize logger
            _init_logger()

            # Start ether
            _ether.start(config=config, restart=restart)
        
            # Mark as initialized
            self._initialized = True
            _logger.info("Ether system initialized")
            
            # Register single cleanup handler
            atexit.register(_ether.shutdown)

def _init_logger(log_level: int = logging.INFO):
    """Initialize logger with proper cleanup"""
    global _logger
    if _logger is None:
        _logger = _get_logger("EtherMain", log_level=log_level)
 

# Export public interface
ether = Ether()
__all__ = ['ether', 'ether_pub', 'ether_sub', 'ether_save', 'ether_cleanup']
