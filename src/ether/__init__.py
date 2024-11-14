import functools
import logging
from typing import Optional, Union, Dict
from pydantic import BaseModel
from .decorators import ether_pub, ether_sub, ether_init, ether_save, ether_cleanup
from ._internal._utils import _get_logger
from ._internal._ether import _ether
from ._internal._config import EtherConfig
import atexit


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
    cleanup = staticmethod(functools.partial(_pub, {}, topic="Ether.cleanup"))
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
            print("Force reinitializing Ether system...")
            self.shutdown()
            self._initialized = False
            
        if not self._initialized:

            # Start ether
            _ether.start(config=config, restart=restart)
        
            # Mark as initialized
            self._initialized = True
            print("Ether system initialized")
            
            # Register single cleanup handler
            atexit.register(self.shutdown)

    def shutdown(self):
        """Shutdown the Ether messaging system"""
        _ether.shutdown()
 

## Export public interface
# instantiate singleton for API
ether = Ether()
# export decorators
decorators = ['ether_pub', 'ether_sub', 'ether_init', 'ether_save', 'ether_cleanup']
__all__ = ['ether'] + decorators

