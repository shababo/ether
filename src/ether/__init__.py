import functools
import logging
from typing import Optional, Union, Dict
from pydantic import BaseModel
import atexit
import time
import multiprocessing
import os
import uuid
# first thing we do is figure out if we are the first ether process,
# and if so, we start the discovery service and later will init pubsub/redis/etc

from .decorators import ether_pub, ether_sub, ether_init, ether_save, ether_cleanup
from .utils import _get_logger
from ._internal._ether import _ether
from ._internal._config import EtherConfig


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
    _ether_id = str(uuid.uuid4())
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Ether, cls).__new__(cls)
        return cls._instance

    def init(self, config: Optional[Union[str, dict, EtherConfig]] = None, restart: bool = False):
        """Initialize the Ether messaging system."""
        # Only set root logger level without adding a handler
        logging.getLogger().setLevel(logging.DEBUG)

        if self._initialized and restart:
            print("Force reinitializing Ether system...")
            self.shutdown()
            self._initialized = False
            
        if not self._initialized:
            # Start ether
            _ether.start(ether_id=self._ether_id, config=config, restart=restart)
        
            # Mark as initialized
            self._initialized = True
            print("Ether system initialized")
            
            # Register single cleanup handler
            atexit.register(self.shutdown)

        time.sleep(0.002)

    def shutdown(self):
        self.cleanup()
        self.save()
        _ether.shutdown()
 

## Export public interface
# instantiate singleton for API
ether = Ether()
# export decorators
decorators = ['ether_pub', 'ether_sub', 'ether_init', 'ether_save', 'ether_cleanup']
__all__ = ['ether'] + decorators

