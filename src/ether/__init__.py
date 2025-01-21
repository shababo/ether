import functools
import logging
from typing import Optional, Union, Dict
from pydantic import BaseModel
import atexit
import time
import uuid

from .decorators import ether_pub, ether_sub, ether_init, ether_save, ether_cleanup, ether_start, ether_get, ether_save_all, ether_shutdown
from .utils import get_ether_logger
from ._internal._ether import _ether
from ._internal._config import EtherConfig


def _pub(data: Union[Dict, BaseModel] = None, topic: str = None):
    """Publish data to a topic
    
    Args:
        data: Data to publish (dict or Pydantic model)
        topic: Topic to publish to
    """
    data = data or {}
    topic = topic or "general"
    _ether.publish(data, topic)

def _request(service_class: str, method_name: str, params=None, request_type="get", timeout=2500):
    """Make a request to a service
    
    Args:
        service_class: Name of the service class
        method_name: Name of the method to call
        params: Parameters to pass to the method
        request_type: Type of request ("get" or "save")
        timeout: Request timeout in milliseconds
    """
    params = params or {}
    return _ether.request(service_class, method_name, params, request_type, timeout)

# Public singleton instance of Ether API
class Ether:
    pub = staticmethod(_pub)
    request = staticmethod(_request)
    get = staticmethod(functools.partial(_request, request_type="get"))
    save = staticmethod(functools.partial(_request, request_type="save"))
    save_all = staticmethod(functools.partial(_pub, {}, topic="Ether.save_all"))
    start = staticmethod(functools.partial(_pub, {}, topic="Ether.start"))
    cleanup = staticmethod(functools.partial(_pub, {}, topic="Ether.cleanup"))
    shutdown = staticmethod(functools.partial(_pub, {}, topic="Ether.shutdown"))
    _initialized = False
    _instance = None
    _ether_id = str(uuid.uuid4())
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Ether, cls).__new__(cls)
        return cls._instance

    def tap(self, config: Optional[Union[str, dict, EtherConfig]] = None, restart: bool = False, discovery: bool = True):
        """Initialize the Ether messaging system."""
        # Only set root logger level without adding a handler
        logging.getLogger().setLevel(logging.DEBUG)

        if self._initialized and restart:
            print("Force reinitializing Ether system...")
            self.shutdown()
            self._initialized = False
            
        if not self._initialized:
            # Start ether
            _ether.start(ether_id=self._ether_id, config=config, restart=restart, discovery=discovery)
        
            # Mark as initialized
            self._initialized = True
            
            # Register single cleanup handler
            atexit.register(self.shutdown)

        time.sleep(1.1)

    def shutdown(self):
        self.cleanup()
        self.save_all()
        _ether.shutdown()
 

## Export public interface
# instantiate singleton for API
ether = Ether()
# export decorators
decorators = ['ether_pub', 'ether_sub', 'ether_init', 'ether_save', 'ether_cleanup', 'ether_start', 'ether_get', 'ether_save_all', 'ether_shutdown']
__all__ = ['ether'] + decorators

