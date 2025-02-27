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
from .config import EtherConfig


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

    # ether public API
    # just some sugar to make the UX better and to codify common actions into topics
    pub = staticmethod(_pub)
    request = staticmethod(_request)
    get = staticmethod(functools.partial(_request, request_type="get"))
    save = staticmethod(functools.partial(_request, request_type="save"))
    save_all = staticmethod(functools.partial(_pub, {}, topic="Ether.save_all"))
    start = staticmethod(functools.partial(_pub, {}, topic="Ether.start"))
    cleanup = staticmethod(functools.partial(_pub, {}, topic="Ether.cleanup"))
    shutdown = staticmethod(functools.partial(_pub, {}, topic="Ether.shutdown"))
    pause = staticmethod(functools.partial(_pub, {}, topic="Ether.pause"))
    resume = staticmethod(functools.partial(_pub, {}, topic="Ether.resume"))
    get_session_metadata = _ether.get_session_metadata
    
    _initialized = False
    _instance = None
    _within_process_instances = []
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Ether, cls).__new__(cls)
        return cls._instance

    def tap(self, config: Optional[Union[str, dict, EtherConfig]] = None, restart: bool = False, allow_host: bool = True, ether_run: bool = False):
        """Initialize the Ether messaging system."""

        if self._initialized and restart:
            print("Force reinitializing Ether system...")
            self.shutdown()
            self._initialized = False
            
        if not self._initialized:
            # Start ether
            _ether.start(config=config, restart=restart, allow_host=allow_host, ether_run=ether_run)
        
            # Mark as initialized
            self._initialized = True
            
            # Register single cleanup handler
            atexit.register(self.shutdown)

        time.sleep(1.1)

    def shutdown(self):
        self.cleanup()
        for instance in self._within_process_instances:
            # self._logger.debug(f"Shutting down instance: {instance.name}-{instance.id}")
            instance.cleanup()

        self.save_all()
        time.sleep(2.0)
        _ether.shutdown()
        self._initialized = False

    @property
    def ether_id(self):
        return _ether._ether_id
 

## Export public interface
# instantiate singleton for API
ether = Ether()
# export decorators
decorators = ['ether_pub', 'ether_sub', 'ether_init', 'ether_save', 'ether_cleanup', 'ether_start', 'ether_get', 'ether_save_all', 'ether_shutdown']
__all__ = ['ether'] + decorators

