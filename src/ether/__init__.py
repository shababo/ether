import logging
from ._ether import (
    EtherMixin, ether_pub, ether_sub,
    _get_logger, _EtherPubSubProxy, _proxy_manager
)

_initialized = False

def init():
    """Initialize the Ether messaging system.
    
    This starts the proxy process if it's not already running.
    Must be called once before using any Ether functionality.
    """
    global _initialized
    if not _initialized:
        _proxy_manager.start_proxy()
        _initialized = True

# Export public interface
__all__ = [
    'EtherMixin',
    'ether_pub',
    'ether_sub',
    'init'
]

