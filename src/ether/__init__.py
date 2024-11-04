import logging
from ._ether import (
    ether_pub, ether_sub,
    _get_logger, _EtherPubSubProxy, _proxy_manager,
    _ETHER_PUB_PORT, _ETHER_SUB_PORT
)
from ._registry import EtherRegistry

_initialized = False

def ether_init():
    """Initialize the Ether messaging system."""
    global _initialized
    if not _initialized:
        # Start proxy if not already running
        _proxy_manager.start_proxy()
        # Process any pending classes
        EtherRegistry.process_pending_classes()
        # Mark as initialized
        _initialized = True
        logging.debug("Ether system initialized")

# Export public interface
__all__ = [
    'ether_pub',
    'ether_sub',
    'ether_init'
]

