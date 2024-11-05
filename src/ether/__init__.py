import logging
import zmq
import time
from ._ether import (
    ether_pub, ether_sub, EtherRegistry,
    _get_logger,
    _ETHER_PUB_PORT, _ETHER_SUB_PORT
)

from ._proxy import proxy_manager as _proxy_manager
_initialized = False

def ether_init():
    """Initialize the Ether messaging system."""
    global _initialized
    if not _initialized:
        # Clean up any existing ZMQ contexts first
        zmq.Context.instance().term()
        # Start proxy if not already running
        _proxy_manager.start_proxy()
        # Process any pending classes
        EtherRegistry.process_pending_classes()
        # Mark as initialized
        _initialized = True
        logging.debug("Ether system initialized")
        time.sleep(2.0)

# Export public interface
__all__ = [
    'ether_pub',
    'ether_sub',
    'ether_init'
]

