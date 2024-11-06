import logging
import zmq
import time
from typing import Optional, Union
from ._ether import (
    ether_pub, ether_sub, EtherRegistry,
    _get_logger
)
from ._daemon import daemon_manager

_ether_initialized = False

def ether_init(config: Optional[Union[str, dict]] = None):
    """Initialize the Ether messaging system."""
    global _ether_initialized
    if not _ether_initialized:
        # Clean up any existing ZMQ contexts
        zmq.Context.instance().term()
        # Start daemon (which manages Redis and PubSub)
        daemon_manager  # Access singleton to ensure initialization
        # Process any pending classes
        EtherRegistry.process_pending_classes()
        # Mark as initialized
        _ether_initialized = True
        logging.debug("Ether system initialized")
        time.sleep(2.0)

# Export public interface
__all__ = ['ether_pub', 'ether_sub', 'ether_init']

