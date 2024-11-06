import logging
import zmq
import time
from typing import Optional, Union, List, Dict
from multiprocessing import Process
from ._ether import (
    ether_pub, ether_sub, EtherRegistry,
    _get_logger
)
from ._daemon import daemon_manager
from ._config import EtherConfig
import atexit

_ether_initialized = False
_instance_processes: Dict[str, Process] = {}

def ether_init(config: Optional[Union[str, dict, EtherConfig]] = None):
    """Initialize the Ether messaging system."""
    global _ether_initialized, _instance_processes
    if not _ether_initialized:
        # Clean up any existing ZMQ contexts
        zmq.Context.instance().term()
        
        # Start daemon (which manages Redis and PubSub)
        daemon_manager.start()  # Actually start the daemon here
        
        # Process any pending classes
        EtherRegistry.process_pending_classes()
        
        # Mark as initialized
        _ether_initialized = True
        logging.debug("Ether system initialized")
        time.sleep(2.0)
        
        # Handle configuration if provided - do this last after classes are processed
        if config is not None:
            if isinstance(config, str):
                config = EtherConfig.from_yaml(config)
            elif isinstance(config, dict):
                config = EtherConfig.model_validate(config)
            
            # Launch configured instances
            _instance_processes = config.launch_instances()

def stop_instance(instance_id: str):
    """Stop a specific instance"""
    if instance_id in _instance_processes:
        process = _instance_processes[instance_id]
        process.terminate()
        process.join(timeout=5)
        del _instance_processes[instance_id]

def stop_all_instances():
    """Stop all running instances"""
    for instance_id in list(_instance_processes.keys()):
        stop_instance(instance_id)

# Register cleanup on exit
@atexit.register
def _cleanup_instances():
    stop_all_instances()

# Export public interface
__all__ = ['ether_pub', 'ether_sub', 'ether_init', 'stop_instance', 'stop_all_instances']

