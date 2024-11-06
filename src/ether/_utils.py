import logging
import os
from pathlib import Path

# Standard ports for Ether communication
_ETHER_SUB_PORT = 5555  # subscribe to this port
_ETHER_PUB_PORT = 5556  # publish to this port

# Log directory structure
_LOG_DIR = Path("logs")
_DAEMON_LOG_DIR = _LOG_DIR / "daemon"
_INSTANCES_LOG_DIR = _LOG_DIR / "instances"

def _ensure_log_dir(path: Path):
    """Ensure log directory exists"""
    path.mkdir(parents=True, exist_ok=True)

def _get_logger(process_name: str, instance_name: str = None, log_level=logging.INFO) -> logging.Logger:
    """Get or create a logger with file and console handlers
    
    Args:
        process_name: Name of the process (e.g., "DataGenerator", "Proxy")
        instance_name: Optional instance name for instances
        log_level: Logging level
    """
    # Create logger
    logger = logging.getLogger(f"{process_name}:{instance_name or ''}")
    logger.setLevel(log_level)
    
    # Don't add handlers if they already exist
    if logger.handlers:
        return logger
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s - %(name)s - %(message)s'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Create file handler
    if process_name in ["Proxy", "Redis", "EtherMonitor"]:
        # Daemon processes
        _ensure_log_dir(_DAEMON_LOG_DIR)
        log_file = _DAEMON_LOG_DIR / f"{process_name.lower()}.log"
    else:
        # Instance processes
        if instance_name:
            instance_dir = _INSTANCES_LOG_DIR / instance_name
            _ensure_log_dir(instance_dir)
            log_file = instance_dir / f"{instance_name}.log"
        else:
            _ensure_log_dir(_INSTANCES_LOG_DIR)
            log_file = _INSTANCES_LOG_DIR / f"{process_name.lower()}.log"
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    return logger