import logging
from pathlib import Path
import os
import uuid
from datetime import datetime

# Standard ports for Ether communication
_ETHER_SUB_PORT = 5555  # subscribe to this port
_ETHER_PUB_PORT = 5556  # publish to this port

# Log directory structure
_LOG_DIR = Path("logs")

def _ensure_log_dir(path: Path):
    """Ensure log directory exists"""
    path.mkdir(parents=True, exist_ok=True)

def _get_logger(process_name: str, instance_name: str = None, log_level=logging.INFO) -> logging.Logger:
    """Get or create a logger with file and console handlers
    
    Args:
        process_name: Name of the process/class (e.g., "DataGenerator", "Ether")
        instance_name: Optional instance name (e.g., "generator1", "processor2x")
        log_level: Logging level
    """
    # Create logger with hierarchical name
    logger_name = f"{process_name}:{instance_name}" if instance_name else process_name
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    
    # Don't add handlers if they already exist
    if logger.handlers:
        return logger
    
    # Create formatters with timestamps
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler
    class_name = process_name.split('.')[-1]  # Get last part of process name
    class_dir = _LOG_DIR / class_name
    _ensure_log_dir(class_dir)
    
    if instance_name:
        # Instance-specific log file with timestamp and UUID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        instance_id = str(uuid.uuid4())[:8]
        log_file = class_dir / f"{instance_name}_{timestamp}_{instance_id}.log"
    else:
        # Class-level log file (persistent across runs)
        log_file = class_dir / f"_{class_name}_class.log"
    
    file_handler = logging.FileHandler(log_file, mode='a' if not instance_name else 'w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Log the file location for reference
    logger.debug(f"Logging to file: {log_file}")
    
    return logger