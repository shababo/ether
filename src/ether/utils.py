
import logging
from pathlib import Path
from datetime import datetime


# Standard ports for Ether communication
_ETHER_SUB_PORT = 5555  # subscribe to this port
_ETHER_PUB_PORT = 5556  # publish to this port

# Log directory structure
_LOG_DIR = Path("logs")

def _ensure_log_dir(path: Path):
    """Ensure log directory exists"""
    path.mkdir(parents=True, exist_ok=True)

def _get_logger(
    process_name: str, 
    run_id: str = None,
    instance_name: str = None, 
    console_level=logging.DEBUG,
    file_level=logging.DEBUG
) -> logging.Logger:
    """Get or create a logger with file and console handlers"""

    # Create logger with hierarchical name
    logger_name = f"{process_name}:{instance_name}" if instance_name else process_name
    logger = logging.getLogger(logger_name)
    logger.setLevel(min(console_level, file_level))
    
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
    console_handler.setLevel(console_level)
    logger.addHandler(console_handler)
    
    # Get the current run directory from context
    run_log_dir = _LOG_DIR
    
    # Create file handler
    class_name = process_name.split('.')[-1]  # Get last part of process name
    
    # Determine log directory structure within run directory
    if class_name.startswith('Ether'):
        # All Ether classes go in Ether directory
        log_dir = run_log_dir / "Ether" / class_name
    else:
        # Other classes get their own directory
        log_dir = run_log_dir / class_name
    
    _ensure_log_dir(log_dir)
    
    if instance_name:
        # Instance-specific log file
        log_file = log_dir / f"{instance_name}.log"
    else:
        # Class-level log file
        log_file = log_dir / "class.log"
    
    file_handler = logging.FileHandler(log_file, mode='a' if not instance_name else 'w')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(file_level)
    logger.addHandler(file_handler)
    
    # Log the file location for reference
    logger.debug(f"Logging to file: {log_file}")
    
    return logger