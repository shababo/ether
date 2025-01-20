import logging
from pathlib import Path
import os
import socket
import requests
# Standard ports for Ether communication
_ETHER_SUB_PORT = 5555  # subscribe to this port
_ETHER_PUB_PORT = 5556  # publish to this port

# Log directory structure
_LOG_DIR = Path("logs")

def _ensure_log_dir(path: Path):
    """Ensure log directory exists"""
    path.mkdir(parents=True, exist_ok=True)

def get_ether_logger(
    process_name: str, 
    run_id: str = None,
    instance_name: str = None, 
    console_level=logging.INFO,
    file_level=logging.DEBUG
) -> logging.Logger:
    """Get or create a logger with file and console handlers"""

    # Create logger with hierarchical name
    logger_name = f"{process_name}:{instance_name}" if instance_name else process_name
    logger_name = f"{logger_name}:{os.getpid()}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(min(console_level, file_level))
    
    # Don't add handlers if they already exist
    if logger.handlers:
        return logger
    
    # Create formatters with timestamps
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
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
    
    file_handler = logging.FileHandler(log_file, mode='a')  # Always append
    file_handler.setFormatter(formatter)
    file_handler.setLevel(file_level)
    logger.addHandler(file_handler)
    
    # Log the file location for reference
    logger.debug(f"Logging to file: {log_file}")
    
    return logger

def get_ip_address(use_public: bool = True) -> str:
    """Get IP address
    
    Args:
        use_public: If True, attempts to get public IP. Falls back to local IP if failed.
    
    Returns:
        IP address as string
    """
    if use_public:
        # Try multiple IP lookup services in case one is down
        services = [
            "https://api.ipify.org",
            "https://api.my-ip.io/ip",
            "https://checkip.amazonaws.com",
        ]
        for service in services:
            try:
                response = requests.get(service, timeout=2)
                if response.status_code == 200:
                    return response.text.strip()
            except:
                continue
        

    # Get local IP as fallback
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't need to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP