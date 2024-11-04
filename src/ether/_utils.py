import logging

# Standard ports for Ether communication
_ETHER_SUB_PORT = 5555  # subscribe to this port
_ETHER_PUB_PORT = 5556  # publish to this port

def _get_logger(process_name, log_level=logging.INFO):
    """Get or create a logger with a single handler"""
    logger = logging.getLogger(process_name)
    logger.setLevel(log_level)
    logger.propagate = True
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add a single handler
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger