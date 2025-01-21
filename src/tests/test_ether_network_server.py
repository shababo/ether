from ether import ether
from ether._internal._config import EtherConfig, EtherInstanceConfig, EtherNetworkConfig
from ether import ether_save, ether_get, ether_start
from ether.utils import get_ether_logger, get_ip_address
import socket
import requests
from typing import Optional


class NetworkTestService:
    def __init__(self):
        self.data = {}

    @ether_start
    def reset(self):
        self.data = {}
        self._logger.info(f"Resetting data store, data: {self.data}")
    
    @ether_save
    def save_item(self, id: int, value: str) -> dict:
        """Save an item to the data store"""
        self.data[id] = value
        self._logger.info(f"Saved item {id} with value {value}, data: {self.data}")
        return {"id": id, "value": value}

    @ether_get
    def get_item(self, id: int) -> dict:
        """Get a single item by ID"""
        if id not in self.data:
            return None
        return {"id": id, "value": self.data[id]}

def run_server(host: str = None):
    """Run the Ether server
    
    Args:
        host: IP to bind to.
    """
    logger = get_ether_logger("EtherNetworkServer")
    logger.info(f"Starting server on {host}")

    # Get the public IP address
    ip = get_ip_address(use_public=True)
    logger.info(f"Server IP address: {ip}")
    
    network_config = EtherNetworkConfig(
        host=ip,  # Use public/local IP
    )
    
    config = EtherConfig(
        network=network_config,
        instances={
            "network_test": EtherInstanceConfig(
                class_path="tests.test_ether_network_server.NetworkTestService",
                kwargs={"ether_name": "network_test_data_service"}
            )
        }
    )
    
    try:
        ether.tap(config=config)
        logger.info("Server running. Press Ctrl+C to stop.")
        
        # Keep the server running
        while True:
            input()
            
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
    finally:
        ether.shutdown()

if __name__ == "__main__":
    run_server() 