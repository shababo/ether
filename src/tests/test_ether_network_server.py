from ether import ether
from ether._internal._config import EtherConfig, EtherInstanceConfig, EtherNetworkConfig
from ether import ether_save, ether_get
from ether.utils import get_ether_logger
import socket
import requests
from typing import Optional

def get_ip_address(use_public: bool = True) -> str:
    """Get IP address
    
    Args:
        use_public: If True, attempts to get public IP. Falls back to local IP if failed.
    
    Returns:
        IP address as string
    """
    if use_public:
        try:
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
            
            # If all services fail, fall back to local IP
            logger.warning("Could not get public IP, falling back to local IP")
        except Exception as e:
            logger.warning(f"Error getting public IP: {e}, falling back to local IP")
    
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

class NetworkTestService:
    def __init__(self):
        self.data = {}
    
    @ether_save
    def save_item(self, id: int, value: str) -> dict:
        """Save an item to the data store"""
        self.data[id] = value
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
        pubsub_frontend_port=5555,
        pubsub_backend_port=5556,
        reqrep_frontend_port=5559,
        reqrep_backend_port=5560,
        redis_port=6379
    )
    
    config = EtherConfig(
        network=network_config,
        instances={
            "network_test": EtherInstanceConfig(
                class_path="tests.test_ether_network_server.NetworkTestService",
                kwargs={"ether_name": "network_test"}
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