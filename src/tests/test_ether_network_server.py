from ether import ether
from ether._internal._config import EtherConfig, EtherInstanceConfig, EtherNetworkConfig
from ether import ether_save, ether_get
from ether.utils import get_ether_logger
import socket
import urllib.request
import json
import time

def get_public_ip():
    """Get the public IP address using an IP lookup service"""
    try:
        # Try multiple IP lookup services in case one fails
        services = [
            'https://api.ipify.org?format=json',
            'https://api.myip.com',
            'https://api.ip.sb/jsonip'
        ]
        
        for service in services:
            try:
                response = urllib.request.urlopen(service, timeout=2)
                data = json.loads(response.read())
                # Different services use different key names
                ip = data.get('ip') or data.get('ipAddress')
                if ip:
                    return ip
            except:
                continue
                
        # Fallback to local IP if public IP lookup fails
        return get_local_ip()
        
    except Exception as e:
        print(f"Error getting public IP: {e}")
        return None

def get_local_ip():
    """Get the local IP address"""
    # This gets the local IP that would be used to connect to the internet
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

    # Get both local and public IPs
    local_ip = get_local_ip()
    public_ip = get_public_ip()
    
    logger.info(f"Local IP address: {local_ip}")
    logger.info(f"Public IP address: {public_ip}")
    
    # Use public IP if available, otherwise fall back to local IP
    server_ip = public_ip or local_ip
    logger.info(f"Using IP address: {server_ip}")
    
    network_config = EtherNetworkConfig(
        host=server_ip,  # Use public/local IP
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
        
        time.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
    finally:
        ether.shutdown()

if __name__ == "__main__":
    run_server() 