import time
from typing import List
import socket
from ether import ether
from ether._internal._config import EtherConfig, EtherInstanceConfig, EtherNetworkConfig
from ether import ether_save, ether_get
from ether.utils import get_ether_logger

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

def test_ether_network_communication():
    """Test Ether communication between different network configurations"""
    logger = get_ether_logger("TestEtherNetwork")
    
    # Get the local IP address
    local_ip = get_local_ip()
    logger.info(f"Local IP address: {local_ip}")
    
    # Create network configs for "server" and "client"
    server_network = EtherNetworkConfig(
        host=local_ip,  # Use actual IP for server
        pubsub_frontend_port=5555,
        pubsub_backend_port=5556,
        reqrep_frontend_port=5559,
        reqrep_backend_port=5560,
        redis_port=6379
    )
    
    client_network = EtherNetworkConfig(
        host=local_ip,  # Connect to server IP
        pubsub_frontend_port=5555,
        pubsub_backend_port=5556,
        reqrep_frontend_port=5559,
        reqrep_backend_port=5560,
        redis_port=6379
    )
    
    # Create server config
    server_config = EtherConfig(
        network=server_network,
        instances={
            "network_test": EtherInstanceConfig(
                class_path="tests.test_ether_network.NetworkTestService",
                kwargs={"ether_name": "network_test"}
            )
        }
    )
    
    # Create client config (no instances, just network config)
    client_config = EtherConfig(
        network=client_network
    )
    
    # Start server Ether instance
    server_ether = ether.__class__()  # Create new instance for server
    server_ether.tap(config=server_config)
    logger.info("Server Ether instance started")
    
    # Give server time to start
    time.sleep(2)
    
    # Start client Ether instance
    client_ether = ether.__class__()  # Create new instance for client
    client_ether.tap(config=client_config)
    logger.info("Client Ether instance started")
    
    try:
        # Test saving item through network
        save_reply = client_ether.save(
            "NetworkTestService", 
            "save_item", 
            params={
                "id": 1,
                "value": "test_network_value"
            }
        )
        logger.info(f"Save reply: {save_reply}")
        assert save_reply["status"] == "success"
        
        # Test getting item through network
        get_reply = client_ether.get(
            "NetworkTestService", 
            "get_item", 
            params={"id": 1}
        )
        logger.info(f"Get reply: {get_reply}")
        assert get_reply["value"] == "test_network_value"
        
    finally:
        # Cleanup
        client_ether.shutdown()
        server_ether.shutdown()

if __name__ == "__main__":
    test_ether_network_communication() 