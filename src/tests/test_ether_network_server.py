from ether import ether
from ether._internal._config import EtherConfig, EtherInstanceConfig, EtherNetworkConfig
from ether import ether_save, ether_get
from ether.utils import get_ether_logger

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

def run_server(host: str = "0.0.0.0"):
    """Run the Ether server
    
    Args:
        host: IP to bind to. Use 0.0.0.0 to accept connections from any IP
    """
    logger = get_ether_logger("EtherNetworkServer")
    logger.info(f"Starting server on {host}")
    
    network_config = EtherNetworkConfig(
        host="0.0.0.0",  # Accept connections from anywhere
        redis_host="0.0.0.0",  # Allow Redis connections from anywhere
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