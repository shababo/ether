from ether import ether
from ether._internal._config import EtherConfig, EtherNetworkConfig
from ether.utils import get_ether_logger

def run_client(server_host: str):
    """Run the Ether client
    
    Args:
        server_host: IP address of the Ether server
    """
    logger = get_ether_logger("EtherNetworkClient")
    logger.info(f"Connecting to server at {server_host}")
    
    network_config = EtherNetworkConfig(
        host=server_host,  # Connect to server for ZMQ
        pubsub_frontend_port=5555,
        pubsub_backend_port=5556,
        reqrep_frontend_port=5559,
        reqrep_backend_port=5560,
        redis_port=6379
    )
    
    config = EtherConfig(network=network_config)
    
    try:
        ether.tap(config=config)
        
        # Test connection by saving and retrieving data
        save_reply = ether.save(
            "NetworkTestService",
            "save_item",
            params={
                "id": 1,
                "value": "test_from_remote_client"
            }
        )
        logger.info(f"Save reply: {save_reply}")
        
        get_reply = ether.get(
            "NetworkTestService",
            "get_item",
            params={"id": 1}
        )
        logger.info(f"Get reply: {get_reply}")
        
        if get_reply and get_reply.get("value") == "test_from_remote_client":
            logger.info("Network test successful!")
        else:
            logger.error("Network test failed!")
            
    finally:
        ether.shutdown()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python test_ether_network_client.py <server_ip>")
        sys.exit(1)
    run_client(sys.argv[1]) 