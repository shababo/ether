import time

from ether import ether, ether_start, ether_get
from ether._internal._config import EtherConfig, EtherNetworkConfig, EtherInstanceConfig
from ether.utils import get_ether_logger

class NetworkClient:

    @ether_start
    def run(self):

        # Test connection by saving and retrieving data
        save_reply = ether.save(
            "NetworkTestService",
            "save_item",
            params={
                "id": 1,
                "value": "test_from_remote_client"
            }
        )
        self._logger.info(f"Save reply: {save_reply}")
        
        get_reply = ether.get(
            "NetworkTestService",
            "get_item",
            params={"id": 1}
        )
        self._logger.info(f"Get reply: {get_reply}")
        
        if get_reply and get_reply.get("value") == "test_from_remote_client":
            self._logger.info("Network test successful!")
            return {"status": "success"}
        else:
            self._logger.error("Network test failed!")
            return {"status": "error"}
            

def run_client(server_host: str):
    """Run the Ether client
    
    Args:
        server_host: IP address of the Ether server
    """
    logger = get_ether_logger("TestNetworkClient")
    logger.info(f"Connecting to server at {server_host}")
    
    network_config = EtherNetworkConfig(
        host=server_host,  # Connect to server for ZMQ
    )
    
    config = EtherConfig(
        instances={
            "network_test_client": EtherInstanceConfig(
                class_path="tests.test_ether_network_client.NetworkClient",
                kwargs={"ether_name": "network_test_client"}
            )
        },
        network=network_config)
    
    ether.tap(config=config)
    time.sleep(2.0)

    # ether.get("NetworkClient", "run")    
    ether.start()
    time.sleep(5.0)
    
    ## RETEST WITHOUT INSTANCE
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

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python test_ether_network_client.py <server_ip>")
        sys.exit(1)
    run_client(sys.argv[1]) 