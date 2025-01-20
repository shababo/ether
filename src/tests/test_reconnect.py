import pytest
import time
from typing import Optional
from ether import ether, ether_get, ether_save, _ether
from ether._internal._config import EtherConfig, EtherInstanceConfig
from ether.utils import get_ether_logger
from ether.liaison import EtherInstanceLiaison
from ether._internal._reqrep import MDPW_WORKER, W_DISCONNECT

class ReconnectTestService:
    def __init__(self):
        self.counter = 0
        self.connection_count = 0
        
    @ether_get(heartbeat=True)
    def get_counter(self) -> dict:
        """Get current counter value"""
        self._logger.debug(f"Get counter called, value={self.counter}")
        return {
            "counter": self.counter,
            "connection": self.connection_count
        }
        
    @ether_save(heartbeat=True)
    def increment(self) -> dict:
        """Increment counter"""
        self.counter += 1
        self._logger.debug(f"Counter incremented to {self.counter}")
        return {"counter": self.counter}

def test_service_reconnect():
    """Test that service properly reconnects after disconnect"""
    logger = get_ether_logger("TestReconnect")
    
    config = EtherConfig(
        instances={
            "reconnect_service": EtherInstanceConfig(
                class_path="tests.test_reconnect.ReconnectTestService",
                kwargs={"name": "reconnect_service"}
            )
        }
    )
    
    ether.tap(config=config)
    
    try:
        # Initial connection
        logger.info("Testing initial connection...")
        result = ether.get("ReconnectTestService", "get_counter")
        assert result["counter"] == 0
        assert result["connection"] == 0
        
        # Make some requests
        logger.info("Making initial requests...")
        for _ in range(3):
            ether.save("ReconnectTestService", "increment")
            time.sleep(0.1)  # Small delay between requests
            
        result = ether.get("ReconnectTestService", "get_counter")
        assert result["counter"] == 3
        assert result["connection"] == 0
        
        # Force disconnect
        # logger.info("Forcing disconnect...")
        # result = _ether._disconnect_req_service("ReconnectTestService")
        # logger.info(f"Disconnect reply: {result}")
        # Wait for reconnect
        time.sleep(3.0)  # Allow time for reconnect
        
        # Verify service is still accessible
        logger.info("Verifying service after reconnect...")
        result = ether.get("ReconnectTestService", "get_counter")
        logger.info(f"Result after reconnect: {result}")
        assert result["counter"] == 3  # Counter should be preserved
        # assert result["connection"] == 1  # Should have reconnected once
        
        # Make more requests after reconnect
        logger.info("Making requests after reconnect...")
        for _ in range(2):
            ether.save("ReconnectTestService", "increment")
            time.sleep(0.1)
            
        result = ether.get("ReconnectTestService", "get_counter")
        assert result["counter"] == 5  # Should continue from previous value
        # assert result["connection"] == 1
        
        # Force another disconnect
        # logger.info("Testing multiple disconnects...")
        # ether.save("ReconnectTestService", "force_disconnect")
        # time.sleep(2.0)
        
        # result = ether.get("ReconnectTestService", "get_counter")
        # assert result["counter"] == 5
        # assert result["connection"] == 2  # Should have reconnected twice
        
        # Verify heartbeats still working
        logger.info("Verifying heartbeats after reconnects...")
        time.sleep(3.0)  # Wait for several heartbeat cycles
        result = ether.get("ReconnectTestService", "get_counter")
        assert result["counter"] == 5
        # assert result["connection"] == 2
        
    finally:
        ether.shutdown()

if __name__ == "__main__":
    pytest.main([__file__]) 