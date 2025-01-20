import pytest
import time
from typing import List, Optional
from ether import ether
from ether._internal._config import EtherConfig, EtherInstanceConfig
from ether import ether_get, ether_save  
from ether.utils import get_ether_logger
import zmq
from ether.liaison import EtherInstanceLiaison

class HeartbeatService:
    def __init__(self):
        self.data = {}
        self.counter = 0
        self.slow_until = 0
    
    @ether_get(heartbeat=True)  # Changed from _ether_get
    def get_count(self) -> int:
        """Get current counter value"""
        # Add artificial delay if we're in slow mode
        if time.time() < self.slow_until:
            time.sleep(0.1)  # Sleep 100ms
        return self.counter
    
    @ether_save(heartbeat=True)  # Changed from _ether_save
    def increment(self, amount: int = 1) -> dict:
        """Increment counter by amount"""
        self.counter += amount
        return {"counter": self.counter}
    
    @ether_save(heartbeat=True)  # Changed from _ether_save
    def set_slow(self, duration: float = 1.0) -> dict:
        """Make the service slow for a duration"""
        self.slow_until = time.time() + duration
        return {"slow_until": self.slow_until}

def test_heartbeat_and_reconnect():
    logger = get_ether_logger("TestHeartbeat")
    
    # Setup retry counting
    retry_count = 0
    original_recv_multipart = zmq.Socket.recv_multipart
    
    def counting_recv_multipart(self, *args, **kwargs):
        nonlocal retry_count
        try:
            return original_recv_multipart(self, *args, **kwargs)
        except zmq.error.Again:
            retry_count += 1
            raise
            
    zmq.Socket.recv_multipart = counting_recv_multipart
    
    config = EtherConfig(
        instances={
            "heartbeat_service": EtherInstanceConfig(
                class_path="tests.test_ether_reqrep_advanced.HeartbeatService",
                kwargs={"name": "heartbeat_service"}
            )
        }
    )
    
    ether.tap(config=config)
    
    try:
        # Test basic request-reply with heartbeat
        result = ether.get("HeartbeatService", "get_count")
        assert result == 0
        
        # Test save with heartbeat
        reply = ether.save(
            "HeartbeatService", 
            "increment", 
            params={"amount": 5},
        )
        assert reply["status"] == "success"
        assert reply["result"]["counter"] == 5
        
        # Verify counter was updated
        result = ether.get("HeartbeatService", "get_count")
        assert result == 5
        
        # Let heartbeats happen
        logger.info("Waiting for heartbeats...")
        time.sleep(3.0)  # Allow multiple heartbeat cycles
        
        # Test another request after heartbeats
        result = ether.get("HeartbeatService", "get_count")
        assert result == 5
        
        # Make service slow
        reply = ether.get(
            "HeartbeatService",
            "set_slow",
            params={"duration": 1.0},
            request_type="save"
        )
        assert reply["status"] == "success"
        
        # Test timeout and retry behavior
        logger.info("Testing timeout handling...")
        retry_count = 0  # Reset counter
        
        start_time = time.time()
        try:
            result = ether.get(
                "HeartbeatService", 
                "get_count", 
                timeout=50  # 50ms timeout
            )
        except Exception as e:
            logger.info(f"Request failed as expected: {e}")
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        logger.info(f"Request had {retry_count} retries over {elapsed:.3f}s")
        assert retry_count > 1, f"Expected multiple retries, got {retry_count}"
        
        # Verify service still works
        result = ether.get("HeartbeatService", "get_count")
        assert result == 5  # Service should still be working
        
        # Test reconnection
        logger.info("Testing reconnection...")
        
        # Force reconnect by closing socket
        liaison = EtherInstanceLiaison()
        instances = liaison.get_active_instances()
        
        # Find our service instance
        service_id = None
        for instance_id, info in instances.items():
            if info['class'] == 'HeartbeatService':
                service_id = instance_id
                break
                
        assert service_id is not None, "Could not find HeartbeatService instance"
        
        # Send disconnect command to broker
        logger.info("Sending disconnect command...")
        try:
            ether.get(
                "HeartbeatService",
                "get_count",
                timeout=1  # Very short timeout to force disconnect
            )
        except zmq.error.Again:
            logger.info("Got expected timeout, service should reconnect")
        
        time.sleep(0.5)  # Allow more time for reconnect
        
        # Verify service still works after reconnect
        result = ether.get("HeartbeatService", "get_count")
        assert result == 5  # Service should still work
        
        # Verify instance is still registered
        instances = liaison.get_active_instances()
        assert service_id in instances, "Service instance lost after reconnect"
        assert instances[service_id]['class'] == 'HeartbeatService'
        
    finally:
        # Restore original method
        zmq.Socket.recv_multipart = original_recv_multipart
        ether.shutdown()

if __name__ == '__main__':
    pytest.main([__file__]) 