import pytest
import time
from typing import List, Optional
from ether import ether
from ether._internal._config import EtherConfig, EtherInstanceConfig
from ether import ether_get, ether_save
from ether.utils import get_ether_logger
from ether.liaison import EtherInstanceLiaison

class FlowControlService:
    def __init__(self):
        self.requests = []
        self.replies = []
    
    @ether_get(heartbeat=True)
    def get_request_history(self) -> List[dict]:
        """Get history of requests and replies"""
        return {
            "requests": self.requests,
            "replies": self.replies
        }
    
    @ether_save(heartbeat=True)
    def process_request(self, data: str) -> dict:
        """Process a request and track message flow"""
        # Store request info
        self.requests.append({
            "data": data,
            "time": time.time()
        })
        
        # Create reply
        reply = {
            "processed_data": f"Processed: {data}",
            "time": time.time()
        }
        self.replies.append(reply)
        
        return reply

def test_message_flow_control():
    logger = get_ether_logger("TestFlowControl")
    
    config = EtherConfig(
        instances={
            "flow_service": EtherInstanceConfig(
                class_path="tests.test_ether_reqrep_flow.FlowControlService",
                kwargs={"name": "flow_service"}
            )
        }
    )
    
    ether.tap(config=config)
    
    try:
        # Make several requests
        test_data = ["request1", "request2", "request3"]
        replies = []
        
        for data in test_data:
            reply = ether.save(
                "FlowControlService",
                "process_request",
                params={"data": data},
            )
            assert reply["status"] == "success"
            replies.append(reply["result"])
        
        # Get request history
        history = ether.get("FlowControlService", "get_request_history")
        
        # Verify request/reply matching
        assert len(history["requests"]) == len(test_data)
        assert len(history["replies"]) == len(test_data)
        
        # Verify request order maintained
        for i, data in enumerate(test_data):
            assert history["requests"][i]["data"] == data
            assert history["replies"][i]["processed_data"] == f"Processed: {data}"
        
        # Verify timing - replies should come after requests
        for req, rep in zip(history["requests"], history["replies"]):
            assert rep["time"] >= req["time"], "Reply came before request"
            
    finally:
        ether.shutdown()

if __name__ == '__main__':
    pytest.main([__file__]) 