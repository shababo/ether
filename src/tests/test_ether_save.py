import pytest
import time
from typing import List
from ether import ether
from ether._internal._config import EtherConfig, EtherInstanceConfig
from ether._internal._registry import _ether_save, _ether_get
from ether.utils import _get_logger

@pytest.mark.skip(reason="Not a test class")
class DataService:
    def __init__(self):
        self.data = {}
    
    @_ether_save(heartbeat=True)
    def save_item(self, id: int, name: str, tags: List[str]) -> dict:
        """Save an item to the data store"""
        if id in self.data:
            raise ValueError(f"Item {id} already exists")
            
        self.data[id] = {
            "name": name,
            "tags": tags
        }
        return self.data[id]  # Return saved item

    @_ether_get
    def get_item(self, id: int) -> dict:
        """Get a single item by ID"""
        if id not in self.data:
            raise KeyError(f"Item {id} not found")
        return self.data[id]

def test_ether_save():
    logger = _get_logger("TestEtherSave")
    
    config = EtherConfig(
        instances={
            "data_service": EtherInstanceConfig(
                class_path="tests.test_ether_save.DataService",
                kwargs={"name": "data_service"}
            )
        }
    )
    
    ether.tap(config=config)
    
    try:
        
        # Test saving new item
        reply = ether.request(
            "DataService", 
            "save_item", 
            params={
                "id": 1,
                "name": "Test Item",
                "tags": ["test", "new"]
            },
            request_type="save"
        )
        logger.info(f"Save reply: {reply}")
        assert reply["status"] == "success"
        result = reply["result"]
        assert result["name"] == "Test Item"
        
        # Verify item was saved
        item = ether.request("DataService", "get_item", params={"id": 1})
        logger.info(f"Get item: {item}")
        assert item["name"] == "Test Item"
        assert item["tags"] == ["test", "new"]
        
        # Test saving duplicate item

        reply = ether.request(
            "DataService", 
            "save_item", 
            params={
                "id": 1,
                "name": "Duplicate",
                "tags": []
            },
            request_type="save"
        )
        logger.info(f"Duplicate save reply: {reply}")
        assert reply["status"] == "error"
        assert "already exists" in str(reply["error"])
        
    finally:
        ether.shutdown() 