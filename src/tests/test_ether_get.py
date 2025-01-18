import pytest
import time
from ether import ether
from ether._internal._registry import _ether_get
from ether.utils import _get_logger
from typing import Optional, List
from ether._internal._config import EtherConfig, EtherInstanceConfig

class DataService:
    def __init__(self):
        self.data = {
            1: {"name": "Item 1", "tags": ["a", "b"]},
            2: {"name": "Item 2", "tags": ["b", "c"]},
            3: {"name": "Item 3", "tags": ["a", "c"]}
        }
    
    @_ether_get
    def get_item(self, id: int) -> dict:
        """Get a single item by ID"""
        if id not in self.data:
            raise KeyError(f"Item {id} not found")
        return self.data[id]
    
    @_ether_get
    def get_items_by_tag(self, tag: str, limit: Optional[int] = None) -> List[dict]:
        """Get items that have a specific tag"""
        items = [
            {"id": id, **item}
            for id, item in self.data.items()
            if tag in item["tags"]
        ]
        if limit:
            items = items[:limit]
        return items
    
    @_ether_get
    def get_stats(self) -> dict:
        """Get stats about the data (no parameters)"""
        return {
            "total_items": len(self.data),
            "total_tags": len(set(
                tag 
                for item in self.data.values() 
                for tag in item["tags"]
            ))
        }

def test_ether_get_with_params():
    logger = _get_logger("TestEtherGet")
    logger.debug("Starting Ether get test")
    
    # Create configuration
    config = EtherConfig(
        instances={
            "data_service": EtherInstanceConfig(
                class_path="tests.test_ether_get.DataService",
                kwargs={"name": "data_service"}
            )
        }
    )
    
    # Initialize Ether with config
    ether.tap(config=config)
    
    try:
        
        # Test get_item with valid ID
        logger.debug("Testing get_item with valid ID")
        result = ether.request("DataService", "get_item", params={"id": 1})
        logger.debug(f"get_item result: {result}")
        assert result == {"name": "Item 1", "tags": ["a", "b"]}
        
        # Test get_item with invalid ID
        logger.debug("Testing get_item with invalid ID")
        with pytest.raises(Exception) as exc:
            ether.request("DataService", "get_item", params={"id": 999})
        logger.debug(f"get_item error: {str(exc.value)}")
        assert "Item 999 not found" in str(exc.value)
        
        # Test get_item with invalid parameter type
        logger.debug("Testing get_item with invalid parameter type")
        with pytest.raises(Exception) as exc:
            ether.request("DataService", "get_item", params={"id": "not an int"})
        logger.debug(f"get_item validation error: {str(exc.value)}")
        assert "Invalid parameters" in str(exc.value)
        
        # Test get_items_by_tag with required parameter
        logger.debug("Testing get_items_by_tag with required parameter")
        result = ether.request("DataService", "get_items_by_tag", params={"tag": "b"})
        logger.debug(f"get_items_by_tag result: {result}")
        assert len(result) == 2
        assert all("b" in item["tags"] for item in result)
        
        # Test get_items_by_tag with optional parameter
        logger.debug("Testing get_items_by_tag with optional parameter")
        result = ether.request("DataService", "get_items_by_tag", 
                              params={"tag": "b", "limit": 1})
        logger.debug(f"get_items_by_tag with limit result: {result}")
        assert len(result) == 1
        assert "b" in result[0]["tags"]
        
        # Test get_stats with no parameters
        logger.debug("Testing get_stats with no parameters")
        result = ether.request("DataService", "get_stats")
        logger.debug(f"get_stats result: {result}")
        assert result == {"total_items": 3, "total_tags": 3}
        
        # Test get_stats with unexpected parameter (should be ignored)
        logger.debug("Testing get_stats with unexpected parameter")
        result = ether.request("DataService", "get_stats", params={"unexpected": "param"})
        logger.debug(f"get_stats with unexpected param result: {result}")
        assert result == {"total_items": 3, "total_tags": 3}
        
    finally:
        ether.shutdown()

if __name__ == '__main__':
    pytest.main([__file__]) 