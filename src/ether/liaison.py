import redis
from typing import Dict, Any
import json
import time
import os

from ether.utils import _get_logger


class EtherInstanceLiaison:
    """An interface for (de)registrations and process tracking for instances"""
    _instance = None
    
    def __new__(cls, redis_url: str = "redis://localhost:6379"):
        if cls._instance is None:
            cls._instance = super(EtherInstanceLiaison, cls).__new__(cls)
            cls._instance._init(redis_url)
        return cls._instance
    
    def _init(self, redis_url: str):
        """Initialize the instance (only called once)"""
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.instance_key_prefix = "ether:instance:"
        self._ttl = 60  # seconds until instance considered dead
        self._logger = _get_logger("EtherInstanceLiaison")
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        # __new__ handles initialization
        pass
    
    @property
    def ttl(self) -> int:
        return self._ttl
    
    @ttl.setter
    def ttl(self, value: int):
        """Set TTL and update all existing instances"""
        self._ttl = value
        # Update TTL for all existing instances
        for key in self.redis.keys(f"{self.instance_key_prefix}*"):
            self.redis.expire(key, value)
    
    def register_instance(self, instance_id: str, metadata: Dict[str, Any]) -> None:
        """Register a new Ether instance"""
        key = f"{self.instance_key_prefix}{instance_id}"
        metadata['registered_at'] = time.time()
        metadata['pid'] = os.getpid()  # Add process ID
        self.redis.set(key, json.dumps(metadata), ex=self._ttl)
    
    def refresh_instance(self, instance_id: str) -> None:
        """Refresh instance TTL"""
        key = f"{self.instance_key_prefix}{instance_id}"
        if data := self.redis.get(key):
            self.redis.expire(key, self._ttl)
    
    def deregister_instance(self, instance_id: str) -> None:
        """Remove an instance registration"""
        key = f"{self.instance_key_prefix}{instance_id}"
        self.redis.delete(key)
    
    def get_active_instances(self) -> Dict[str, Dict[str, Any]]:
        """Get all currently active instances"""
        instances = {}
        for key in self.redis.keys(f"{self.instance_key_prefix}*"):
            if data := self.redis.get(key):
                instance_id = key.replace(self.instance_key_prefix, "")
                instances[instance_id] = json.loads(data)
        return instances
    
    def get_instance_data(self, instance_id: str) -> Dict[str, Any]:
        """Get data for a specific instance"""
        key = f"{self.instance_key_prefix}{instance_id}"
        data = self.redis.get(key)
        if data:
            return json.loads(data)
        return {}
    
    def update_instance_data(self, instance_id: str, data: Dict[str, Any]) -> None:
        """Update data for a specific instance"""
        key = f"{self.instance_key_prefix}{instance_id}"
        existing_data = self.get_instance_data(instance_id)
        existing_data.update(data)
        self.redis.set(key, json.dumps(existing_data), ex=self._ttl)
    
    def deregister_all(self):
        """Remove all tracked instances"""
        self._logger.debug("Deleting all instances")
        pattern = f"{self.instance_key_prefix}*"
        keys = self.redis.keys(pattern)
        if keys:
            self.redis.delete(*keys)
    
    def cull_dead_processes(self) -> int:
        """Remove instances whose processes no longer exist
        
        Returns:
            Number of instances removed
        """
        removed = 0
        instances = self.get_active_instances()
        
        for instance_id, info in instances.items():
            pid = info.get('pid')
            if pid is None:
                continue
                
            try:
                # Check if process exists
                os.kill(pid, 0)
            except OSError:
                # Process doesn't exist
                self._logger.debug(f"Culling dead instance {instance_id} (PID {pid})")
                self.deregister_instance(instance_id)
                removed += 1
                
        return removed
    
    def store_registry_config(self, config: dict):
        """Store registry configuration in Redis"""
        self._logger.debug(f"Current registry config: {self.get_registry_config()}")
        self._logger.debug(f"Storing registry config: {config}")
        self.redis.set("ether:registry_config", json.dumps(config))
        self._logger.debug(f"Updated registry config: {self.get_registry_config()}")
    
    def get_registry_config(self) -> dict:
        """Get registry configuration from Redis"""
        config = self.redis.get("ether:registry_config")
        return json.loads(config) if config else {}

