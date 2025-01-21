import redis
from typing import Dict, Any
import json
import time
import os

from ether.utils import get_ether_logger, get_ip_address
from ether._internal._session import EtherSession
from ether._internal._config import EtherNetworkConfig


class EtherInstanceLiaison:
    """An interface for (de)registrations and process tracking for instances"""
    
    def __init__(self, network_config: EtherNetworkConfig = None):
        """Initialize the instance (only called once)"""
        self._logger = get_ether_logger("EtherInstanceLiaison")
        
        # Get network config from session
        session_data = EtherSession.get_current_session()
        if session_data and "network" in session_data:
            network_config = EtherNetworkConfig.model_validate(session_data["network"])
            self._logger.debug(f"Using network config from session: {network_config}")
        else:
            self._logger.debug("No session found, using default network config")
        network_config = network_config or EtherNetworkConfig()

        if session_data and "public_ip" in session_data:
            host = session_data["public_ip"]
            self._logger.debug(f"Using public IP {host} from session")
            my_ip = get_ip_address()
            self._logger.debug(f"My IP is {my_ip}")
            if my_ip == host:
                host = "localhost"
        else:
            host = network_config.host
            
        # Connect to Redis using network config
        self._logger.info(f"redis://{host}:{network_config.redis_port}")
        redis_url = f"redis://{host}:{network_config.redis_port}"
        self._logger.debug(f"Connecting to Redis at {redis_url}")
        pool = redis.ConnectionPool(
            host = host,
            port = network_config.redis_port,
            decode_responses = True,
            health_check_interval=30
        )
        self.redis = redis.Redis(connection_pool=pool)
        # self.redis = redis.Redis.from_url(redis_url, decode_responses=True, health_check_interval=10,
        #     socket_timeout=10, socket_keepalive=True,
        #     socket_connect_timeout=10, retry_on_timeout=True)
        
        self.instance_key_prefix = "ether:instance:"
        self._ttl = 60  # seconds until instance considered dead
    
    
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
        self._logger.debug(f"Registering instance {instance_id} with metadata: {metadata}")
        key = f"{self.instance_key_prefix}{instance_id}"
        metadata['registered_at'] = time.time()
        metadata['pid'] = os.getpid()  # Add process ID
        self.redis.set(key, json.dumps(metadata), ex=self._ttl)
    
    def refresh_instance(self, instance_id: str) -> None:
        """Refresh instance TTL"""
        self._logger.debug(f"Refreshing instance {instance_id} TTL")
        key = f"{self.instance_key_prefix}{instance_id}"
        if data := self.redis.get(key):
            self.redis.expire(key, self._ttl)
    
    def deregister_instance(self, instance_id: str) -> None:
        """Remove an instance registration"""
        self._logger.debug(f"Deregistering instance {instance_id}")
        key = f"{self.instance_key_prefix}{instance_id}"
        self.redis.delete(key)
    
    def get_active_instances(self) -> Dict[str, Dict[str, Any]]:
        """Get all currently active instances"""
        self._logger.debug("Getting active instances")
        instances = {}
        for key in self.redis.keys(f"{self.instance_key_prefix}*"):
            if data := self.redis.get(key):
                instance_id = key.replace(self.instance_key_prefix, "")
                instances[instance_id] = json.loads(data)
        self._logger.debug(f"Active instances: {instances}")
        return instances
    
    def get_instance_data(self, instance_id: str) -> Dict[str, Any]:
        """Get data for a specific instance"""
        key = f"{self.instance_key_prefix}{instance_id}"
        data = self.redis.get(key)
        if data:
            self._logger.debug(f"Instance data for {instance_id}: {data}")
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

