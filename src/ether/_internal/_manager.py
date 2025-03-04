from typing import Any, Dict, Union
from multiprocessing import Process
import time
import redis
import os
import json

from ..utils import get_ether_logger
from ._config import _EtherConfig


class _EtherInstanceManager:
    """Manages Ether instance processes"""
    def __init__(
            self, 
            config: Union[_EtherConfig, str, dict] = None, 
            autolaunch: bool = False
        ):
        self._instance_processes: dict[str, Process] = {}
        self._logger = get_ether_logger("EtherInstanceManager")
        self._logger.debug("Initializing EtherInstanceManager")
        self._config = config or _EtherConfig()

        self._connect_to_redis()
        
        self._autolaunch = autolaunch
        
        if self._autolaunch:
            self._logger.debug("Autolaunch enabled, starting instances")
            self.launch_instances()

    def _connect_to_redis(self):
        # Connect to Redis using network config
        redis_url = f"redis://{self._config.session.host}:{self._config.session.redis_port}"
        self._logger.debug(f"Connecting to Redis at {redis_url}")
        pool = redis.ConnectionPool(
            host = self._config.session.host,
            port = self._config.session.redis_port,
            decode_responses = True,
            health_check_interval=30
        )
        self.redis = redis.Redis(connection_pool=pool)
        # self.redis = redis.Redis.from_url(redis_url, decode_responses=True, health_check_interval=10,
        #     socket_timeout=10, socket_keepalive=True,
        #     socket_connect_timeout=10, retry_on_timeout=True)
        
        self.instance_key_prefix = "ether:instance:"
        self._ttl = 60  # seconds until instance considered dead
        
    def register_instance(self, instance_id: str, metadata: Dict[str, Any]) -> None:
        """Register a new Ether instance"""
        self._logger.debug(f"Registering instance {instance_id} with metadata: {metadata}")
        key = f"{self.instance_key_prefix}{instance_id}"
        metadata['registered_at'] = time.time()
        metadata['pid'] = os.getpid()  # Add process ID
        self.redis.set(key, json.dumps(metadata), ex=self._ttl)

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
        self._logger.debug(f"Updating instance data for {instance_id}: {data}")
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

    def store_registry_config(self, config: dict):
        """Store registry configuration in Redis"""
        self._logger.debug(f"Storing registry config: {config}")
        self.redis.set("ether:registry_config", json.dumps(config))
    
    def get_registry_config(self) -> dict:
        """Get registry configuration from Redis"""
        config = self.redis.get("ether:registry_config")
        return json.loads(config) if config else {}

    @property
    def ttl(self) -> int:
        return self._ttl
    
    @ttl.setter
    def ttl(self, value: int):
        """Set TTL and update all existing instances"""
        self._ttl = value
        # Update TTL for all existing instances

    def stop_instance(self, instance_id: str, force: bool = False):
        self._logger.debug(f"Stopping instance {instance_id} (force={force})")
        
        if instance_id in self._instance_processes:
            process = self._instance_processes[instance_id]
            
            # Get instance info before stopping
            instances = self.get_active_instances()
            
            # Find Redis ID
            redis_id = None
            for rid, info in instances.items():
                if info.get('process_name') == instance_id:
                    redis_id = rid
                    break
            
            self._logger.debug(f"Found Redis ID for {instance_id}: {redis_id}")
            
            # Stop process
            process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                self._logger.warning(f"Instance {instance_id} didn't stop gracefully, killing")
                process.kill()
                process.join(timeout=1)
            
            if redis_id:
                self._logger.debug(f"Deregistering instance {instance_id} (Redis ID: {redis_id})")
                self.deregister_instance(redis_id)
            
            del self._instance_processes[instance_id]
            self._logger.debug(f"Instance {instance_id} stopped and removed")

    def launch_instances(
            self, 
            config: Union[_EtherConfig, str, dict] = None
        ) -> dict[str, Process]:
        self._logger.debug("Launching instances from config")
        
        config = config or self._config
        processes = {}
        if not config:
            self._logger.debug("No config provided, skipping instance launch")
            return processes
            
        # Process config
        if isinstance(config, str):
            self._logger.debug(f"Loading config from YAML: {config}")
            config = _EtherConfig.from_yaml(config)
        elif isinstance(config, dict):
            self._logger.debug("Converting dict to EtherConfig")
            config = _EtherConfig.model_validate(config)
            
        
        # Launch new instances
        for instance_name, instance_config in config.instances.items():
            if not instance_config.autorun:
                self._logger.debug(f"Skipping {instance_name} (autorun=False)")
                continue
        
                
            self._logger.debug(f"Launching instance: {instance_name}")
            process = Process(
                target=instance_config.run,
                args=(instance_name,),
                name=instance_name
            )
            process.daemon = False
            process.start()
            time.sleep(0.1)
            # confirm process is running
            if not process.is_alive():
                self._logger.error(f"Instance {instance_name} failed to start")
                raise RuntimeError(f"Instance {instance_name} failed to start")
            processes[instance_name] = process
            self._logger.debug(f"Instance {instance_name} launched with PID {process.pid}")

        self._instance_processes.update(processes)

    def stop_all_instances(self, force: bool = False):
        self._logger.debug(f"Stopping all instances (force={force})")
        for instance_id in list(self._instance_processes.keys()):
            self.stop_instance(instance_id, force)

    def add_instance(self, instance_id: str, process: Process):
        """Add a running instance to be managed"""
        self._instance_processes[instance_id] = process

    def get_instance_processes(self) -> dict[str, Process]:
        """Get all managed instance processes"""
        return self._instance_processes.copy()
    
    def refresh_instance(self, instance_id: str) -> None:
        """Refresh instance TTL"""
        self._logger.debug(f"Refreshing instance {instance_id} TTL")
        key = f"{self.instance_key_prefix}{instance_id}"
        if data := self.redis.get(key):
            self.redis.expire(key, self._ttl)

    def cleanup(self):
        """Clean up all instances and resources"""
        self.stop_all_instances() 