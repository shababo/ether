import redis
from typing import Optional, Dict, Any, Union
import json
import time
from multiprocessing import Process
import os
import logging

from ._utils import _get_logger
from ._config import EtherConfig

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
        self._logger = logging.getLogger("InstanceLiaison")
    
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
    
    def deregister_all(self):
        """Remove all tracked instances"""
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
                self._logger.info(f"Culling dead instance {instance_id} (PID {pid})")
                self.deregister_instance(instance_id)
                removed += 1
                
        return removed

class _EtherInstanceManager:
    """Manages Ether instance processes"""
    def __init__(
            self, 
            config: Union[EtherConfig, str, dict] = None, 
            autolaunch: bool = True
        ):
        self._instance_processes: Dict[str, Process] = {}
        self._logger = _get_logger("EtherInstanceManager", log_level=logging.INFO)
        self._liaison = EtherInstanceLiaison()
        self._config = config
        self._autolaunch = autolaunch
        if self._autolaunch:
            self.launch_instances(self._config)

    def stop_instance(self, instance_id: str, force: bool = False):
        """Stop a specific instance"""
        if instance_id in self._instance_processes:
            process = self._instance_processes[instance_id]
            self._logger.debug(f"Stopping instance {instance_id}")
            
            # Get instance info before stopping
            instances = self._liaison.get_active_instances()
            
            # Find the Redis ID for this process
            redis_id = None
            for rid, info in instances.items():
                if info.get('process_name') == instance_id:
                    redis_id = rid
                    break
            
            # Stop the process
            process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                self._logger.warning(f"Instance {instance_id} didn't stop gracefully, killing")
                process.kill()
                process.join(timeout=1)
                
            # Deregister from Redis if we found the ID
            if redis_id:
                self._logger.debug(f"Deregistering instance {instance_id} (Redis ID: {redis_id})")
                self._liaison.deregister_instance(redis_id)
            else:
                self._logger.warning(f"Could not find Redis ID for instance {instance_id}")
                
            del self._instance_processes[instance_id]

    def launch_instances(self, config: Union[EtherConfig, str, dict] = None) -> Dict[str, Process]:
        """Launch configured instances
        
        Args:
            only_autorun: If True, only launch instances with autorun=True
        """

        processes = {}

        if not config or not isinstance(config, (str, dict, EtherConfig)):
            return processes
        elif isinstance(config, str):
            config = EtherConfig.from_yaml(config)
        elif isinstance(config, dict):
            config = EtherConfig.model_validate(config)

        liaison = EtherInstanceLiaison()
        current_instances = liaison.get_active_instances()
        
        for instance_name, instance_config in config.instances.items():
            if not instance_config.autorun:
                continue
            
            # Check if instance is already running by process name
            already_running = any(
                info.get('process_name') == instance_name 
                for info in current_instances.values()
            )
            if already_running:
                continue
            
            # Create and start process
            process = Process(
                target=instance_config.run,
                args=(instance_name,),
                name=instance_name
            )
            process.daemon = True
            process.start()
            processes[instance_name] = process

            liaison.add_instance(instance_name, process)
        

        self._instance_processes.update(processes)
        return processes

    def stop_all_instances(self, force: bool = False):
        """Stop all running instances"""
        for instance_id in list(self._instance_processes.keys()):
            self.stop_instance(instance_id, force)

    def add_instance(self, instance_id: str, process: Process):
        """Add a running instance to be managed"""
        self._instance_processes[instance_id] = process

    def get_instance_processes(self) -> Dict[str, Process]:
        """Get all managed instance processes"""
        return self._instance_processes.copy()

    def cleanup(self):
        """Clean up all instances and resources"""
        self.stop_all_instances() 