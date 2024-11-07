from typing import Dict, Union
import time
from multiprocessing import Process
import logging

from .._utils import _get_logger
from .._config import EtherConfig
from ._liaison import EtherInstanceLiaison


class _EtherInstanceManager:
    """Manages Ether instance processes"""
    def __init__(
            self, 
            config: Union[EtherConfig, str, dict] = None, 
            autolaunch: bool = True
        ):
        self._instance_processes: Dict[str, Process] = {}
        self._logger = _get_logger("EtherInstanceManager", log_level=logging.DEBUG)
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

    def launch_instances(
            self, 
            config: Union[EtherConfig, str, dict] = None
        ) -> Dict[str, Process]:
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

            # liaison.register_instance(instance_name, process)
        

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