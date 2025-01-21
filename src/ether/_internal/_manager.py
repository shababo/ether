from typing import Union
from multiprocessing import Process

from ..utils import get_ether_logger
from ._config import EtherConfig
from ether.liaison import EtherInstanceLiaison


class _EtherInstanceManager:
    """Manages Ether instance processes"""
    def __init__(
            self, 
            config: Union[EtherConfig, str, dict] = None, 
            autolaunch: bool = True
        ):
        self._instance_processes: dict[str, Process] = {}
        self._logger = get_ether_logger("EtherInstanceManager")
        self._logger.debug("Initializing EtherInstanceManager")
        self._config = config or EtherConfig()
        self._liaison = EtherInstanceLiaison(network_config=self._config.network)
        
        self._autolaunch = autolaunch
        
        if self._autolaunch:
            self._logger.debug("Autolaunch enabled, starting instances")
            self.launch_instances(self._config)

    def stop_instance(self, instance_id: str, force: bool = False):
        self._logger.debug(f"Stopping instance {instance_id} (force={force})")
        
        if instance_id in self._instance_processes:
            process = self._instance_processes[instance_id]
            
            # Get instance info before stopping
            instances = self._liaison.get_active_instances()
            
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
                self._liaison.deregister_instance(redis_id)
            
            del self._instance_processes[instance_id]
            self._logger.debug(f"Instance {instance_id} stopped and removed")

    def launch_instances(
            self, 
            config: Union[EtherConfig, str, dict] = None
        ) -> dict[str, Process]:
        self._logger.debug("Launching instances from config")
        
        processes = {}
        if not config:
            self._logger.debug("No config provided, skipping instance launch")
            return processes
            
        # Process config
        if isinstance(config, str):
            self._logger.debug(f"Loading config from YAML: {config}")
            config = EtherConfig.from_yaml(config)
        elif isinstance(config, dict):
            self._logger.debug("Converting dict to EtherConfig")
            config = EtherConfig.model_validate(config)
            
        # Check current instances
        liaison = EtherInstanceLiaison(network_config=self._config.network)
        current_instances = liaison.get_active_instances()
        self._logger.debug(f"Current active instances: {list(current_instances.keys())}")
        
        # Launch new instances
        for instance_name, instance_config in config.instances.items():
            if not instance_config.autorun:
                self._logger.debug(f"Skipping {instance_name} (autorun=False)")
                continue
                
            # Check if already running
            already_running = any(
                info.get('process_name') == instance_name 
                for info in current_instances.values()
            )
            if already_running:
                self._logger.debug(f"Instance {instance_name} already running, skipping")
                continue
                
            self._logger.debug(f"Launching instance: {instance_name}")
            process = Process(
                target=instance_config.run,
                args=(instance_name,),
                name=instance_name
            )
            process.daemon = True
            process.start()
            processes[instance_name] = process
            self._logger.debug(f"Instance {instance_name} launched with PID {process.pid}")

        self._instance_processes.update(processes)
        return processes

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

    def cleanup(self):
        """Clean up all instances and resources"""
        self.stop_all_instances() 