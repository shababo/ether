from typing import Dict, Any, Optional, Union
from pydantic import BaseModel
import yaml
import importlib
from multiprocessing import Process
import uuid

from ._ether import EtherRegistry
from ._instance_tracker import EtherInstanceTracker

class EtherInstanceConfig(BaseModel):
    """Configuration for a single Ether instance"""
    class_path: str  # format: "module.submodule.ClassName"
    args: list[Any] = []
    kwargs: Dict[str, Any] = {}
    autorun: bool = True  # Whether to automatically launch this instance
    
    def get_class(self):
        """Get and ensure the class is processed"""
        module_path, class_name = self.class_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        
        # Ensure class is processed if it has Ether methods
        if class_name in EtherRegistry._pending_classes:
            EtherRegistry.process_pending_classes()
            
        return cls
    
    def run(self, instance_name: str):
        """Run a single instance with the configured args and kwargs"""
        cls = self.get_class()
        kwargs = self.kwargs.copy()
        # Override name if not explicitly set in kwargs
        if 'name' not in kwargs:
            kwargs['name'] = instance_name
        instance = cls(*self.args, **kwargs)
        instance.run()

class EtherConfig(BaseModel):
    """Complete Ether configuration"""
    instances: Dict[str, EtherInstanceConfig]  # name -> config
    
    @classmethod
    def from_yaml(cls, path: str) -> "EtherConfig":
        """Load configuration from YAML file"""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
    
    def launch_instances(self, only_autorun: bool = True) -> Dict[str, Process]:
        """Launch configured instances
        
        Args:
            only_autorun: If True, only launch instances with autorun=True
        """
        processes = {}
        tracker = EtherInstanceTracker()
        current_instances = tracker.get_active_instances()
        
        for instance_name, instance_config in self.instances.items():
            if only_autorun and not instance_config.autorun:
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
        
        return processes