from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel
import yaml
import importlib
from multiprocessing import Process
import uuid

from ._ether import EtherRegistry
class EtherInstanceConfig(BaseModel):
    """Configuration for a single Ether instance"""
    class_path: str  # format: "module.submodule.ClassName"
    args: List[Any] = []
    kwargs: Dict[str, Any] = {}
    
    def get_class(self):
        """Get and ensure the class is processed"""
        module_path, class_name = self.class_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        
        # Ensure class is processed if it has Ether methods
        if class_name in EtherRegistry._pending_classes:
            EtherRegistry.process_pending_classes()
            
        return cls
    
    def run(self):
        """Run a single instance with the configured args and kwargs"""
        cls = self.get_class()
        instance = cls(*self.args, **self.kwargs)
        instance.run()

class EtherConfig(BaseModel):
    """Complete Ether configuration"""
    instances: List[EtherInstanceConfig]
    
    @classmethod
    def from_yaml(cls, path: str) -> "EtherConfig":
        """Load configuration from YAML file"""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
    
    def launch_instances(self) -> Dict[str, Process]:
        """Launch all configured instances in separate processes"""
        processes = {}
        
        for instance_config in self.instances:
            # Generate instance ID
            class_name = instance_config.class_path.split('.')[-1]
            instance_id = f"{class_name}_{uuid.uuid4().hex[:8]}"
            
            # Create and start process
            process = Process(
                target=instance_config.run,
                name=instance_id
            )
            process.daemon = True
            process.start()
            processes[instance_id] = process
        
        return processes