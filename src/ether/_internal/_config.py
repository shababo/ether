from typing import Any
from pydantic import BaseModel
import yaml
import importlib

from ._registry import EtherRegistry


class EtherInstanceConfig(BaseModel):
    """Configuration for a single Ether instance"""
    class_path: str  # format: "module.submodule.ClassName"
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
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
    instances: dict[str, EtherInstanceConfig]  # name -> config
    
    @classmethod
    def from_yaml(cls, path: str) -> "EtherConfig":
        """Load configuration from YAML file"""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
    
    