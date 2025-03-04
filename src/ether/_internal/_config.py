from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
import importlib

# from ._registry import EtherRegistry
from ether.config import EtherSessionConfig, EtherConfig
from ether.utils import get_ether_logger


class _EtherInstanceConfig(BaseModel):
    """Configuration for a single Ether instance"""
    class_path: str  # format: "module.submodule.ClassName"
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    autorun: bool = True  # Whether to automatically launch this instance
    session_config: Optional[EtherSessionConfig] = Field(default_factory=EtherSessionConfig)
    
    def get_class(self):
        """Get the class from its path"""
        module_path, class_name = self.class_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        
        # Check Redis for registry configuration
        # from ether.liaison import EtherInstanceLiaison
        # liaison = EtherInstanceLiaison(session_config=self.session_config)
        from ._ether import _ether
        if _ether._instance_manager:
            registry_config = _ether._instance_manager.get_registry_config()
        else:
            registry_config = {}

        print(f"Registry config: {registry_config}")
        
        # If this class has registry configuration, process it
        print(f"Class path: {self.class_path}")
        if self.class_path in registry_config:
            from ._registry import EtherRegistry
            from ..config import EtherClassConfig
            class_config = EtherClassConfig.model_validate(registry_config[self.class_path])
            EtherRegistry().process_registry_config({self.class_path: class_config})
        
        # Always add Ether functionality
        from ._registry import add_ether_functionality
        cls = add_ether_functionality(cls)
        
        return cls
    
    def run(self, instance_name: str):
        """Run a single instance with the configured args and kwargs"""
        logger = get_ether_logger("EtherInstanceConfig")
        logger.info(f"Running instance {instance_name} with class {self.class_path}\n\targs: {self.args}\n\tkwargs: {self.kwargs}")
        try:
            from ether import ether
            ether.tap(config=_EtherConfig(session=self.session_config), allow_host=False, ether_run = True)

            cls = self.get_class()
            kwargs = self.kwargs.copy()
            # Override name if not explicitly set in kwargs
            if 'ether_name' not in kwargs:
                kwargs['ether_name'] = instance_name
            kwargs['ether_session_config'] = self.session_config
            # kwargs['ether_run'] = True
            
            instance = cls(*self.args, **kwargs)
            instance.run()
        except Exception as e:
            logger.error(f"Error running instance {instance_name}: {e}")
            raise e


class _EtherConfig(EtherConfig):
    instances: Dict[str, _EtherInstanceConfig] = Field(default_factory=dict)

    
    