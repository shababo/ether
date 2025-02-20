from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
import yaml
import importlib

# from ._registry import EtherRegistry
from ether.utils import get_ether_logger


class EtherDecoratorConfig(BaseModel):
    """Configuration for a single Ether decorator"""
    topic: Optional[str] = None
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)


class EtherMethodConfig(BaseModel):
    """Configuration for a method's decorators"""
    ether_pub: Optional[EtherDecoratorConfig] = None
    ether_sub: Optional[EtherDecoratorConfig] = None


class EtherClassConfig(BaseModel):
    """Configuration for a class's methods"""
    methods: Dict[str, EtherMethodConfig] = Field(default_factory=dict)
    
class EtherNetworkConfig(BaseModel):
    """Network configuration for Ether"""
    host: str = "localhost"
    pubsub_frontend_port: int = 13311
    pubsub_backend_port: int = 13312
    reqrep_frontend_port: int = 13313
    reqrep_backend_port: int = 13314
    redis_host: str = "0.0.0.0"  # Add separate Redis host config
    redis_port: int = 13315
    session_discovery_port: int = 13309
    session_query_port: int = 13310

class EtherSecurityLevel(Enum):
    BASIC = "basic"      # Just group isolation
    STANDARD = "standard"  # + role-based access
    HIGH = "high"        # + session key rotation, logging


class EtherSecurityConfig(BaseModel):
    """Security configuration for Ether"""
    security_level: EtherSecurityLevel = EtherSecurityLevel.STANDARD
    group_key: Optional[str] = None
    user_key: Optional[str] = None



class EtherInstanceConfig(BaseModel):
    """Configuration for a single Ether instance"""
    class_path: str  # format: "module.submodule.ClassName"
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    autorun: bool = True  # Whether to automatically launch this instance
    network_config: Optional[EtherNetworkConfig] = None
    
    def get_class(self):
        """Get the class from its path"""
        module_path, class_name = self.class_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        
        # Check Redis for registry configuration
        from ether.liaison import EtherInstanceLiaison
        liaison = EtherInstanceLiaison(network_config=self.network_config)
        registry_config = liaison.get_registry_config()
        
        # If this class has registry configuration, process it
        if self.class_path in registry_config:
            from ._registry import EtherRegistry
            from ._config import EtherClassConfig
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
            cls = self.get_class()
            kwargs = self.kwargs.copy()
            # Override name if not explicitly set in kwargs
            if 'ether_name' not in kwargs:
                kwargs['ether_name'] = instance_name
            kwargs['ether_network_config'] = self.network_config
            kwargs['ether_run'] = True
            instance = cls(*self.args, **kwargs)
            instance.run()
        except Exception as e:
            logger.error(f"Error running instance {instance_name}: {e}")
            raise e




class EtherConfig(BaseModel):
    """Complete Ether configuration"""
    registry: Dict[str, EtherClassConfig] = Field(default_factory=dict)
    instances: Dict[str, EtherInstanceConfig] = Field(default_factory=dict)
    network: EtherNetworkConfig = Field(default_factory=EtherNetworkConfig)  # Add network config

    @classmethod
    def from_yaml(cls, path: str) -> "EtherConfig":
        """Load configuration from YAML file"""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
    
    