from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
import yaml
import importlib

class EtherDecoratorConfig(BaseModel):
    """Configuration for a single Ether decorator"""
    topic: Optional[str] = None
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)


class EtherMethodConfig(BaseModel):
    """Configuration for a method's decorators"""
    ether_pub: Optional[EtherDecoratorConfig] = None
    ether_sub: Optional[EtherDecoratorConfig] = None


class EtherSecurityLevel(Enum):
    BASIC = "basic"      # Just group isolation
    STANDARD = "standard"  # + role-based access
    HIGH = "high"        # + session key rotation, logging


class EtherSecurityConfig(BaseModel):
    """Security configuration for Ether"""
    security_level: EtherSecurityLevel = EtherSecurityLevel.STANDARD
    group_key: Optional[str] = None
    user_key: Optional[str] = None


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
    
class EtherInstanceConfig(BaseModel):
    """Configuration for a single Ether instance"""
    class_path: str  # format: "module.submodule.ClassName"
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    autorun: bool = True  # Whether to automatically launch this instance
    network_config: Optional[EtherNetworkConfig] = None


class EtherClassConfig(BaseModel):
    """Configuration for a class's methods"""
    methods: Dict[str, EtherMethodConfig] = Field(default_factory=dict)


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





