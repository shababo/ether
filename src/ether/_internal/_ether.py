# Modify existing _proxy.py to include daemon functionality

import time
import zmq
from typing import Union, Dict, Optional
from pydantic import BaseModel
import json
import uuid

from ..utils import get_ether_logger, get_ip_address
from ._session import EtherSession, session_discovery_launcher
# from ..liaison import EtherInstanceLiaison 
from ._manager import _EtherInstanceManager
from ..config import EtherConfig
from ._config import _EtherConfig
from ._registry import EtherRegistry
from ._reqrep import (
    MDPW_WORKER,
    W_DISCONNECT,
    MDPC_CLIENT,
    REPLY_MSG_CLIENT_INDEX,
    REPLY_MSG_SERVICE_INDEX,
    REPLY_MSG_DATA_INDEX,
)


class _Ether:
    """Singleton to manage Ether services behind the scenes."""
    _instance = None
    _ether_id = None
    _logger = None
    _config = None
    _session = None
    _instance_manager = None
    _ether_session_process = None
    _redis_client = None
    _session_metadata = None
    _started = False
    _is_main_session = False
    _pub_socket = None
    _request_socket = None
    _zmq_context = None
    _reqrep_broker_process = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_Ether, cls).__new__(cls)
            cls._instance._ether_id = str(uuid.uuid4())
            cls._instance._logger = get_ether_logger("Ether")
            cls._instance._logger.debug("Initializing Ether instance")
            
        return cls._instance
        
    def _setup_publisher(self):
        """Set up the ZMQ publisher socket"""
        self._logger.debug("Setting up publisher socket")
        if self._pub_socket is None:
            self._zmq_context = zmq.Context()
            self._pub_socket = self._zmq_context.socket(zmq.PUB)
            self._pub_socket.connect(f"tcp://{self._config.session.host}:{self._config.session.pubsub_backend_port}")
            self._logger.debug("Publisher socket connected")
    
    def publish(self, data: Union[Dict, BaseModel], topic: str) -> None:
        """Publish data to a topic
        
        Args:
            data: Data to publish (dict or Pydantic model)
            topic: Topic to publish to
        """
        self._logger.debug(f"Publishing request received - Topic: {topic}")
        
        if not self._started:
            self._logger.debug(f"Cannot publish {data} to {topic}: Ether system not started")
            return
            
        if self._pub_socket is None:
            self._logger.debug("Publisher socket not initialized, setting up...")
            self._setup_publisher()
            
        # Convert data to JSON
        if isinstance(data, BaseModel):
            json_data = data.model_dump_json()
        elif isinstance(data, dict):
            json_data = json.dumps(data)
        else:
            raise TypeError("Data must be a dict or Pydantic model")
        
        # Publish message
        self._pub_socket.send_multipart([
            topic.encode(),
            json_data.encode()
        ])
        self._logger.debug(f"Published to {topic}: {json_data}")

    def _process_config(self, config: Union[str, dict, EtherConfig]):
        """Process the configuration"""
        # Process configuration
        try:
            if config:
                if isinstance(config, str):
                    self._config = _EtherConfig.from_yaml(config)
                elif isinstance(config, dict):
                    self._config = _EtherConfig.model_validate(config)
                elif isinstance(config, EtherConfig):
                    self._config = _EtherConfig(**config.model_dump())
                else:
                    self._config = _EtherConfig()
            else:
                self._config = _EtherConfig()
        except Exception as e:
            self._logger.error(f"Failed to process configuration: {e}", exc_info=True)
            raise

        # If we are running on the same machine as the session host, replace the public IP with the local IP
        try:
            public_ip = get_ip_address(use_public=True)
            if config.session.host == public_ip:
                # Replace with local IP
                local_ip = get_ip_address(use_public=False)
                self._logger.debug(f"Replacing public IP {public_ip} with local IP {local_ip} for connections")
                config.session.host = local_ip
        except Exception as e:
            self._logger.debug(f"Warning: Error checking IP addresses: {e}, using original host")
            

        self._logger.debug(f"Processed Config: {self._config}")
            
    
    def _handle_registry(self):


        # # Clean Redis state
        # self._logger.debug("Cleaning Redis state...")
        # liaison = EtherInstanceLiaison(session_config=self._config.session)
        # liaison.deregister_all()
        # liaison.store_registry_config({})

            
        # Store registry config in Redis if present
        if self._config and self._config.registry:
            # Convert the entire registry config to a dict
            registry_dict = {
                class_path: class_config.model_dump()
                for class_path, class_config in self._config.registry.items()
            }
            self._instance_manager.store_registry_config(registry_dict)
            EtherRegistry().process_registry_config(self._config.registry)
            
        # Process any pending classes
        EtherRegistry().process_pending_classes()
        
    def _handle_instances(self):
        if self._config and self._config.instances:
            for instance_name, instance_cfg in self._config.instances.items():
                instance_cfg.session_config = self._config.session
                self._config.instances[instance_name] = instance_cfg
            self._run_instances()

        self._is_main_session = True
        
    def _setup_public_sockets(self):
        self._logger.debug("Setting up publisher...")
        self._setup_publisher()

        self._logger.debug("Setting up request socket...")
        self._setup_request_socket()

    def start(self, config: Union[str, dict, EtherConfig] = None, allow_host: bool = True, ether_run: bool = False):
        """Start all daemon services"""

        try:
            if self._started:
                self._logger.debug("Ether system already started, skipping start")
                return
            
            # self.metadata = {
            #     "session_id": self.session_id,
            #     "ether_id": ether_id,
            #     "start_time": time.time(),
            #     "pid": os.getpid(),
            #     "public_ip": get_ip_address(),
            #     "config": self._config.model_dump()
            # }

            self._logger.debug(f"Start called with ether_id={self._ether_id}, config={config}, allow_host={allow_host}")

            self._process_config(config)

            self._session = EtherSession(ether_id=self._ether_id, config=self._config.session)

            self._instance_manager = _EtherInstanceManager(config=self._config)
            self._handle_registry()
            self._handle_instances()
                
            self._setup_public_sockets()
                
            self._started = True
            self._logger.info("Ether system started successfully")

        except Exception as e:
            self._logger.error(f"Error starting Ether system: {e}")
            self.shutdown()
    
    @property
    def session_info(self):
        return self._session.get_current_session(session_config=self._config.session)
    
    @property
    def is_host(self):
        return self.session_info.get("ether_id", None) == self._ether_id
    
    def _run_instances(self):
        """Start instances from configuration"""
        
        self._logger.debug("Starting instances")
        if not self._instance_manager:
            self._instance_manager = _EtherInstanceManager(config=self._config)
        self._instance_manager.launch_instances()
        # Wait for instances to be ready
        time.sleep(2.0)

    def _launch_instances(self, instances: dict):
        """Launch an instance from a config dictionary"""
        launch_config = self._config.model_dump()
        launch_config["instances"] = instances
        launch_config = _EtherConfig.model_validate(launch_config)
        self._instance_manager.launch_instances(launch_config)

    def _get_active_instances(self):
        """Get active instances"""
        return self._instance_manager.get_active_instances()


    def shutdown(self):
        """Shutdown all services"""        
        self._logger.info(f"Shutting down Ether: {self._ether_id}...")
        try:
            # stop all instances
            if self._instance_manager:
                self._logger.debug("Stopping all instances...")
                self._instance_manager.stop_all_instances()

            # close request socket
            if self._request_socket:
                self._logger.debug("Closing request socket...")
                self._request_socket.close()
                self._request_socket = None
            
            # close publishing socket and context
            if self._pub_socket:
                self._logger.debug("Closing publisher socket...")
                self._pub_socket.close()
                self._pub_socket = None 
            if self._zmq_context:
                self._zmq_context.term()
                self._zmq_context = None

            self._session.shutdown()

            self._started = False
        except Exception as e:
            self._logger.error(f"Error shutting down Ether: {e}")
            return

        
        finally:
            if not self._started:
                # Clean up logger
                if hasattr(self, '_logger'):
                    for handler in self._logger.handlers[:]:
                        handler.close()
                        self._logger.removeHandler(handler)

    def _setup_request_socket(self):
        """Set up the ZMQ request socket"""
        self._logger.debug("Setting up request socket")
        if self._request_socket is None:
            if self._zmq_context is None:
                self._zmq_context = zmq.Context()
            self._request_socket = self._zmq_context.socket(zmq.DEALER)
            self._request_socket.setsockopt(zmq.RCVTIMEO, 2500)
            self._request_socket.connect(f"tcp://{self._config.session.host}:{self._config.session.reqrep_frontend_port}")
            self._logger.debug("Request socket connected")

    def request(self, service_class: str, method_name: str, params=None, request_type="get", timeout=2500):
        """Make a request to a service"""
        self._logger.debug(f"Request received - Service: {service_class}.{method_name}.{request_type}")
        
        if self._request_socket is None:
            self._logger.debug("Request socket not initialized, setting up...")
            self._setup_request_socket()
        
        service_name = f"{service_class}.{method_name}.{request_type}".encode()
        self._logger.debug(f"Requesting from service: {service_name}")
        
        # Update socket timeout if different from default
        if timeout != 2500:
            self._request_socket.setsockopt(zmq.RCVTIMEO, timeout)
        
        try:
            request_data = {
                "timestamp": time.time(),
                "type": request_type,
                "params": params or {}
            }
            self._request_socket.send_multipart([
                b'',
                MDPC_CLIENT,
                service_name,
                json.dumps(request_data).encode()
            ])
            
            # Get reply with retries
            retries = 5
            while retries > 0:
                try:
                    msg = self._request_socket.recv_multipart()
                    self._logger.debug(f"Reply received: {msg}")
                    break
                except Exception as e:
                    self._logger.warning(f"Error receiving reply: {e}, retries remaining: {retries}")
                    retries -= 1
                    if retries == 0:
                        raise
                    self._logger.debug(f"Request timed out, retrying ({retries} attempts left)")
            
            assert msg[REPLY_MSG_CLIENT_INDEX] == MDPC_CLIENT
            assert msg[REPLY_MSG_SERVICE_INDEX] == service_name
            reply = json.loads(msg[REPLY_MSG_DATA_INDEX].decode())
            
            if request_type == "get":
                if isinstance(reply, dict) and reply.get("status") == "error":
                    return None # TODO: return Ether error message type that can be checked against using isinstance, this allows sending error to client
            
            return reply
        
        except Exception as e:

            reply = {
                "status": "error",
                "error": f"Request failed: {str(e)}"
            }

            if request_type == "get":
                if reply.get("status") == "success":
                    return reply["result"]
                else:
                    self._logger.error( f"Request failed: {reply.get('error', 'Unknown error')}")
                    return None
            
            return reply


        finally:
            # Reset timeout to default if it was changed
            if timeout != 2500:
                self._request_socket.setsockopt(zmq.RCVTIMEO, 2500)



# Create singleton instance but don't start it
_ether = _Ether()

# # Register cleanup
# @atexit.register
# def _cleanup_daemon():
#     daemon_manager.shutdown()