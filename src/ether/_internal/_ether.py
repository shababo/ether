# Modify existing _proxy.py to include daemon functionality
import subprocess
import time
from pathlib import Path
import tempfile
import redis
import os
from multiprocessing import Process
import zmq
from typing import Union, Dict, Optional
from pydantic import BaseModel
import json
import signal
import multiprocessing

from ..utils import get_ether_logger, get_ip_address
from ._session import EtherSession, session_discovery_launcher
from ._pubsub import _EtherPubSubProxy
from ..liaison import EtherInstanceLiaison 
from ._manager import _EtherInstanceManager
from ._config import EtherConfig, EtherNetworkConfig
from ._registry import EtherRegistry
from ._reqrep import (
    MDPW_WORKER,
    W_DISCONNECT,
    EtherReqRepBroker,
    MDPC_CLIENT,
    REPLY_CLIENT_INDEX,
    REPLY_SERVICE_INDEX,
    REPLY_DATA_INDEX,
)


# Constants
CULL_INTERVAL = 10  # seconds between culling checks

def _run_pubsub(network_config: Optional[EtherNetworkConfig] = None):
    """Standalone function to run PubSub proxy
    
    Args:
        network_config: Network configuration for the proxy
    """
    # Get network config from session if not provided
    if network_config is None:
        session_data = EtherSession.get_current_session()
        if session_data and "network" in session_data:
            network_config = EtherNetworkConfig.model_validate(session_data["network"])
        else:
            network_config = EtherNetworkConfig()
    
    proxy = _EtherPubSubProxy(network_config=network_config)
    
    def handle_stop(signum, frame):
        """Handle stop signal by cleaning up proxy"""
        proxy._logger.debug("Received stop signal, shutting down proxy...")
        proxy.cleanup()
        os._exit(0)  # Exit cleanly
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    
    proxy.run()

def _run_monitor(network_config: Optional[EtherNetworkConfig] = None):
    """Standalone function to run instance monitoring"""
    logger = get_ether_logger("EtherMonitor")
    liaison = EtherInstanceLiaison(network_config=network_config)
    
    while True:
        try:
            # Cull dead processes first
            culled = liaison.cull_dead_processes()
            if culled:
                logger.debug(f"Culled {culled} dead instances")
            
            # Get remaining active instances
            instances = liaison.get_active_instances()
            logger.debug(f"Active instances: {instances}")
            time.sleep(CULL_INTERVAL)  # Check every CULL_INTERVAL seconds
        except Exception as e:
            logger.error(f"Error monitoring instances: {e}")
            time.sleep(1)

def _run_reqrep_broker(frontend_port: int = 5559, backend_port: int = 5560):
    """Run the request-reply broker in a separate process"""
    broker = EtherReqRepBroker(frontend_port=frontend_port, backend_port=backend_port)
    try:
        broker.run()
    except KeyboardInterrupt:
        pass
    finally:
        broker.cleanup()

class _Ether:
    """Singleton to manage Ether services behind the scenes."""
    _instance = None
    _ether_id = None
    _logger = None
    _config = None
    _redis_pidfile = None
    _redis_process = None
    _pubsub_process = None
    _monitor_process = None
    _instance_manager = None
    _ether_session_process = None
    _started = False
    _is_main_session = False
    _pub_socket = None
    _request_socket = None
    _zmq_context = None
    _pubsub_proxy_process: Optional[multiprocessing.Process] = None
    _reqrep_broker_process: Optional[multiprocessing.Process] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_Ether, cls).__new__(cls)
            cls._instance._logger = get_ether_logger("Ether")
            cls._instance._logger.debug("Initializing Ether instance")
            cls._instance._redis_pidfile = Path(tempfile.gettempdir()) / 'ether_redis.pid'
            

        return cls._instance
        
    
    def _setup_publisher(self):
        """Set up the ZMQ publisher socket"""
        self._logger.debug("Setting up publisher socket")
        if self._pub_socket is None:
            self._zmq_context = zmq.Context()
            self._pub_socket = self._zmq_context.socket(zmq.PUB)
            self._pub_socket.connect(f"tcp://{self._config.network.host}:{self._config.network.pubsub_backend_port}")
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
    
    def start(self, ether_id: str, config = None, restart: bool = False, discovery = True):
        """Start all daemon services"""
        self._logger.debug(f"Start called with ether_id={ether_id}, config={config}, restart={restart}")
        self._ether_id = ether_id

        # Process configuration
        try:
            if config:
                if isinstance(config, (str, dict)):
                    self._config = EtherConfig.from_yaml(config) if isinstance(config, str) else EtherConfig.model_validate(config)
                elif isinstance(config, EtherConfig):
                    self._config = config
                else:
                    self._config = EtherConfig()
        except Exception as e:
            self._logger.error(f"Failed to process configuration: {e}", exc_info=True)
            raise

        try:
            public_ip = get_ip_address(use_public=True)
            if config.network.host == public_ip:
                # Replace with local IP
                local_ip = get_ip_address(use_public=False)
                self._logger.debug(f"Replacing public IP {public_ip} with local IP {local_ip} for connections")
                config.network.host = local_ip
        except Exception as e:
            self._logger.warning(f"Error checking IP addresses: {e}, using original host")
            

        self._logger.debug(f"Processed Config: {self._config}")

        # Start session with network config
        if discovery:
            try:
                self._ether_session_process = Process(
                    target=session_discovery_launcher, 
                    args=(self._ether_id, self._config.network)
                )
                self._ether_session_process.start()
                time.sleep(1.0)
            except Exception as e:
                self._logger.error(f"Failed to start session discovery process: {e}", exc_info=True)
                raise

        try:
            session_metadata = EtherSession.get_current_session(network_config=self._config.network)
            self._logger.debug(f"Existing session metadata: {session_metadata}")
            
            if session_metadata and session_metadata.get("ether_id") == ether_id:
                self._logger.info(f"Starting Ether session: session id: {session_metadata['session_id']}, session ether id: {session_metadata['ether_id']}...")
                self._is_main_session = True

                # TODO: review restart logic below, not sure we need it, and if we do if it's in the right place
                if self._started:
                    if restart:
                        self._logger.info("Restarting Ether session...")
                        self.shutdown()
                    else:
                        self._logger.debug("Ether session already started, skipping start")
                        return
                
                # Start Redis
                self._logger.debug("Starting Redis server...")
                try:
                    if not self._ensure_redis_running():
                        raise RuntimeError("Redis server failed to start")
                except Exception as e:
                    self._logger.error(f"Redis startup failed: {e}", exc_info=True)
                    raise
                
                # Start Messaging
                self._logger.debug("Starting PubSub proxy...")
                try:
                    if not self._ensure_pubsub_running():
                        raise RuntimeError("PubSub proxy failed to start")
                except Exception as e:
                    self._logger.error(f"PubSub startup failed: {e}", exc_info=True)
                    raise
                
                # Start ReqRep broker
                self._logger.debug("Starting ReqRep broker...")
                try:
                    if not self._ensure_reqrep_running():
                        raise RuntimeError("ReqRep broker failed to start")
                except Exception as e:
                    self._logger.error(f"ReqRep broker startup failed: {e}", exc_info=True)
                    raise
                
                # Start monitoring
                self._logger.debug("Starting instance monitor...")
                self._monitor_process = Process(target=_run_monitor, args=(self._config.network,))
                self._monitor_process.start()
                
                # Clean Redis state
                self._logger.debug("Cleaning Redis state...")
                liaison = EtherInstanceLiaison(network_config=self._config.network)
                liaison.deregister_all()
                liaison.store_registry_config({})

                

            else:
                self._logger.warning(f"Joining Ether session, session id: {session_metadata['session_id']}, session ether id: {session_metadata['ether_id']}")
                self._started = True
                
                # Store registry config in Redis if present
                if self._config and self._config.registry:
                    # Convert the entire registry config to a dict
                    registry_dict = {
                        class_path: class_config.model_dump()
                        for class_path, class_config in self._config.registry.items()
                    }
                    liaison = EtherInstanceLiaison(network_config=self._config.network)
                    liaison.store_registry_config(registry_dict)
                    EtherRegistry().process_registry_config(self._config.registry)
                
                # Process any pending classes
                EtherRegistry().process_pending_classes()

            if self._config and self._config.instances:
                for instance_name, instance_cfg in self._config.instances.items():
                    instance_cfg.network_config = self._config.network
                    self._config.instances[instance_name] = instance_cfg
                self._start_instances()

        except Exception as e:
            self._logger.error(f"Error during Ether startup: {e}", exc_info=True)
            # Clean up any started processes
            self.shutdown()
            raise
        
        self._logger.debug("Setting up publisher...")
        self._setup_publisher()

        self._logger.debug("Setting up request socket...")
        self._setup_request_socket()
        
        self._started = True
        self._logger.info("Ether system started successfully")
    
    def _ensure_redis_running(self) -> bool:
        """Ensure Redis server is running, start if not"""
        if self._redis_pidfile.exists():
            with open(self._redis_pidfile) as f:
                pid = int(f.read().strip())
            try:
                os.kill(pid, 0)
                return self._test_redis_connection()
            except (OSError, redis.ConnectionError):
                self._redis_pidfile.unlink()
        
        self._start_redis_server()
        return self._test_redis_connection()
    
    def _ensure_pubsub_running(self) -> bool:
        """Ensure PubSub proxy is running"""
        # Clean up any existing ZMQ contexts
        zmq.Context.instance().term()
        if self._pubsub_process is None:
            self._pubsub_process = Process(
                target=_run_pubsub,
                args=(self._config.network,)
            )
            self._pubsub_process.daemon = True
            self._pubsub_process.start()

        return self._test_pubsub_connection()
    
    def _ensure_reqrep_running(self) -> bool:
        """Ensure ReqRep broker is running"""
        if self._reqrep_broker_process is None:
            self._reqrep_broker_process = Process(target=_run_reqrep_broker)
            self._reqrep_broker_process.daemon = True
            self._reqrep_broker_process.start()
        return self._test_reqrep_connection()

    def _test_reqrep_connection(self) -> bool:
        """Test ReqRep broker connection"""
        # TODO: Implement actual connection test
        return True
    
    def _test_redis_connection(self) -> bool:
        """Test Redis connection"""
        try:
            r = redis.Redis(port=self._config.network.redis_port)
            r.ping()
            r.close()
            return True
        except redis.ConnectionError:
            return False
        
    def _test_pubsub_connection(self) -> bool:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        for _ in range(10):  # Try for 5 seconds
            try:
                socket.connect(f"tcp://{self._config.network.host}:{self._config.network.pubsub_frontend_port}")
                socket.close()
                context.term()
                return True
            except zmq.error.ZMQError:
                time.sleep(0.5)
        return False
    
    def _start_redis_server(self):
        """Start Redis server process"""
        self._logger.debug("Starting Redis server")
        self._redis_process = subprocess.Popen(
            [
                'redis-server',
                '--port', str(self._config.network.redis_port),
                '--bind', self._config.network.redis_host,
                '--dir', tempfile.gettempdir(),  # Use temp dir for dump.rdb
                '--save', "", 
                '--appendonly', 'no',
                '--protected-mode', 'no'
            ],
        )
        # Save PID
        with open(self._redis_pidfile, 'w') as f:
            f.write(str(self._redis_process.pid))
        
        # Wait for Redis to be ready
        time.sleep(0.5)
        max_attempts = 10
        for _ in range(max_attempts):
            try:
                if self._test_redis_connection():
                    self._logger.debug("Redis server ready")
                    break
                else:
                    self._logger.debug("Redis server not ready, waiting")
                    time.sleep(0.5)
            except:
                self._logger.debug("Redis server not ready, waiting")
                time.sleep(0.5)
        else:
            raise RuntimeError("Redis server failed to start")
    
    def _start_instances(self):
        """Start instances from configuration"""
        
        self._logger.debug("Starting instances")
        if not self._instance_manager:
            self._instance_manager = _EtherInstanceManager(config=self._config)
        else:
            self._instance_manager.launch_instances(self._config)
        # Wait for instances to be ready
        time.sleep(1.0)

    # def save(self):
    #     if self._pub_socket:
    #         self._pub_socket.send_multipart([
    #             "Ether.save",
    #             "{}".encode()
    #         ])

    # def cleanup(self):
    #     if self._pub_socket:
    #         self._pub_socket.send([
    #             "Ether.cleanup",
    #             "{}".encode()
    #         ])

    def shutdown(self):
        """Shutdown all services"""        
        self._logger.info(f"Shutting down Ether session: {self._ether_id}...")
        # session_metadata = EtherSession.get_current_session()
        if not self._is_main_session:
            self._logger.debug(f"Session metadata does not match ether_id {self._ether_id}, skipping shutdown")
        else:
            self._logger.debug(f"Session metadata matches ether_id {self._ether_id}, shutting down session")
            
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

                
                    
                # Terminate pubsub proxy process
                if self._pubsub_process:
                    self._logger.debug("Shutting down PubSub proxy")
                    self._pubsub_process.terminate()  # This will trigger SIGTERM
                    self._pubsub_process.join(timeout=2)
                    if self._pubsub_process.is_alive():
                        self._logger.warning("PubSub proxy didn't stop gracefully, killing")
                        self._pubsub_process.kill()
                        self._pubsub_process.join(timeout=1)
                    self._pubsub_process = None
                    
                # Terminate ReqRep broker process
                if self._reqrep_broker_process:
                    self._logger.debug("Shutting down ReqRep broker")
                    self._reqrep_broker_process.terminate()
                    self._reqrep_broker_process.join(timeout=2)
                    if self._reqrep_broker_process.is_alive():
                        self._logger.warning("ReqRep broker didn't stop gracefully, killing")
                        self._reqrep_broker_process.kill()
                        self._reqrep_broker_process.join(timeout=1)
                    self._reqrep_broker_process = None
                    
                # Terminate instance monitoring process
                if self._monitor_process:
                    self._logger.debug("Shutting down monitor")
                    self._monitor_process.terminate()
                    self._monitor_process.join(timeout=2)
                    if self._monitor_process.is_alive():
                        self._monitor_process.kill()
                        self._monitor_process.join(timeout=1)
                    self._monitor_process = None
                    
                # Terminate Redis server
                try:
                    if self._redis_process:
                        self._logger.debug("Shutting down Redis server")
                        try:
                            if self._redis_process.poll() is None:

                                self._redis_process.terminate()
                                self._redis_process.wait(timeout=5)
                                if self._redis_process.poll() is None:
                                    self._redis_process.kill()
                                    self._redis_process.wait(timeout=1)
                        except Exception as e:
                            self._logger.warning(f"Error terminating Redis process: {e}")
                        finally:
                            self._redis_process = None
                            
                        if hasattr(self, '_redis_pidfile') and self._redis_pidfile.exists():
                            self._redis_pidfile.unlink()
                except Exception as e:
                    self._logger.error(f"Error cleaning up Redis: {e}")
                    
                self._ether_session_process.terminate()
                self._started = False
                self._logger.info("Ether system shutdown complete")
            
            except Exception as e:
                self._logger.error(f"Error during shutdown: {e}")
            finally:
                # Clean up logger
                if hasattr(self, '_logger'):
                    for handler in self._logger.handlers[:]:
                        handler.close()
                        self._logger.removeHandler(handler)
        
    def cleanup(self):
        """Clean up Ether resources"""
        self._logger.debug("Cleaning up Ether")
        
        # Terminate proxy process
        if self._pubsub_proxy_process and self._pubsub_proxy_process.is_alive():
            self._logger.debug("Terminating PubSub proxy")
            self._pubsub_proxy_process.terminate()
            self._pubsub_proxy_process.join(timeout=1)
            if self._pubsub_proxy_process.is_alive():
                self._logger.warning("PubSub proxy didn't terminate, killing")
                self._pubsub_proxy_process.kill()
                self._pubsub_proxy_process.join(timeout=1)
                
        # Terminate broker process
        if self._reqrep_broker_process and self._reqrep_broker_process.is_alive():
            self._logger.debug("Terminating ReqRep broker")
            self._reqrep_broker_process.terminate()
            self._reqrep_broker_process.join(timeout=1)
            if self._reqrep_broker_process.is_alive():
                self._logger.warning("ReqRep broker didn't terminate, killing")
                self._reqrep_broker_process.kill()
                self._reqrep_broker_process.join(timeout=1)

    def _setup_request_socket(self):
        """Set up the ZMQ request socket"""
        self._logger.debug("Setting up request socket")
        if self._request_socket is None:
            if self._zmq_context is None:
                self._zmq_context = zmq.Context()
            self._request_socket = self._zmq_context.socket(zmq.DEALER)
            self._request_socket.setsockopt(zmq.RCVTIMEO, 2500)
            self._request_socket.connect(f"tcp://{self._config.network.host}:{self._config.network.reqrep_frontend_port}")
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
            
            assert msg[REPLY_CLIENT_INDEX] == MDPC_CLIENT
            assert msg[REPLY_SERVICE_INDEX] == service_name
            reply = json.loads(msg[REPLY_DATA_INDEX].decode())
            
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

    def _disconnect_req_service(self, service_name: str) -> dict:
        """Disconnect a request-reply service using MDP protocol"""
        self._logger.debug(f"Disconnecting service: {service_name}")
        
        if not self._started:
            self._logger.warning("Cannot disconnect: Ether system not started")
            return {"status": "error", "error": "Ether system not started"}
        
        if self._request_socket is None:
            self._logger.warning("Cannot disconnect: No request socket")
            return {"status": "error", "error": "No request socket"}
        
        # Set shorter timeout for disconnect
        original_timeout = self._request_socket.getsockopt(zmq.RCVTIMEO)
        self._request_socket.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout
        
        try:
            # Send disconnect command through client protocol
            self._request_socket.send_multipart([
                MDPC_CLIENT,
                service_name.encode(),
                W_DISCONNECT  # Use command directly as request data
            ])
            self._logger.debug("Disconnect command sent")
            
            # Wait for acknowledgment with retries
            retries = 3
            while retries > 0:
                try:
                    msg = self._request_socket.recv_multipart()
                    reply = json.loads(msg[REPLY_DATA_INDEX].decode())
                    self._logger.debug(f"Received disconnect reply: {reply}")
                    return reply
                except zmq.error.Again:
                    retries -= 1
                    if retries > 0:
                        self._logger.debug(f"Retrying disconnect response, {retries} attempts left")
                        time.sleep(0.1)
                    else:
                        return {
                            "status": "success",
                            "result": {"status": "disconnected", "note": "No acknowledgment received"}
                        }
        except Exception as e:
            self._logger.error(f"Error disconnecting service: {e}")
            return {
                "status": "error",
                "error": f"Disconnect failed: {str(e)}"
            }
        finally:
            # Restore original timeout
            self._request_socket.setsockopt(zmq.RCVTIMEO, original_timeout)

# Create singleton instance but don't start it
_ether = _Ether()

# # Register cleanup
# @atexit.register
# def _cleanup_daemon():
#     daemon_manager.shutdown()