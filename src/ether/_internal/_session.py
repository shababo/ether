from pathlib import Path
import zmq
import time
import subprocess
import redis
from typing import Optional, Dict, Any
from time import sleep
from multiprocessing import Process
import tempfile

from ether.utils import get_ether_logger, get_ip_address
from ether.config import EtherSessionConfig
from ether._internal._discovery import _EtherDiscovery, session_discovery_launcher
from ether._internal._pubsub import _run_pubsub
from ether._internal._reqrep import MDPC_CLIENT, _run_reqrep_broker

# DISCOVERY_PUB_PORT = 301309
# QUERY_PORT = 301310

class EtherSession: 
    _ether_id = None
    _logger = None
    _config = None
    _redis_pidfile = None
    _redis_process = None
    _pubsub_process = None
    _monitor_process = None
    _ether_discovery_process = None
    _session_metadata = None
    _started = False
    _is_host = False
    # _request_socket = None
    _zmq_context = None
    _reqrep_broker_process = None

    def __init__(self, ether_id: str = "default", config: Optional[EtherSessionConfig] = None):
        self._ether_id = ether_id
        self.context = zmq.Context()
        self.is_host = False
        self.running = False
        self._logger = get_ether_logger(process_name="EtherSession")
        
        # Use provided config or defaults
        self._config = config or EtherSessionConfig()
        
        self._session_discovery()

        self._started = True
        

    def _setup_sockets(self):
        self.pub_socket = self.context.socket(zmq.PUB)
        # Bind to all interfaces for remote connections
        self.pub_socket.bind(f"tcp://*:{self._config.discovery_port}")
        
        self.rep_socket = self.context.socket(zmq.REP)
        self.rep_socket.bind(f"tcp://*:{self._config.query_port}")

    def _session_discovery(self):

        # Attempt to host session
        if self._config.allow_host:
            self._logger.debug("Starting session discovery process...")
            try:
                self._ether_discovery_process = Process(
                    target=session_discovery_launcher, 
                    kwargs={"ether_id": self._ether_id, "session_config": self._config}
                )
                self._ether_discovery_process.start()
                self._logger.info("Session discovery process started")
                time.sleep(1.0)
            except Exception as e:
                self._logger.error(f"Failed to start session discovery process: {e}", exc_info=True)
                raise
        
        # If not allow_host, we still want to retry if first discovery fails
        # If we are host, confirm that discvoery is running
        session_metadata = None
        retries = 5 
        while retries > 0 and not session_metadata:
            try:
                session_metadata = self.get_current_session(session_config=self._config)
                assert session_metadata

                self._logger.debug(f"Existing session metadata: {session_metadata}")

            except Exception as e:
                
                retries -= 1
                if retries > 0:
                    time.sleep(1.0)
                    continue
                else:
                    self._logger.error(f"Failed to connect to session discovery process: {e}", exc_info=True)
                    raise e
                
        if session_metadata['ether_id'] == self._ether_id:
            self._is_host = True
            self._launch_ether_host_services()

    def _launch_ether_host_services(self):
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

    def _ensure_redis_running(self) -> bool:
        """Ensure Redis server is running, start if not"""
        
        self._start_redis_server()
        return self._test_redis_connection()
    
    def _ensure_pubsub_running(self) -> bool:
        """Ensure PubSub proxy is running"""
        
        if self._pubsub_process is None:
            self._pubsub_process = Process(
                target=_run_pubsub,
                args=(self._config.pubsub_frontend_port, self._config.pubsub_backend_port)
            )
            self._pubsub_process.daemon = True
            self._pubsub_process.start()

        return self._test_pubsub_connection()
    
    def _ensure_reqrep_running(self) -> bool:
        """Ensure ReqRep broker is running"""
        if self._reqrep_broker_process is None:
            self._reqrep_broker_process = Process(
                target=_run_reqrep_broker,
                args=(self._config.reqrep_frontend_port, self._config.reqrep_backend_port)
            )
            self._reqrep_broker_process.daemon = True
            self._reqrep_broker_process.start()
        return self._test_reqrep_connection()

    def _test_reqrep_connection(self) -> bool:
        """Test ReqRep broker connection"""
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        max_attempts = 10
        for _ in range(max_attempts):
            try:
                socket.connect(f"tcp://{self._config.host}:{self._config.reqrep_frontend_port}")
                socket.send_multipart([
                    MDPC_CLIENT,
                    "ping".encode(),
                    "ping".encode(),
                ])
                socket.RCVTIMEO = 1000  # 1 second timeout
                response = socket.recv_multipart()
                if response[2].decode() == "pong":
                    return True
            except zmq.error.ZMQError:
                self._logger.debug(f"ReqRep broker not ready, waiting")
                time.sleep(0.1)
            finally:
                socket.close()
                context.term()
        return False
    
    def _test_redis_connection(self) -> bool:
        """Test Redis connection"""
        max_attempts = 10
        for _ in range(max_attempts):
            try:
                r = redis.Redis(port=self._config.redis_port)
                r.ping()
                r.close()
                return True
            except Exception as e:
                time.sleep(0.1)

        return False
                
        
    def _test_pubsub_connection(self) -> bool:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        max_attempts = 10
        for _ in range(max_attempts):  
            try:
                socket.connect(f"tcp://{self._config.host}:{self._config.pubsub_frontend_port}")
                return True
            except zmq.error.ZMQError:
                time.sleep(0.1)
            finally:
                socket.close()
                context.term()
        return False
    
    def _start_redis_server(self):
        """Start Redis server process"""
        if self._redis_process is not None:
            self._logger.debug("Redis server already running, skipping start")
            return
        
        self._logger.debug("Starting Redis server")
        self._redis_pidfile = Path(tempfile.gettempdir()) / 'ether_redis.pid'
        self._redis_process = subprocess.Popen(
            [
                'redis-server',
                '--port', str(self._config.redis_port),
                '--bind', self._config.redis_host,
                '--dir', tempfile.gettempdir(),  # Use temp dir for dump.rdb
                '--save', "", 
                '--appendonly', 'no',
                '--protected-mode', 'no'
            ],
        )

    def shutdown(self):
        if not self._is_host:
            self._logger.debug(f"Session is not host, skipping shutdown")
        elif self._started:
            self._logger.debug(f"Shutting down Ether Session host")
            
            try:

                # stop session discovery process
                if self._ether_discovery_process:
                    self._logger.debug("Stopping session discovery process...")
                    self._ether_discovery_process.terminate()
                    self._ether_discovery_process.join(timeout=2)
                    self._ether_discovery_process = None

                

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
                    
                # self._ether_session_process.terminate()
                self._started = False
                self._logger.info("Ether Session shutdown complete")
            
            except Exception as e:
                self._logger.error(f"Error during shutdown: {e}")

    # def _disconnect_req_service(self, service_name: str) -> dict:
    #     """Disconnect a request-reply service using MDP protocol"""
    #     self._logger.debug(f"Disconnecting service: {service_name}")
        
    #     if not self._started:
    #         self._logger.warning("Cannot disconnect: Ether system not started")
    #         return {"status": "error", "error": "Ether system not started"}
        
    #     if self._request_socket is None:
    #         self._logger.warning("Cannot disconnect: No request socket")
    #         return {"status": "error", "error": "No request socket"}
        
    #     # Set shorter timeout for disconnect
    #     original_timeout = self._request_socket.getsockopt(zmq.RCVTIMEO)
    #     self._request_socket.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout
        
    #     try:
    #         # Send disconnect command through client protocol
    #         self._request_socket.send_multipart([
    #             MDPC_CLIENT,
    #             service_name.encode(),
    #             W_DISCONNECT  # Use command directly as request data
    #         ])
    #         self._logger.debug("Disconnect command sent")
            
    #         # Wait for acknowledgment with retries
    #         retries = 3
    #         while retries > 0:
    #             try:
    #                 msg = self._request_socket.recv_multipart()
    #                 reply = json.loads(msg[REPLY_MSG_DATA_INDEX].decode())
    #                 self._logger.debug(f"Received disconnect reply: {reply}")
    #                 return reply
    #             except zmq.error.Again:
    #                 retries -= 1
    #                 if retries > 0:
    #                     self._logger.debug(f"Retrying disconnect response, {retries} attempts left")
    #                     time.sleep(0.1)
    #                 else:
    #                     return {
    #                         "status": "success",
    #                         "result": {"status": "disconnected", "note": "No acknowledgment received"}
    #                     }
    #     except Exception as e:
    #         self._logger.error(f"Error disconnecting service: {e}")
    #         return {
    #             "status": "error",
    #             "error": f"Disconnect failed: {str(e)}"
    #         }
    #     finally:
    #         # Restore original timeout
    #         self._request_socket.setsockopt(zmq.RCVTIMEO, original_timeout)
        
        

    @classmethod
    def get_current_session(cls, timeout: int = 200, session_config: Optional[EtherSessionConfig] = None) -> Optional[Dict[str, Any]]:  # reduced timeout
        # print(f"Process {os.getpid()}: Attempting to get current session...")
        context = zmq.Context()
        req_socket = context.socket(zmq.REQ)

        # Use provided session config or defaults
        session_config = session_config or EtherSessionConfig()
        
        try:
            req_socket.setsockopt(zmq.LINGER, 0)  # Don't wait on close
            req_socket.connect(f"tcp://{session_config.host}:{session_config.session_query_port}")
            # print(f"Process {os.getpid()}: Connected to query port")
            
            req_socket.send_json({"type": "query"})
            # print(f"Process {os.getpid()}: Sent query")
            
            poller = zmq.Poller()
            poller.register(req_socket, zmq.POLLIN)
            
            # print(f"Process {os.getpid()}: Waiting for response...")
            events = dict(poller.poll(timeout))
            if req_socket in events:
                response = req_socket.recv_json()
                # print(f"Process {os.getpid()}: Received response")
                if response.get("type") == "response":
                    return response["data"]
            # print(f"Process {os.getpid()}: No response received")
            return None
            
        except Exception as e:
            # print(f"Process {os.getpid()}: Error in get_current_session: {e}")
            return None
        finally:
            req_socket.close()
            context.term()

