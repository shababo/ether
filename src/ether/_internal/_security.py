import os
import sys
import zmq
import json
from pathlib import Path
from datetime import datetime, timezone
from zmq.auth import create_certificates, load_certificate
from typing import Tuple, Optional

class KeyManager:
    def __init__(self, app_name: str = "myapp"):
        self.app_name = app_name
        self.keys_dir = self._get_platform_config_dir() / app_name / "keys"
        self.metadata_file = self.keys_dir / "metadata.json"
        self._ensure_dirs()

    def _get_platform_config_dir(self) -> Path:
        """Get the appropriate config directory for the current platform"""
        if sys.platform.startswith('win'):
            # Windows: C:\Users\Username\AppData\Local
            return Path(os.environ.get('LOCALAPPDATA'))
        elif sys.platform.startswith('darwin'):
            # macOS: ~/Library/Application Support
            return Path.home() / "Library" / "Application Support"
        else:
            # Linux/Unix: ~/.config
            return Path.home() / ".config"

    def _ensure_dirs(self) -> None:
        """Create necessary directories if they don't exist"""
        self.keys_dir.mkdir(parents=True, exist_ok=True)

    def _save_metadata(self, metadata: dict) -> None:
        """Save key metadata"""
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f)

    def _load_metadata(self) -> dict:
        """Load key metadata"""
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        return {}

    def _should_rotate_keys(self, metadata: dict) -> bool:
        """Check if keys should be rotated based on age"""
        if not metadata:
            return True
        
        created_at = datetime.fromisoformat(metadata.get('created_at', ''))
        now = datetime.now(timezone.utc)
        # Rotate keys if they're older than 90 days
        return (now - created_at).days > 90

    def get_or_create_certificates(self) -> Tuple[bytes, bytes]:
        """Get existing certificates or create new ones"""
        metadata = self._load_metadata()
        
        # Check for key rotation
        if self._should_rotate_keys(metadata):
            return self._create_new_certificates()

        # Load existing certificates
        try:
            public_key, secret_key = load_certificate(
                self.keys_dir / "server.key_secret"
            )
            return public_key, secret_key
        except Exception:
            return self._create_new_certificates()

    def _create_new_certificates(self) -> Tuple[bytes, bytes]:
        """Create new certificates and save metadata"""
        # Archive old keys if they exist
        self._archive_old_keys()
        
        # Create new keys
        public_file, secret_file = create_certificates(
            self.keys_dir, "server"
        )
        
        # Save metadata
        metadata = {
            'created_at': datetime.now(timezone.utc).isoformat(),
            'public_key': str(public_file),
            'secret_key': str(secret_file)
        }
        self._save_metadata(metadata)
        
        # Load and return the new certificates
        return load_certificate(self.keys_dir / "server.key_secret")

    def _archive_old_keys(self) -> None:
        """Archive old keys with timestamp"""
        if not self.metadata_file.exists():
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_dir = self.keys_dir / "archive" / timestamp
        archive_dir.mkdir(parents=True, exist_ok=True)

        # Move old keys to archive
        for key_file in self.keys_dir.glob("server.key*"):
            if key_file.is_file():
                key_file.rename(archive_dir / key_file.name)

class SessionManager:
    def start_key_rotation(self, socket, peer_id):
        """Periodically rotate session keys while maintaining connection"""
        def rotate_keys():
            while True:
                new_keys = self.generate_session_keys()
                self.signal_key_rotation(socket, peer_id, new_keys)
                time.sleep(KEY_ROTATION_INTERVAL)
                
        threading.Thread(target=rotate_keys, daemon=True).start()



class SessionManager4:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.group_key = None
            self.security_level = SecurityLevel.BASIC
            self.roles = {}
            self.is_host = False
            self.session_keys = {}  # For HIGH security with key rotation
            self.initialized = True
    
    def set_group_key(self, key: str):
        self.group_key = key
        # Derive base keys for all ZMQ connections
        self.base_public_key, self.base_secret_key = derive_keys(key)

class SessionManager3:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.session_key = None
            self.host_key = None
            self.security_level = SecurityLevel.STANDARD
            self.initialized = True
    
    def create_session(self, security_level: SecurityLevel = SecurityLevel.STANDARD) -> str:
        """Host creates a new session"""
        self.session_key = generate_session_key()  # Strong random key
        self.host_key = generate_host_key()  # Host's admin key
        self.security_level = security_level
        return self.session_key

    def join_session(self, session_key: str):
        """Clients join existing session"""
        self.session_key = session_key

class SecureMessaging3:
    def __init__(self):
        self.context = zmq.Context()
        self.session = SessionManager()
    
    def create_secure_socket(self, socket_type: int) -> zmq.Socket:
        socket = self.context.socket(socket_type)
        
        # Use session key for encryption
        if self.session.session_key:
            public_key, secret_key = derive_keys(self.session.session_key)
            socket.curve_secretkey = secret_key
            socket.curve_publickey = public_key
            socket.curve_server = True
            
        return socket

class SecureMessaging2:
    def __init__(self):
        self.context = zmq.Context()
        self.key_manager = KeyManager()
        self.session_manager = SessionManager()

    def create_secure_socket(self, socket_type: int, peer_id: str = None):
        socket = self.context.socket(socket_type)
        
        # Base security with CurveZMQ
        public_key, secret_key = self.key_manager.keys
        socket.curve_secretkey = secret_key
        socket.curve_publickey = public_key
        
        # Add session key rotation for long-term connections
        if peer_id:
            self.session_manager.start_key_rotation(socket, peer_id)
            
        return socket

class SecureMessaging:
    def __init__(self, app_name: str = "myapp"):
        self.context = zmq.Context()
        self.key_manager = KeyManager(app_name)
        self._public_key = None
        self._secret_key = None

    @property
    def keys(self) -> Tuple[bytes, bytes]:
        """Lazy load keys"""
        if not (self._public_key and self._secret_key):
            self._public_key, self._secret_key = (
                self.key_manager.get_or_create_certificates()
            )
        return self._public_key, self._secret_key

    def create_secure_socket(self, socket_type: int) -> zmq.Socket:
        """Create a secure socket of the specified type"""
        socket = self.context.socket(socket_type)
        public_key, secret_key = self.keys
        
        socket.curve_secretkey = secret_key
        socket.curve_publickey = public_key
        socket.curve_server = True
        
        return socket

    def create_secure_client_socket(
        self, 
        socket_type: int, 
        server_public_key: bytes
    ) -> zmq.Socket:
        """Create a secure client socket"""
        socket = self.context.socket(socket_type)
        public_key, secret_key = self.keys
        
        socket.curve_secretkey = secret_key
        socket.curve_publickey = public_key
        socket.curve_serverkey = server_public_key
        
        return socket

# Access Control and Method Security 
class ServiceDecorator:
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Verify caller has permission to execute this method
            if not self.verify_caller_permissions(func.__name__):
                raise SecurityError("Unauthorized method call")
            
            # Validate input parameters
            if not self.validate_parameters(func, args, kwargs):
                raise SecurityError("Invalid parameters")
            
            # Log access for audit
            self.log_method_access(func.__name__)
            
            return func(*args, **kwargs)
        return wrapper

    def verify_caller_permissions(self, method_name):
        """Check if caller is allowed to execute this method"""
        # Implement role-based access control
        return True
    
# Equipment/Resource Protection
class ResourceManager:
    def __init__(self):
        self.protected_resources = set()
        self.resource_locks = {}

    def protect_resource(self, resource_name, allowed_operations):
        """Register protected lab equipment/resources"""
        self.protected_resources.add({
            'name': resource_name,
            'allowed_ops': allowed_operations,
            'lock': threading.Lock()
        })

    def validate_operation(self, resource, operation):
        """Ensure operation is safe and allowed"""
        if resource not in self.protected_resources:
            return False
        return operation in resource['allowed_ops']
    
# Message Privacy and Integrity
class MessageHandler:
    def __init__(self):
        self.security_level = SecurityLevel.STANDARD
        
    def send_message(self, socket, message, security_level=None):
        """Send message with appropriate security"""
        level = security_level or self.security_level
        
        if level == SecurityLevel.HIGH:
            # Use additional encryption for sensitive data
            message = self.encrypt_sensitive_data(message)
            
        # Add message integrity check
        message['hmac'] = self.generate_hmac(message)
        
        socket.send_json(message)

# System Hardening
class SystemSecurity:
    def harden_environment(self):
        """Implement system-level security measures"""
        # Restrict file system access
        self.set_working_directory()
        
        # Monitor system resources
        self.start_resource_monitor()
        
        # Set up network isolation
        self.configure_network_restrictions()
        
    def set_working_directory(self):
        """Restrict file system access to specific directories"""
        os.chdir(self.safe_working_dir)
        
    def start_resource_monitor(self):
        """Monitor CPU, memory, disk usage for anomalies"""
        ResourceMonitor().start()


"""

Example usage:

# Decorating a lab equipment method
@service(security_level=SecurityLevel.HIGH)
@requires_permission('equipment_control')
def control_microscope(self, parameters):
    if not resource_manager.validate_operation('microscope', parameters['operation']):
        raise SecurityError("Operation not allowed")
    
    # Proceed with microscope control
    return self._execute_microscope_command(parameters)

# Long-running data acquisition
@service(security_level=SecurityLevel.STANDARD)
@requires_permission('data_acquisition')
def acquire_data(self, duration_hours):
    session = SessionManager().create_session()
    
    try:
        while session.running:
            data = self._collect_data_point()
            self.send_secure_message(data, session)
            time.sleep(collection_interval)
    finally:
        session.close()

"""
