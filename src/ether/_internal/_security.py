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