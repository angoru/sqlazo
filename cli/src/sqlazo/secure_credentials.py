"""Secure credential storage with encryption."""

import os
import json
import getpass
import base64
import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class SecureCredentialManager:
    """Manages encrypted storage of database credentials."""
    
    def __init__(self, storage_dir: Optional[Path] = None):
        """
        Initialize the secure credential manager.
        
        Args:
            storage_dir: Directory to store encrypted credentials. 
                        Defaults to ~/.config/sqlazo/credentials
        """
        self.storage_dir = storage_dir or Path.home() / ".config" / "sqlazo" / "credentials"
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.credentials_file = self.storage_dir / "encrypted_credentials.json"
        
    def _derive_key(self, password: str, salt: bytes) -> bytes:
        """
        Derive encryption key from password using PBKDF2.

        Args:
            password: Master password for encryption/decryption
            salt: Salt for key derivation

        Returns:
            Encryption key as bytes
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key
    
    def _get_or_create_salt(self) -> bytes:
        """Get existing salt or create a new one."""
        salt_file = self.storage_dir / "salt.bin"
        
        if salt_file.exists():
            with open(salt_file, 'rb') as f:
                return f.read()
        else:
            salt = os.urandom(16)  # 16 bytes salt
            with open(salt_file, 'wb') as f:
                f.write(salt)
            return salt
    
    def store_credentials(self, profile_name: str, credentials: Dict[str, Any], 
                         master_password: Optional[str] = None) -> bool:
        """
        Store encrypted credentials for a profile.
        
        Args:
            profile_name: Name of the credential profile
            credentials: Dictionary containing credential information
            master_password: Master password. If None, will prompt user.
            
        Returns:
            True if successful, False otherwise
        """
        import base64
        
        if master_password is None:
            master_password = getpass.getpass("Enter master password for encryption: ")
        
        # Get or create salt
        salt = self._get_or_create_salt()
        
        # Derive key from password
        key = self._derive_key(master_password, salt)
        cipher_suite = Fernet(key)
        
        # Prepare credentials data
        credentials_data = {
            "profile": profile_name,
            "credentials": credentials,
            "created_at": str(datetime.datetime.now().isoformat())
        }
        
        # Serialize and encrypt
        serialized_data = json.dumps(credentials_data).encode()
        encrypted_data = cipher_suite.encrypt(serialized_data)
        
        # Load existing credentials
        existing_credentials = {}
        if self.credentials_file.exists():
            with open(self.credentials_file, 'r') as f:
                try:
                    existing_credentials = json.load(f)
                except json.JSONDecodeError:
                    existing_credentials = {}
        
        # Add new credentials
        existing_credentials[profile_name] = {
            "data": base64.b64encode(encrypted_data).decode(),
            "salt": base64.b64encode(salt).decode(),
            "created_at": str(datetime.datetime.now().isoformat())
        }
        
        # Save to file
        with open(self.credentials_file, 'w') as f:
            json.dump(existing_credentials, f, indent=2)
        
        return True
    
    def retrieve_credentials(self, profile_name: str, 
                           master_password: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieve and decrypt credentials for a profile.
        
        Args:
            profile_name: Name of the credential profile
            master_password: Master password. If None, will prompt user.
            
        Returns:
            Decrypted credentials dictionary or None if not found/failed
        """
        import base64
        import datetime
        
        if master_password is None:
            master_password = getpass.getpass("Enter master password for decryption: ")
        
        # Load stored credentials
        if not self.credentials_file.exists():
            return None
        
        with open(self.credentials_file, 'r') as f:
            try:
                stored_credentials = json.load(f)
            except json.JSONDecodeError:
                return None
        
        if profile_name not in stored_credentials:
            return None
        
        profile_data = stored_credentials[profile_name]
        
        try:
            # Decode encrypted data and salt
            encrypted_data = base64.b64decode(profile_data["data"].encode())
            salt = base64.b64decode(profile_data["salt"].encode())
            
            # Derive key from password
            key = self._derive_key(master_password, salt)
            cipher_suite = Fernet(key)
            
            # Decrypt and deserialize
            decrypted_data = cipher_suite.decrypt(encrypted_data)
            credentials_data = json.loads(decrypted_data.decode())
            
            return credentials_data["credentials"]
        except Exception:
            # Wrong password or corrupted data
            return None
    
    def list_profiles(self) -> list:
        """List all stored credential profiles."""
        if not self.credentials_file.exists():
            return []
        
        with open(self.credentials_file, 'r') as f:
            try:
                stored_credentials = json.load(f)
                return list(stored_credentials.keys())
            except json.JSONDecodeError:
                return []
    
    def delete_profile(self, profile_name: str) -> bool:
        """
        Delete a credential profile.
        
        Args:
            profile_name: Name of the profile to delete
            
        Returns:
            True if successful, False otherwise
        """
        if not self.credentials_file.exists():
            return False
        
        with open(self.credentials_file, 'r') as f:
            try:
                stored_credentials = json.load(f)
            except json.JSONDecodeError:
                return False
        
        if profile_name not in stored_credentials:
            return False
        
        del stored_credentials[profile_name]
        
        with open(self.credentials_file, 'w') as f:
            json.dump(stored_credentials, f, indent=2)
        
        return True