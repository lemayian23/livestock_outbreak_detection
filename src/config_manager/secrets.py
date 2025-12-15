"""
Secure secrets management with encryption and environment support
"""
import os
import base64
import json
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
import hashlib
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)


class SecretSource(Enum):
    """Secret source enumeration"""
    ENVIRONMENT = "environment"
    FILE = "file"
    VAULT = "vault"
    AWS_SECRETS = "aws_secrets"
    DEFAULT = "default"


@dataclass
class Secret:
    """Secret with metadata"""
    name: str
    value: str
    source: SecretSource
    encrypted: bool = False
    description: str = ""
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class SecretsManager:
    """
    Secure secrets manager with encryption and multiple backends
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.secrets: Dict[str, Secret] = {}
        self.fernet: Optional[Fernet] = None
        self.secrets_file: Optional[Path] = None
        
        # Initialize encryption
        self._initialize_encryption()
        
        # Load secrets
        self._load_secrets()
        
        logger.info("SecretsManager initialized")
    
    def _initialize_encryption(self) -> None:
        """Initialize encryption key"""
        # Get encryption key from environment or config
        encryption_key = self.config.get("encryption_key") or \
                        os.getenv("SECRETS_ENCRYPTION_KEY")
        
        if encryption_key:
            try:
                # Derive Fernet key from password
                password = encryption_key.encode()
                salt = b'lvstk_secrets_salt'  # Should be random in production
                kdf = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=32,
                    salt=salt,
                    iterations=100000,
                )
                key = base64.urlsafe_b64encode(kdf.derive(password))
                self.fernet = Fernet(key)
                logger.debug("Encryption initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize encryption: {str(e)}")
                self.fernet = None
    
    def _load_secrets(self) -> None:
        """Load secrets from all sources"""
        # Load from environment variables first (highest priority)
        self._load_from_environment()
        
        # Load from secrets file if configured
        secrets_file = self.config.get("secrets_file") or \
                      os.getenv("SECRETS_FILE")
        
        if secrets_file and Path(secrets_file).exists():
            self.secrets_file = Path(secrets_file)
            self._load_from_file(self.secrets_file)
        
        # Load from default secrets file
        default_file = Path("config/secrets.yaml")
        if default_file.exists() and not secrets_file:
            self.secrets_file = default_file
            self._load_from_file(default_file)
        
        logger.info(f"Loaded {len(self.secrets)} secrets")
    
    def _load_from_environment(self) -> None:
        """Load secrets from environment variables"""
        env_prefix = self.config.get("env_prefix", "SECRET_")
        
        for key, value in os.environ.items():
            if key.startswith(env_prefix):
                secret_name = key[len(env_prefix):].lower()
                secret = Secret(
                    name=secret_name,
                    value=value,
                    source=SecretSource.ENVIRONMENT,
                    encrypted=False
                )
                self.secrets[secret_name] = secret
                logger.debug(f"Loaded secret from environment: {secret_name}")
    
    def _load_from_file(self, filepath: Path) -> None:
        """Load secrets from YAML file"""
        import yaml
        
        try:
            with open(filepath, 'r') as f:
                secrets_data = yaml.safe_load(f)
            
            if not secrets_data:
                return
            
            for secret_name, secret_value in secrets_data.items():
                # Check if value is encrypted
                encrypted = False
                value = secret_value
                
                if isinstance(secret_value, dict):
                    # Handle structured secret
                    if "encrypted" in secret_value and secret_value["encrypted"]:
                        encrypted = True
                        value = secret_value["value"]
                    elif "value" in secret_value:
                        value = secret_value["value"]
                
                # Decrypt if needed
                if encrypted and self.fernet:
                    try:
                        value = self.fernet.decrypt(value.encode()).decode()
                    except Exception as e:
                        logger.error(f"Failed to decrypt secret {secret_name}: {str(e)}")
                        continue
                
                secret = Secret(
                    name=secret_name,
                    value=str(value),
                    source=SecretSource.FILE,
                    encrypted=encrypted,
                    description=""
                )
                self.secrets[secret_name] = secret
                logger.debug(f"Loaded secret from file: {secret_name}")
                
        except Exception as e:
            logger.error(f"Failed to load secrets from {filepath}: {str(e)}")
    
    def get(self, name: str, default: Any = None) -> Optional[str]:
        """Get a secret value by name"""
        if name in self.secrets:
            return self.secrets[name].value
        return default
    
    def get_secret(self, name: str) -> Optional[Secret]:
        """Get secret object by name"""
        return self.secrets.get(name)
    
    def set(self, name: str, value: str, encrypt: bool = True, 
           description: str = "") -> None:
        """Set a secret value"""
        encrypted = False
        secret_value = value
        
        # Encrypt if requested and encryption is available
        if encrypt and self.fernet:
            try:
                secret_value = self.fernet.encrypt(value.encode()).decode()
                encrypted = True
            except Exception as e:
                logger.error(f"Failed to encrypt secret {name}: {str(e)}")
        
        secret = Secret(
            name=name,
            value=secret_value,
            source=SecretSource.DEFAULT,
            encrypted=encrypted,
            description=description
        )
        self.secrets[name] = secret
        logger.debug(f"Set secret: {name} (encrypted: {encrypted})")
    
    def delete(self, name: str) -> bool:
        """Delete a secret"""
        if name in self.secrets:
            del self.secrets[name]
            logger.debug(f"Deleted secret: {name}")
            return True
        return False
    
    def list(self, include_values: bool = False) -> Dict[str, Any]:
        """List all secrets"""
        result = {}
        
        for name, secret in self.secrets.items():
            if include_values:
                result[name] = {
                    "value": secret.value,
                    "source": secret.source.value,
                    "encrypted": secret.encrypted,
                    "description": secret.description
                }
            else:
                result[name] = {
                    "source": secret.source.value,
                    "encrypted": secret.encrypted,
                    "description": secret.description
                }
        
        return result
    
    def save_to_file(self, filepath: Optional[Path] = None, encrypt_all: bool = True) -> None:
        """Save secrets to file"""
        import yaml
        
        if not filepath:
            if not self.secrets_file:
                self.secrets_file = Path("config/secrets.yaml")
            filepath = self.secrets_file
        
        # Prepare secrets for saving
        secrets_to_save = {}
        
        for name, secret in self.secrets.items():
            # Skip environment secrets (they shouldn't be saved to file)
            if secret.source == SecretSource.ENVIRONMENT:
                continue
            
            # Encrypt if requested and not already encrypted
            if encrypt_all and not secret.encrypted and self.fernet:
                try:
                    encrypted_value = self.fernet.encrypt(secret.value.encode()).decode()
                    secrets_to_save[name] = {
                        "value": encrypted_value,
                        "encrypted": True
                    }
                except Exception as e:
                    logger.error(f"Failed to encrypt secret {name} for saving: {str(e)}")
                    secrets_to_save[name] = secret.value
            else:
                if secret.encrypted:
                    secrets_to_save[name] = {
                        "value": secret.value,
                        "encrypted": True
                    }
                else:
                    secrets_to_save[name] = secret.value
        
        # Save to file
        try:
            filepath.parent.mkdir(exist_ok=True)
            
            with open(filepath, 'w') as f:
                yaml.dump(secrets_to_save, f, default_flow_style=False)
            
            # Set restrictive permissions
            filepath.chmod(0o600)
            
            logger.info(f"Secrets saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save secrets: {str(e)}")
    
    def rotate_key(self, new_key: str) -> None:
        """Rotate encryption key"""
        if not self.fernet:
            logger.error("Cannot rotate key: encryption not initialized")
            return
        
        try:
            # Create new Fernet instance
            password = new_key.encode()
            salt = b'lvstk_secrets_salt_new'
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
            )
            new_fernet_key = base64.urlsafe_b64encode(kdf.derive(password))
            new_fernet = Fernet(new_fernet_key)
            
            # Re-encrypt all secrets
            for name, secret in self.secrets.items():
                if secret.encrypted:
                    # Decrypt with old key
                    try:
                        decrypted = self.fernet.decrypt(secret.value.encode()).decode()
                        # Re-encrypt with new key
                        reencrypted = new_fernet.encrypt(decrypted.encode()).decode()
                        secret.value = reencrypted
                    except Exception as e:
                        logger.error(f"Failed to rotate secret {name}: {str(e)}")
            
            # Update Fernet instance
            self.fernet = new_fernet
            logger.info("Encryption key rotated successfully")
            
        except Exception as e:
            logger.error(f"Failed to rotate encryption key: {str(e)}")
    
    def mask_value(self, value: str) -> str:
        """Mask a secret value for logging"""
        if len(value) <= 4:
            return "***"
        return value[:2] + "***" + value[-2:]
    
    def validate_access(self, required_secrets: List[str]) -> bool:
        """Validate access to required secrets"""
        missing = []
        
        for secret_name in required_secrets:
            if secret_name not in self.secrets:
                missing.append(secret_name)
        
        if missing:
            logger.error(f"Missing required secrets: {missing}")
            return False
        
        return True
    
    def create_template(self, filepath: Path) -> None:
        """Create a secrets template file"""
        template = {
            "database_password": {
                "value": "YOUR_DATABASE_PASSWORD",
                "encrypted": False,
                "description": "Database connection password"
            },
            "api_key": {
                "value": "YOUR_API_KEY",
                "encrypted": True,
                "description": "External API key"
            },
            "email_password": {
                "value": "YOUR_EMAIL_PASSWORD",
                "encrypted": True,
                "description": "Email service password"
            },
            "jwt_secret": {
                "value": "YOUR_JWT_SECRET_KEY",
                "encrypted": True,
                "description": "JWT token signing secret"
            }
        }
        
        try:
            import yaml
            with open(filepath, 'w') as f:
                yaml.dump(template, f, default_flow_style=False)
            
            # Set restrictive permissions
            filepath.chmod(0o600)
            
            logger.info(f"Secrets template created at {filepath}")
        except Exception as e:
            logger.error(f"Failed to create secrets template: {str(e)}")


# Global secrets manager instance
_secrets_manager: Optional[SecretsManager] = None


def get_secrets_manager(config: Optional[Dict] = None) -> SecretsManager:
    """Get or create the global secrets manager"""
    global _secrets_manager
    
    if _secrets_manager is None:
        _secrets_manager = SecretsManager(config)
    
    return _secrets_manager


def reset_secrets_manager() -> None:
    """Reset the global secrets manager (for testing)"""
    global _secrets_manager
    _secrets_manager = None