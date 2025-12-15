"""
Environment management for different deployment environments
"""
import os
import socket
import platform
from typing import Dict, Any, Optional
from enum import Enum
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class Environment(Enum):
    """Environment types"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    DEMO = "demo"


@dataclass
class EnvironmentInfo:
    """Environment information"""
    name: Environment
    hostname: str
    os: str
    python_version: str
    is_container: bool = False
    is_cloud: bool = False
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class EnvironmentManager:
    """
    Manages different deployment environments and their configurations
    """
    
    def __init__(self):
        self.current_env = self.detect_environment()
        self.env_info = self.collect_environment_info()
        self.env_configs: Dict[Environment, Dict] = {}
        
        # Default environment configurations
        self._initialize_default_configs()
        
        logger.info(f"Environment manager initialized for {self.current_env.value}")
    
    def detect_environment(self) -> Environment:
        """Detect current environment"""
        # Check environment variable
        env_var = os.getenv("APP_ENV", "").lower()
        
        if env_var:
            env_map = {
                "dev": Environment.DEVELOPMENT,
                "development": Environment.DEVELOPMENT,
                "test": Environment.TESTING,
                "testing": Environment.TESTING,
                "staging": Environment.STAGING,
                "prod": Environment.PRODUCTION,
                "production": Environment.PRODUCTION,
                "demo": Environment.DEMO
            }
            
            if env_var in env_map:
                return env_map[env_var]
        
        # Auto-detect based on hostname and other factors
        hostname = socket.gethostname().lower()
        
        if any(x in hostname for x in ["prod", "production", "live"]):
            return Environment.PRODUCTION
        elif any(x in hostname for x in ["staging", "stage", "preprod"]):
            return Environment.STAGING
        elif any(x in hostname for x in ["test", "testing", "qa"]):
            return Environment.TESTING
        elif any(x in hostname for x in ["dev", "development", "local"]):
            return Environment.DEVELOPMENT
        else:
            # Default to development for safety
            return Environment.DEVELOPMENT
    
    def collect_environment_info(self) -> EnvironmentInfo:
        """Collect information about the current environment"""
        hostname = socket.gethostname()
        os_name = platform.system()
        python_version = platform.python_version()
        
        # Check if running in container
        is_container = os.path.exists("/.dockerenv") or \
                      os.path.exists("/run/.containerenv") or \
                      "CONTAINER" in os.environ
        
        # Check if running in cloud
        is_cloud = self._detect_cloud_environment()
        
        metadata = {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "machine": platform.machine(),
            "container_runtime": os.getenv("CONTAINER_RUNTIME"),
            "cloud_provider": self._detect_cloud_provider() if is_cloud else None
        }
        
        return EnvironmentInfo(
            name=self.current_env,
            hostname=hostname,
            os=os_name,
            python_version=python_version,
            is_container=is_container,
            is_cloud=is_cloud,
            metadata=metadata
        )
    
    def _detect_cloud_environment(self) -> bool:
        """Detect if running in cloud environment"""
        # Check for cloud environment variables
        cloud_indicators = [
            "AWS_", "AZURE_", "GCP_", "GOOGLE_CLOUD_",
            "CLOUD_", "KUBERNETES_", "DOCKER_"
        ]
        
        for key in os.environ:
            for indicator in cloud_indicators:
                if key.startswith(indicator):
                    return True
        
        # Check for cloud metadata services
        try:
            import urllib.request
            # AWS EC2 metadata
            urllib.request.urlopen("http://169.254.169.254/latest/meta-data/", timeout=1)
            return True
        except:
            pass
        
        return False
    
    def _detect_cloud_provider(self) -> Optional[str]:
        """Detect cloud provider"""
        # Check environment variables
        if "AWS_" in str(os.environ):
            return "aws"
        elif "AZURE_" in str(os.environ):
            return "azure"
        elif "GCP_" in str(os.environ) or "GOOGLE_CLOUD_" in str(os.environ):
            return "gcp"
        
        # Check metadata services
        try:
            import urllib.request
            # AWS EC2
            urllib.request.urlopen("http://169.254.169.254/latest/meta-data/", timeout=1)
            return "aws"
        except:
            pass
        
        return None
    
    def _initialize_default_configs(self) -> None:
        """Initialize default configurations for each environment"""
        
        # Development environment
        self.env_configs[Environment.DEVELOPMENT] = {
            "debug": True,
            "log_level": "DEBUG",
            "database": {
                "host": "localhost",
                "port": 5432,
                "name": "livestock_dev"
            },
            "api": {
                "port": 8000,
                "cors_origins": ["http://localhost:3000", "http://127.0.0.1:3000"]
            },
            "features": {
                "enable_experimental": True,
                "enable_debug_endpoints": True
            }
        }
        
        # Testing environment
        self.env_configs[Environment.TESTING] = {
            "debug": True,
            "log_level": "INFO",
            "database": {
                "host": "localhost",
                "port": 5432,
                "name": "livestock_test"
            },
            "api": {
                "port": 8001,
                "cors_origins": []
            },
            "features": {
                "enable_experimental": False,
                "enable_debug_endpoints": False
            }
        }
        
        # Staging environment
        self.env_configs[Environment.STAGING] = {
            "debug": False,
            "log_level": "INFO",
            "database": {
                "host": "staging-db.example.com",
                "port": 5432,
                "name": "livestock_staging"
            },
            "api": {
                "port": 8080,
                "cors_origins": ["https://staging.example.com"]
            },
            "features": {
                "enable_experimental": True,
                "enable_debug_endpoints": False
            }
        }
        
        # Production environment
        self.env_configs[Environment.PRODUCTION] = {
            "debug": False,
            "log_level": "WARNING",
            "database": {
                "host": "production-db.example.com",
                "port": 5432,
                "name": "livestock_prod"
            },
            "api": {
                "port": 443,
                "cors_origins": ["https://production.example.com"]
            },
            "features": {
                "enable_experimental": False,
                "enable_debug_endpoints": False
            },
            "security": {
                "require_https": True,
                "rate_limit": True,
                "enable_firewall": True
            }
        }
        
        # Demo environment
        self.env_configs[Environment.DEMO] = {
            "debug": False,
            "log_level": "INFO",
            "database": {
                "host": "demo-db.example.com",
                "port": 5432,
                "name": "livestock_demo"
            },
            "api": {
                "port": 8080,
                "cors_origins": ["https://demo.example.com"]
            },
            "features": {
                "enable_experimental": False,
                "enable_debug_endpoints": True
            }
        }
    
    def get_config(self, env: Optional[Environment] = None) -> Dict:
        """Get configuration for environment"""
        if env is None:
            env = self.current_env
        
        return self.env_configs.get(env, {}).copy()
    
    def get_feature_flags(self, env: Optional[Environment] = None) -> Dict:
        """Get feature flags for environment"""
        config = self.get_config(env)
        return config.get("features", {}).copy()
    
    def is_feature_enabled(self, feature: str, env: Optional[Environment] = None) -> bool:
        """Check if a feature is enabled in environment"""
        features = self.get_feature_flags(env)
        return features.get(feature, False)
    
    def get_security_config(self, env: Optional[Environment] = None) -> Dict:
        """Get security configuration for environment"""
        config = self.get_config(env)
        return config.get("security", {}).copy()
    
    def get_database_config(self, env: Optional[Environment] = None) -> Dict:
        """Get database configuration for environment"""
        config = self.get_config(env)
        return config.get("database", {}).copy()
    
    def get_api_config(self, env: Optional[Environment] = None) -> Dict:
        """Get API configuration for environment"""
        config = self.get_config(env)
        return config.get("api", {}).copy()
    
    def get_environment_summary(self) -> Dict:
        """Get summary of current environment"""
        return {
            "environment": self.current_env.value,
            "hostname": self.env_info.hostname,
            "os": self.env_info.os,
            "python_version": self.env_info.python_version,
            "is_container": self.env_info.is_container,
            "is_cloud": self.env_info.is_cloud,
            "metadata": self.env_info.metadata
        }
    
    def validate_environment(self) -> bool:
        """Validate current environment configuration"""
        errors = []
        
        # Check environment variables based on environment
        if self.current_env == Environment.PRODUCTION:
            required_vars = ["SECRET_KEY", "DATABASE_URL"]
            for var in required_vars:
                if var not in os.environ:
                    errors.append(f"Missing required environment variable for production: {var}")
        
        # Check security settings
        if self.current_env == Environment.PRODUCTION:
            config = self.get_config()
            security = config.get("security", {})
            if not security.get("require_https", False):
                errors.append("Production environment should require HTTPS")
        
        if errors:
            logger.error(f"Environment validation failed: {errors}")
            return False
        
        logger.info("Environment validation passed")
        return True
    
    def switch_environment(self, new_env: Environment) -> bool:
        """Switch to a different environment"""
        old_env = self.current_env
        self.current_env = new_env
        
        # Update environment info
        self.env_info.name = new_env
        
        logger.info(f"Switched environment from {old_env.value} to {new_env.value}")
        return True


# Global environment manager instance
_environment_manager: Optional[EnvironmentManager] = None


def get_environment_manager() -> EnvironmentManager:
    """Get or create the global environment manager"""
    global _environment_manager
    
    if _environment_manager is None:
        _environment_manager = EnvironmentManager()
    
    return _environment_manager