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
            "features