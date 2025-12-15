"""
Enhanced configuration manager with environment support and validation
"""
import os
import yaml
import json
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
import copy
import hashlib
from datetime import datetime

logger = logging.getLogger(__name__)


class ConfigSource(Enum):
    """Configuration source enumeration"""
    FILE = "file"
    ENVIRONMENT = "environment"
    DATABASE = "database"
    DEFAULT = "default"


class ConfigValidationError(Exception):
    """Exception raised when configuration validation fails"""
    pass


@dataclass
class ConfigSection:
    """Configuration section with metadata"""
    name: str
    data: Dict[str, Any]
    source: ConfigSource = ConfigSource.FILE
    timestamp: datetime = field(default_factory=datetime.now)
    version: str = "1.0"
    description: str = ""
    required: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


class ConfigManager:
    """
    Enhanced configuration manager with environment support, validation,
    and hot reload capabilities.
    """
    
    def __init__(self, config_dir: str = "config", env: str = None):
        self.config_dir = Path(config_dir)
        self.env = env or os.getenv("APP_ENV", "development")
        self.configs: Dict[str, ConfigSection] = {}
        self.config_hash: Dict[str, str] = {}
        self.watchers: List[callable] = []
        
        # Create config directory if it doesn't exist
        self.config_dir.mkdir(exist_ok=True)
        
        # Default configuration structure
        self.default_structure = {
            "app": {
                "name": "livestock_outbreak_detection",
                "version": "1.0.0",
                "description": "Livestock Outbreak Detection System",
                "debug": False,
                "log_level": "INFO"
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "name": "livestock_db",
                "user": "",
                "password": "",
                "pool_size": 10,
                "timeout": 30
            },
            "api": {
                "host": "0.0.0.0",
                "port": 8000,
                "workers": 4,
                "timeout": 30,
                "cors_origins": ["http://localhost:3000"]
            },
            "monitoring": {
                "enabled": True,
                "interval_seconds": 60,
                "prometheus_port": 9090,
                "health_check_endpoint": "/health"
            },
            "features": {
                "data_collection": True,
                "anomaly_detection": True,
                "notifications": True,
                "dashboard": True
            }
        }
        
        logger.info(f"ConfigManager initialized for environment: {self.env}")
    
    def load_all(self, validate: bool = True) -> None:
        """Load all configuration files"""
        logger.info(f"Loading configurations from {self.config_dir}")
        
        # Load base configuration
        base_config = self._load_config_file("settings.yaml")
        if base_config:
            self.configs["base"] = ConfigSection(
                name="base",
                data=base_config,
                source=ConfigSource.FILE,
                description="Base application configuration"
            )
        
        # Load environment-specific configuration
        env_config_file = f"settings.{self.env}.yaml"
        env_config = self._load_config_file(env_config_file)
        if env_config:
            self.configs["environment"] = ConfigSection(
                name="environment",
                data=env_config,
                source=ConfigSource.FILE,
                description=f"{self.env} environment configuration"
            )
        
        # Load secrets configuration
        secrets_config = self._load_config_file("secrets.yaml", required=False)
        if secrets_config:
            self.configs["secrets"] = ConfigSection(
                name="secrets",
                data=secrets_config,
                source=ConfigSource.FILE,
                description="Secrets configuration",
                required=False
            )
        
        # Load feature flags
        features_config = self._load_config_file("features.yaml", required=False)
        if features_config:
            self.configs["features"] = ConfigSection(
                name="features",
                data=features_config,
                source=ConfigSource.FILE,
                description="Feature flags configuration"
            )
        
        # Load environment variables
        env_vars_config = self._load_environment_variables()
        if env_vars_config:
            self.configs["environment_variables"] = ConfigSection(
                name="environment_variables",
                data=env_vars_config,
                source=ConfigSource.ENVIRONMENT,
                description="Environment variables configuration"
            )
        
        # Merge all configurations
        self._merge_configurations()
        
        # Validate if requested
        if validate:
            self.validate()
        
        logger.info(f"Loaded {len(self.configs)} configuration sections")
    
    def _load_config_file(self, filename: str, required: bool = True) -> Optional[Dict]:
        """Load configuration from YAML file"""
        filepath = self.config_dir / filename
        
        if not filepath.exists():
            if required:
                logger.warning(f"Configuration file not found: {filepath}")
            return None
        
        try:
            with open(filepath, 'r') as f:
                config = yaml.safe_load(f)
            
            # Calculate hash for change detection
            config_hash = self._calculate_file_hash(filepath)
            self.config_hash[filename] = config_hash
            
            logger.debug(f"Loaded configuration from {filename}")
            return config
            
        except Exception as e:
            if required:
                raise ConfigValidationError(f"Failed to load config file {filename}: {str(e)}")
            logger.warning(f"Failed to load optional config file {filename}: {str(e)}")
            return None
    
    def _load_environment_variables(self) -> Dict:
        """Load configuration from environment variables"""
        env_config = {}
        
        # Map environment variables to configuration structure
        env_mappings = {
            "APP_NAME": ("app", "name"),
            "APP_VERSION": ("app", "version"),
            "APP_DEBUG": ("app", "debug"),
            "LOG_LEVEL": ("app", "log_level"),
            "DB_HOST": ("database", "host"),
            "DB_PORT": ("database", "port"),
            "DB_NAME": ("database", "name"),
            "DB_USER": ("database", "user"),
            "DB_PASSWORD": ("database", "password"),
            "API_HOST": ("api", "host"),
            "API_PORT": ("api", "port"),
            "MONITORING_ENABLED": ("monitoring", "enabled")
        }
        
        for env_var, config_path in env_mappings.items():
            if env_var in os.environ:
                value = os.environ[env_var]
                
                # Convert string values to appropriate types
                if env_var.endswith("_PORT") or env_var.endswith("_TIMEOUT"):
                    try:
                        value = int(value)
                    except ValueError:
                        logger.warning(f"Invalid integer value for {env_var}: {value}")
                        continue
                elif env_var.endswith("_DEBUG") or env_var.endswith("_ENABLED"):
                    value = value.lower() in ("true", "1", "yes", "y")
                
                # Set value in configuration
                section, key = config_path
                if section not in env_config:
                    env_config[section] = {}
                env_config[section][key] = value
        
        return env_config
    
    def _calculate_file_hash(self, filepath: Path) -> str:
        """Calculate MD5 hash of a file"""
        try:
            with open(filepath, 'rb') as f:
                file_hash = hashlib.md5()
                chunk = f.read(8192)
                while chunk:
                    file_hash.update(chunk)
                    chunk = f.read(8192)
            return file_hash.hexdigest()
        except Exception as e:
            logger.warning(f"Failed to calculate hash for {filepath}: {str(e)}")
            return ""
    
    def _merge_configurations(self) -> None:
        """Merge all configuration sections into a single config"""
        merged = copy.deepcopy(self.default_structure)
        
        # Merge in order of precedence (lowest to highest)
        merge_order = ["base", "environment", "features", "secrets", "environment_variables"]
        
        for section_name in merge_order:
            if section_name in self.configs:
                self._deep_merge(merged, self.configs[section_name].data)
        
        self.config = merged
        logger.debug("Merged all configurations")
    
    def _deep_merge(self, base: Dict, update: Dict) -> None:
        """Deep merge two dictionaries"""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation
        
        Examples:
            config.get("app.name")
            config.get("database.host", "localhost")
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any, section: str = "runtime") -> None:
        """
        Set configuration value at runtime
        
        Args:
            key: Dot notation key (e.g., "app.debug")
            value: Value to set
            section: Configuration section to store in
        """
        if section not in self.configs:
            self.configs[section] = ConfigSection(
                name=section,
                data={},
                source=ConfigSource.DEFAULT,
                description="Runtime configuration"
            )
        
        # Set in section data
        keys = key.split('.')
        data = self.configs[section].data
        
        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]
        
        data[keys[-1]] = value
        
        # Update merged config
        self._merge_configurations()
        
        # Notify watchers
        self._notify_watchers(key, value)
        
        logger.debug(f"Set configuration: {key} = {value}")
    
    def get_section(self, section_name: str) -> Optional[ConfigSection]:
        """Get a configuration section by name"""
        return self.configs.get(section_name)
    
    def list_sections(self) -> List[str]:
        """List all configuration section names"""
        return list(self.configs.keys())
    
    def validate(self) -> bool:
        """Validate the configuration"""
        logger.info("Validating configuration...")
        
        errors = []
        
        # Check required sections
        for section_name, section in self.configs.items():
            if section.required and not section.data:
                errors.append(f"Required section '{section_name}' is empty")
        
        # Validate database configuration
        db_config = self.get("database")
        if db_config:
            if not db_config.get("host"):
                errors.append("Database host is required")
            if not db_config.get("name"):
                errors.append("Database name is required")
        
        # Validate API configuration
        api_config = self.get("api")
        if api_config:
            port = api_config.get("port")
            if port and (port < 1 or port > 65535):
                errors.append(f"Invalid API port: {port}")
        
        if errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            logger.error(error_msg)
            raise ConfigValidationError(error_msg)
        
        logger.info("Configuration validation passed")
        return True
    
    def watch(self, key: str, callback: callable) -> None:
        """Watch for configuration changes"""
        self.watchers.append((key, callback))
        logger.debug(f"Added watcher for key: {key}")
    
    def _notify_watchers(self, key: str, value: Any) -> None:
        """Notify watchers of configuration changes"""
        for watch_key, callback in self.watchers:
            if watch_key == key or watch_key.endswith('*'):
                try:
                    callback(key, value)
                except Exception as e:
                    logger.error(f"Watcher callback failed for {key}: {str(e)}")
    
    def check_for_updates(self) -> bool:
        """Check for configuration file updates"""
        updated = False
        
        for filename in self.config_hash.keys():
            filepath = self.config_dir / filename
            if filepath.exists():
                new_hash = self._calculate_file_hash(filepath)
                old_hash = self.config_hash.get(filename)
                
                if new_hash != old_hash:
                    logger.info(f"Configuration file changed: {filename}")
                    updated = True
                    self.config_hash[filename] = new_hash
        
        if updated:
            self.load_all(validate=False)
            logger.info("Configuration reloaded due to file changes")
        
        return updated
    
    def save_to_file(self, filepath: Union[str, Path], format: str = "yaml") -> None:
        """Save current configuration to file"""
        filepath = Path(filepath)
        
        try:
            if format.lower() == "json":
                with open(filepath, 'w') as f:
                    json.dump(self.config, f, indent=2, default=str)
            else:  # yaml
                with open(filepath, 'w') as f:
                    yaml.dump(self.config, f, default_flow_style=False)
            
            logger.info(f"Configuration saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save configuration: {str(e)}")
    
    def export(self, format: str = "dict") -> Union[Dict, str]:
        """Export configuration in specified format"""
        if format == "dict":
            return copy.deepcopy(self.config)
        elif format == "json":
            return json.dumps(self.config, indent=2, default=str)
        elif format == "yaml":
            return yaml.dump(self.config, default_flow_style=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def create_template(self, filepath: Union[str, Path], 
                       include_secrets: bool = False) -> None:
        """Create a configuration template file"""
        template = copy.deepcopy(self.default_structure)
        
        if include_secrets:
            template["secrets"] = {
                "api_key": "YOUR_API_KEY_HERE",
                "database_password": "YOUR_DB_PASSWORD",
                "email_password": "YOUR_EMAIL_PASSWORD"
            }
        
        filepath = Path(filepath)
        with open(filepath, 'w') as f:
            yaml.dump(template, f, default_flow_style=False)
        
        logger.info(f"Configuration template created at {filepath}")


# Global config manager instance
_config_manager: Optional[ConfigManager] = None


def get_config_manager(config_dir: str = "config", env: str = None) -> ConfigManager:
    """Get or create the global configuration manager"""
    global _config_manager
    
    if _config_manager is None:
        _config_manager = ConfigManager(config_dir, env)
        _config_manager.load_all()
    
    return _config_manager


def reset_config_manager() -> None:
    """Reset the global configuration manager (for testing)"""
    global _config_manager
    _config_manager = None