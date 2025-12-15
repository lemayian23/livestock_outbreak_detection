"""
Tests for configuration management system
"""
import pytest
import tempfile
import os
import yaml
import json
from pathlib import Path
from unittest.mock import patch, mock_open
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config_manager.manager import (
    ConfigManager, ConfigSection, ConfigSource, ConfigValidationError,
    get_config_manager, reset_config_manager
)
from config_manager.secrets import (
    SecretsManager, Secret, SecretSource, get_secrets_manager, reset_secrets_manager
)
from config_manager.environments import (
    EnvironmentManager, Environment, EnvironmentInfo, get_environment_manager
)


class TestConfigManager:
    def setup_method(self):
        """Create a temporary config directory for testing"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = Path(self.temp_dir)
        
        # Create basic config file
        base_config = {
            "app": {
                "name": "test_app",
                "version": "1.0.0"
            },
            "database": {
                "host": "localhost",
                "port": 5432
            }
        }
        
        (self.config_dir / "settings.yaml").write_text(yaml.dump(base_config))
        
        # Create environment config
        env_config = {
            "app": {
                "debug": True
            }
        }
        
        (self.config_dir / "settings.development.yaml").write_text(yaml.dump(env_config))
    
    def teardown_method(self):
        """Clean up temporary directory"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_initialization(self):
        """Test config manager initialization"""
        manager = ConfigManager(str(self.config_dir), "development")
        manager.load_all()
        
        assert manager.env == "development"
        assert len(manager.configs) > 0
        assert "base" in manager.configs
        assert "environment" in manager.configs
    
    def test_load_config(self):
        """Test loading configuration"""
        manager = ConfigManager(str(self.config_dir))
        manager.load_all()
        
        assert manager.get("app.name") == "test_app"
        assert manager.get("database.host") == "localhost"
        assert manager.get("database.port") == 5432
    
    def test_env_config_override(self):
        """Test environment configuration override"""
        manager = ConfigManager(str(self.config_dir), "development")
        manager.load_all()
        
        # Environment config should override base
        assert manager.get("app.debug") == True
    
    def test_get_with_default(self):
        """Test get with default value"""
        manager = ConfigManager(str(self.config_dir))
        manager.load_all()
        
        value = manager.get("nonexistent.key", "default_value")
        assert value == "default_value"
    
    def test_set_value(self):
        """Test setting configuration value"""
        manager = ConfigManager(str(self.config_dir))
        manager.load_all()
        
        manager.set("app.custom_setting", "custom_value")
        
        assert manager.get("app.custom_setting") == "custom_value"
        assert "runtime" in manager.configs
    
    def test_validation(self):
        """Test configuration validation"""
        manager = ConfigManager(str(self.config_dir))
        manager.load_all()
        
        # Should not raise for valid config
        manager.validate()
    
    def test_validation_failure(self):
        """Test configuration validation failure"""
        manager = ConfigManager(str(self.config_dir))
        
        # Create invalid config (empty database)
        manager.configs["base"] = ConfigSection(
            name="base",
            data={"database": {}},
            source=ConfigSource.FILE
        )
        manager._merge_configurations()
        
        with pytest.raises(ConfigValidationError):
            manager.validate()
    
    def test_export(self):
        """Test configuration export"""
        manager = ConfigManager(str(self.config_dir))
        manager.load_all()
        
        # Export as dict
        config_dict = manager.export("dict")
        assert "app" in config_dict
        assert "database" in config_dict
        
        # Export as JSON
        config_json = manager.export("json")
        assert isinstance(config_json, str)
        json.loads(config_json)  # Should be valid JSON
        
        # Export as YAML
        config_yaml = manager.export("yaml")
        assert isinstance(config_yaml, str)
        yaml.safe_load(config_yaml)  # Should be valid YAML
    
    def test_create_template(self):
        """Test template creation"""
        manager = ConfigManager(str(self.config_dir))
        
        template_file = self.config_dir / "template.yaml"
        manager.create_template(template_file)
        
        assert template_file.exists()
        template_data = yaml.safe_load(template_file.read_text())
        assert "app" in template_data
        assert "database" in template_data


class TestSecretsManager:
    def test_initialization(self):
        """Test secrets manager initialization"""
        with patch.dict(os.environ, {"SECRET_TEST_KEY": "test_value"}):
            manager = SecretsManager({"env_prefix": "SECRET_"})
            
            assert "test_key" in manager.secrets
            assert manager.get("test_key") == "test_value"
    
    def test_set_get_secret(self):
        """Test setting and getting secrets"""
        manager = SecretsManager()
        
        manager.set("test_secret", "test_value", encrypt=False)
        
        assert manager.get("test_secret") == "test_value"
        secret_obj = manager.get_secret("test_secret")
        assert secret_obj.name == "test_secret"
        assert secret_obj.value == "test_value"
    
    def test_list_secrets(self):
        """Test listing secrets"""
        manager = SecretsManager()
        
        manager.set("secret1", "value1", encrypt=False)
        manager.set("secret2", "value2", encrypt=False)
        
        secrets = manager.list(include_values=False)
        assert "secret1" in secrets
        assert "secret2" in secrets
        assert "source" in secrets["secret1"]
    
    def test_delete_secret(self):
        """Test deleting secrets"""
        manager = SecretsManager()
        
        manager.set("test_secret", "value", encrypt=False)
        assert manager.get("test_secret") == "value"
        
        manager.delete("test_secret")
        assert manager.get("test_secret") is None
    
    def test_mask_value(self):
        """Test value masking"""
        manager = SecretsManager()
        
        assert manager.mask_value("password123") == "pa***23"
        assert manager.mask_value("ab") == "***"
        assert manager.mask_value("") == "***"
    
    def test_validate_access(self):
        """Test secret access validation"""
        manager = SecretsManager()
        
        manager.set("required1", "value1", encrypt=False)
        manager.set("required2", "value2", encrypt=False)
        
        # Should pass with all required secrets
        assert manager.validate_access(["required1", "required2"])
        
        # Should fail with missing secrets
        assert not manager.validate_access(["required1", "required2", "missing"])


class TestEnvironmentManager:
    @patch('socket.gethostname')
    @patch.dict(os.environ, {})
    def test_detect_environment(self, mock_gethostname):
        """Test environment detection"""
        mock_gethostname.return_value = "localhost"
        
        manager = EnvironmentManager()
        
        # Default should be development
        assert manager.current_env == Environment.DEVELOPMENT
    
    @patch.dict(os.environ, {"APP_ENV": "production"})
    def test_detect_environment_from_env_var(self):
        """Test environment detection from environment variable"""
        manager = EnvironmentManager()
        
        assert manager.current_env == Environment.PRODUCTION
    
    def test_get_config(self):
        """Test getting environment configuration"""
        manager = EnvironmentManager()
        
        config = manager.get_config(Environment.DEVELOPMENT)
        assert "debug" in config
        assert config["debug"] == True
        
        prod_config = manager.get_config(Environment.PRODUCTION)
        assert prod_config["debug"] == False
    
    def test_get_feature_flags(self):
        """Test getting feature flags"""
        manager = EnvironmentManager()
        
        features = manager.get_feature_flags(Environment.DEVELOPMENT)
        assert "enable_experimental" in features
        assert features["enable_experimental"] == True
    
    def test_is_feature_enabled(self):
        """Test checking if feature is enabled"""
        manager = EnvironmentManager()
        
        assert manager.is_feature_enabled("enable_experimental", Environment.DEVELOPMENT)
        assert not manager.is_feature_enabled("enable_experimental", Environment.PRODUCTION)
    
    def test_get_environment_summary(self):
        """Test getting environment summary"""
        manager = EnvironmentManager()
        
        summary = manager.get_environment_summary()
        assert "environment" in summary
        assert "hostname" in summary
        assert "os" in summary
    
    @patch.dict(os.environ, {})
    def test_validate_environment(self):
        """Test environment validation"""
        manager = EnvironmentManager()
        
        # Development environment should pass validation
        assert manager.validate_environment()
    
    def test_switch_environment(self):
        """Test switching environments"""
        manager = EnvironmentManager()
        
        old_env = manager.current_env
        manager.switch_environment(Environment.PRODUCTION)
        
        assert manager.current_env == Environment.PRODUCTION
        assert manager.current_env != old_env


class TestGlobalInstances:
    def test_config_manager_singleton(self):
        """Test config manager singleton pattern"""
        manager1 = get_config_manager()
        manager2 = get_config_manager()
        
        assert manager1 is manager2
        
        reset_config_manager()
        manager3 = get_config_manager()
        
        assert manager1 is not manager3
    
    def test_secrets_manager_singleton(self):
        """Test secrets manager singleton pattern"""
        manager1 = get_secrets_manager()
        manager2 = get_secrets_manager()
        
        assert manager1 is manager2
        
        reset_secrets_manager()
        manager3 = get_secrets_manager()
        
        assert manager1 is not manager3
    
    def test_environment_manager_singleton(self):
        """Test environment manager singleton pattern"""
        manager1 = get_environment_manager()
        manager2 = get_environment_manager()
        
        assert manager1 is manager2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])