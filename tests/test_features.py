"""
Tests for feature toggle system
"""
import pytest
from unittest.mock import Mock, patch
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.feature_manager import (
    FeatureManager, 
    FeatureToggle, 
    FeatureState,
    FeatureDisabledError,
    get_feature_manager,
    reset_feature_manager
)


class TestFeatureToggle:
    def test_feature_toggle_creation(self):
        """Test creating a feature toggle"""
        feature = FeatureToggle(
            name="test_feature",
            description="A test feature",
            default_state=False,
            category="test"
        )
        
        assert feature.name == "test_feature"
        assert feature.description == "A test feature"
        assert feature.default_state == False
        assert feature.current_state == True  # Default is True on creation
        assert feature.category == "test"
        assert feature.state == FeatureState.ENABLED
    
    def test_feature_toggle_with_metadata(self):
        """Test feature toggle with metadata"""
        feature = FeatureToggle(
            name="test",
            description="Test",
            metadata={"version": "1.0", "author": "test"}
        )
        
        assert feature.metadata["version"] == "1.0"
        assert feature.metadata["author"] == "test"


class TestFeatureManager:
    def setup_method(self):
        """Reset feature manager before each test"""
        reset_feature_manager()
    
    def test_initialization(self):
        """Test feature manager initialization"""
        config = {
            'features': {
                'data_collection': False,
                'anomaly_detection': True
            }
        }
        
        manager = FeatureManager(config)
        
        # Check that features are registered
        assert 'data_collection' in manager._features
        assert 'anomaly_detection' in manager._features
        
        # Check config override
        assert manager.is_enabled('data_collection') == False
        assert manager.is_enabled('anomaly_detection') == True
    
    def test_register_feature(self):
        """Test registering a new feature"""
        manager = FeatureManager()
        
        new_feature = FeatureToggle(
            name="custom_feature",
            description="Custom feature",
            default_state=False
        )
        
        manager.register_feature(new_feature)
        assert 'custom_feature' in manager._features
    
    def test_is_enabled_basic(self):
        """Test basic feature enablement check"""
        manager = FeatureManager()
        
        # Default should be enabled
        assert manager.is_enabled('data_collection') == True
        
        # Disable and check
        manager.disable_feature('data_collection')
        assert manager.is_enabled('data_collection') == False
        
        # Re-enable
        manager.enable_feature('data_collection')
        assert manager.is_enabled('data_collection') == True
    
    def test_is_enabled_with_dependencies(self):
        """Test feature with dependencies"""
        manager = FeatureManager()
        
        # Create dependent feature
        dependent = FeatureToggle(
            name="dependent_feature",
            description="Depends on other",
            dependencies=["data_collection"]
        )
        manager.register_feature(dependent)
        
        # Both enabled by default
        assert manager.is_enabled('dependent_feature') == True
        
        # Disable dependency
        manager.disable_feature('data_collection')
        assert manager.is_enabled('dependent_feature') == False
    
    def test_get_feature_status(self):
        """Test getting feature status"""
        manager = FeatureManager()
        
        status = manager.get_feature_status()
        
        assert 'data_collection' in status
        assert status['data_collection']['enabled'] == True
        assert 'description' in status['data_collection']
        assert 'category' in status['data_collection']
    
    def test_get_enabled_features(self):
        """Test getting only enabled features"""
        manager = FeatureManager()
        
        # Disable a feature
        manager.disable_feature('data_quality')
        
        enabled = manager.get_enabled_features()
        
        assert 'data_quality' not in enabled
        assert 'data_collection' in enabled  # Still enabled
    
    def test_get_features_by_category(self):
        """Test getting features by category"""
        manager = FeatureManager()
        
        detection_features = manager.get_features_by_category('detection')
        
        assert 'anomaly_detection' in detection_features
        assert 'ensemble_detection' in detection_features
    
    def test_require_feature_decorator(self):
        """Test the require_feature decorator"""
        manager = FeatureManager()
        
        @manager.require_feature('anomaly_detection')
        def some_function():
            return "success"
        
        # Should work when feature is enabled
        assert some_function() == "success"
        
        # Should fail when feature is disabled
        manager.disable_feature('anomaly_detection')
        with pytest.raises(FeatureDisabledError):
            some_function()
    
    def test_feature_context(self):
        """Test the feature context manager"""
        manager = FeatureManager()
        
        # Initially enabled
        assert manager.is_enabled('anomaly_detection') == True
        
        with manager.feature_context('anomaly_detection', False):
            # Temporarily disabled
            assert manager.is_enabled('anomaly_detection') == False
        
        # Back to original state
        assert manager.is_enabled('anomaly_detection') == True
    
    def test_experimental_features(self):
        """Test experimental feature handling"""
        config = {
            'features': {
                'experimental': {
                    'advanced_seasonal': True,
                    'real_time_streaming': False
                }
            }
        }
        
        manager = FeatureManager(config)
        
        # Check experimental features from config
        assert manager.is_enabled('advanced_seasonal') == True
        assert manager.is_enabled('real_time_streaming') == False
    
    def test_unknown_feature(self):
        """Test behavior with unknown feature"""
        manager = FeatureManager()
        
        with pytest.raises(KeyError):
            manager.is_enabled('non_existent_feature')
        
        # These should just warn
        manager.enable_feature('non_existent')
        manager.disable_feature('non_existent')


class TestGlobalFeatureManager:
    def setup_method(self):
        reset_feature_manager()
    
    def test_singleton_pattern(self):
        """Test that get_feature_manager returns the same instance"""
        manager1 = get_feature_manager()
        manager2 = get_feature_manager()
        
        assert manager1 is manager2
    
    def test_config_passed_to_singleton(self):
        """Test config is passed to singleton"""
        config = {'features': {'data_collection': False}}
        
        manager = get_feature_manager(config)
        assert manager.is_enabled('data_collection') == False
        
        # Second call with different config should still use first config
        manager2 = get_feature_manager({'features': {'data_collection': True}})
        assert manager2.is_enabled('data_collection') == False  # Still false!
    
    def test_reset_global_manager(self):
        """Test resetting global manager"""
        manager1 = get_feature_manager()
        reset_feature_manager()
        manager2 = get_feature_manager()
        
        assert manager1 is not manager2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])