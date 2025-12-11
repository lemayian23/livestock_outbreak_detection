"""
Feature Toggle Manager
Enables/disables features via configuration
"""
import logging
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class FeatureState(Enum):
    """Feature state enumeration"""
    ENABLED = "enabled"
    DISABLED = "disabled"
    EXPERIMENTAL = "experimental"
    DEPRECATED = "deprecated"


@dataclass
class FeatureToggle:
    """Individual feature toggle"""
    name: str
    description: str
    default_state: bool = True
    current_state: bool = True
    category: str = "core"
    state: FeatureState = FeatureState.ENABLED
    dependencies: list[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class FeatureManager:
    """
    Manages feature toggles for the application
    Allows runtime feature enabling/disabling
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self._features: Dict[str, FeatureToggle] = {}
        self._config = config or {}
        self._initialize_features()
        
    def _initialize_features(self) -> None:
        """Initialize feature toggles from config"""
        # Core features
        core_features = [
            FeatureToggle(
                name="data_collection",
                description="Data collection from sources",
                category="core"
            ),
            FeatureToggle(
                name="data_quality",
                description="Data quality checks",
                category="core"
            ),
            FeatureToggle(
                name="preprocessing",
                description="Data preprocessing",
                category="core"
            ),
            FeatureToggle(
                name="anomaly_detection",
                description="Anomaly detection algorithms",
                category="detection"
            ),
            FeatureToggle(
                name="ensemble_detection",
                description="Ensemble anomaly detection",
                category="detection"
            ),
            FeatureToggle(
                name="notifications",
                description="Notification system",
                category="alerting"
            ),
            FeatureToggle(
                name="email_alerts",
                description="Email alert notifications",
                category="alerting",
                dependencies=["notifications"]
            ),
            FeatureToggle(
                name="export_reports",
                description="Export reports to files",
                category="export"
            ),
            FeatureToggle(
                name="dashboard",
                description="Web dashboard",
                category="ui"
            ),
        ]
        
        # Experimental features
        experimental_features = [
            FeatureToggle(
                name="advanced_seasonal",
                description="Advanced seasonal decomposition",
                category="experimental",
                state=FeatureState.EXPERIMENTAL
            ),
            FeatureToggle(
                name="real_time_streaming",
                description="Real-time streaming processing",
                category="experimental",
                state=FeatureState.EXPERIMENTAL
            ),
            FeatureToggle(
                name="ml_boosted_detection",
                description="ML-boosted detection",
                category="experimental",
                state=FeatureState.EXPERIMENTAL
            ),
        ]
        
        # Register all features
        for feature in core_features + experimental_features:
            self.register_feature(feature)
        
        # Update from config
        self._update_from_config()
    
    def register_feature(self, feature: FeatureToggle) -> None:
        """Register a new feature toggle"""
        self._features[feature.name] = feature
        logger.debug(f"Registered feature: {feature.name}")
    
    def _update_from_config(self) -> None:
        """Update feature states from configuration"""
        features_config = self._config.get('features', {})
        
        for feature_name, feature in self._features.items():
            # Check main features
            if feature_name in features_config:
                feature.current_state = bool(features_config[feature_name])
            
            # Check experimental features
            elif (feature.state == FeatureState.EXPERIMENTAL and 
                  feature_name in features_config.get('experimental', {})):
                feature.current_state = bool(features_config['experimental'][feature_name])
            
            # Check category-based config
            elif f"enable_{feature.category}" in features_config:
                feature.current_state = bool(features_config[f"enable_{feature.category}"])
    
    def is_enabled(self, feature_name: str) -> bool:
        """
        Check if a feature is enabled
        
        Args:
            feature_name: Name of the feature
            
        Returns:
            bool: True if feature is enabled
            
        Raises:
            KeyError: If feature is not registered
        """
        if feature_name not in self._features:
            raise KeyError(f"Feature '{feature_name}' not registered")
        
        feature = self._features[feature_name]
        
        # Check dependencies
        for dep in feature.dependencies:
            if not self.is_enabled(dep):
                logger.warning(
                    f"Feature '{feature_name}' disabled because dependency "
                    f"'{dep}' is disabled"
                )
                return False
        
        return feature.current_state
    
    def enable_feature(self, feature_name: str) -> None:
        """Enable a feature at runtime"""
        if feature_name in self._features:
            self._features[feature_name].current_state = True
            logger.info(f"Enabled feature: {feature_name}")
        else:
            logger.warning(f"Cannot enable unregistered feature: {feature_name}")
    
    def disable_feature(self, feature_name: str) -> None:
        """Disable a feature at runtime"""
        if feature_name in self._features:
            self._features[feature_name].current_state = False
            logger.info(f"Disabled feature: {feature_name}")
        else:
            logger.warning(f"Cannot disable unregistered feature: {feature_name}")
    
    def get_feature(self, feature_name: str) -> Optional[FeatureToggle]:
        """Get feature toggle by name"""
        return self._features.get(feature_name)
    
    def get_all_features(self) -> Dict[str, FeatureToggle]:
        """Get all registered features"""
        return self._features.copy()
    
    def get_enabled_features(self) -> Dict[str, FeatureToggle]:
        """Get all enabled features"""
        return {
            name: feature 
            for name, feature in self._features.items() 
            if self.is_enabled(name)
        }
    
    def get_features_by_category(self, category: str) -> Dict[str, FeatureToggle]:
        """Get features by category"""
        return {
            name: feature 
            for name, feature in self._features.items() 
            if feature.category == category
        }
    
    def get_feature_status(self) -> Dict[str, Dict]:
        """Get status of all features"""
        return {
            name: {
                'enabled': self.is_enabled(name),
                'description': feature.description,
                'category': feature.category,
                'state': feature.state.value,
                'dependencies': feature.dependencies
            }
            for name, feature in self._features.items()
        }
    
    def require_feature(self, feature_name: str):
        """
        Decorator to require a feature to be enabled
        
        Usage:
            @feature_manager.require_feature('anomaly_detection')
            def detect_anomalies(data):
                ...
        """
        def decorator(func):
            def wrapper(*args, **kwargs):
                if not self.is_enabled(feature_name):
                    raise FeatureDisabledError(
                        f"Feature '{feature_name}' is required but disabled"
                    )
                return func(*args, **kwargs)
            return wrapper
        return decorator
    
    def feature_context(self, feature_name: str, enabled: bool = True):
        """
        Context manager for temporary feature state change
        
        Usage:
            with feature_manager.feature_context('experimental_feature', True):
                # experimental_feature is temporarily enabled
                do_experimental_stuff()
        """
        class FeatureContext:
            def __init__(self, manager, name, new_state):
                self.manager = manager
                self.name = name
                self.new_state = new_state
                self.original_state = None
            
            def __enter__(self):
                if self.name in self.manager._features:
                    self.original_state = self.manager._features[self.name].current_state
                    self.manager._features[self.name].current_state = self.new_state
                    logger.debug(f"Temporarily set {self.name} to {self.new_state}")
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                if self.original_state is not None:
                    self.manager._features[self.name].current_state = self.original_state
                    logger.debug(f"Restored {self.name} to {self.original_state}")
        
        return FeatureContext(self, feature_name, enabled)


class FeatureDisabledError(Exception):
    """Exception raised when a required feature is disabled"""
    pass


# Global feature manager instance
_feature_manager: Optional[FeatureManager] = None


def get_feature_manager(config: Optional[Dict] = None) -> FeatureManager:
    """
    Get or create the global feature manager instance
    
    Args:
        config: Configuration dictionary
        
    Returns:
        FeatureManager instance
    """
    global _feature_manager
    
    if _feature_manager is None:
        _feature_manager = FeatureManager(config)
    
    return _feature_manager


def reset_feature_manager() -> None:
    """Reset the global feature manager (mainly for testing)"""
    global _feature_manager
    _feature_manager = None