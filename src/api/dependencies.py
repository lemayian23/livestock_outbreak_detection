"""
Shared dependencies: pipeline instance, config, logger.
"""
from typing import Any
from fastapi import Request

from custom_logging.structured_logger import get_structured_logger
from utils.config import Config
from utils.feature_manager import get_feature_manager


logger = get_structured_logger()


def get_pipeline(request: Request) -> Any:
    """Return the singleton pipeline from app state."""
    pipeline = getattr(request.app.state, "pipeline", None)
    if pipeline is None:
        raise RuntimeError("Pipeline not initialised on app state")
    return pipeline


def get_app_config() -> Config:
    """Return the Config object (loaded once)."""
    return Config()