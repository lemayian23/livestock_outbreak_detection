"""
FastAPI application entry point.
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from custom_logging.structured_logger import get_structured_logger
from utils.config import Config

from . import __version__
from .errors import register_exception_handlers
from .routes import router


logger = get_structured_logger()
logger.set_context(component="api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    env = os.getenv("APP_ENV", "development")
    app.state.env = env

    logger.info(f"Starting API for environment: {env}")

    # Load config
    config = Config()
    app.state.config = config

    # Initialise pipeline ONCE (heavy — normalizers, models)
    from run_pipeline import FeatureAwarePipeline
    pipeline = FeatureAwarePipeline(config_path="config/settings.yaml", env=env)
    pipeline.initialize_components()
    app.state.pipeline = pipeline

    logger.info("API startup complete")

    yield

    logger.info("API shutting down")


def create_app() -> FastAPI:
    """Build and configure the FastAPI app."""
    config = Config()
    api_cfg = config.raw_config.get("api", {}) or {}

    docs_enabled = api_cfg.get("docs_enabled", True)

    app = FastAPI(
        title="Livestock Outbreak Detection API",
        version=__version__,
        description="REST wrapper around the livestock anomaly detection pipeline",
        lifespan=lifespan,
        docs_url="/docs" if docs_enabled else None,
        redoc_url="/redoc" if docs_enabled else None,
        openapi_url="/openapi.json" if docs_enabled else None,
    )

    # CORS
    cors_origins = api_cfg.get("cors_origins", ["*"])
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    register_exception_handlers(app)
    app.include_router(router)

    return app


app = create_app()