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
from .database import init_db
from .errors import register_exception_handlers
from .routes import router as detection_router
from .routers.auth import router as auth_router
from .routers.runs import router as runs_router
from .routers.api_keys import router as api_keys_router


logger = get_structured_logger()
logger.set_context(component="api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    env = os.getenv("APP_ENV", "development")
    app.state.env = env

    logger.info(f"Starting API for environment: {env}")

    config = Config()
    app.state.config = config

    # Create tables on startup (dev). In production, rely on Alembic.
    try:
        init_db()
        logger.info("Database tables ensured")
    except Exception as e:
        logger.error(f"Database init failed: {e}")

    from run_pipeline import FeatureAwarePipeline
    pipeline = FeatureAwarePipeline(config_path="config/settings.yaml", env=env)
    pipeline.initialize_components()
    app.state.pipeline = pipeline

    logger.info("API startup complete")
    yield
    logger.info("API shutting down")


def create_app() -> FastAPI:
    config = Config()
    api_cfg = config.raw_config.get("api", {}) or {}
    docs_enabled = api_cfg.get("docs_enabled", True)

    app = FastAPI(
        title="Livestock Outbreak Detection API",
        version=__version__,
        description="REST API for livestock anomaly detection",
        lifespan=lifespan,
        docs_url="/docs" if docs_enabled else None,
        redoc_url="/redoc" if docs_enabled else None,
        openapi_url="/openapi.json" if docs_enabled else None,
    )

    cors_origins = api_cfg.get("cors_origins", ["*"])
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    register_exception_handlers(app)

    # Existing detection endpoints
    app.include_router(detection_router)
    # New auth / data routers
    app.include_router(auth_router)
    app.include_router(runs_router)
    app.include_router(api_keys_router)

    return app


app = create_app()