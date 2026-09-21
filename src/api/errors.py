"""
Centralised exception handlers returning a consistent JSON shape.
"""
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from custom_logging.structured_logger import get_structured_logger


logger = get_structured_logger()


def _json(error: str, code: str, detail: str = None, status: int = 500):
    body = {"error": error, "code": code}
    if detail:
        body["detail"] = detail
    return JSONResponse(status_code=status, content=body)


def register_exception_handlers(app: FastAPI) -> None:
    """Wire up custom exception handlers."""

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request: Request, exc: RequestValidationError):
        logger.warning(
            f"Request validation failed: {request.method} {request.url.path}",
            error_count=len(exc.errors()),
        )
        return _json(
            error="Request validation failed",
            code="REQUEST_VALIDATION_FAILED",
            detail=str(exc.errors()),
            status=422,
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_handler(request: Request, exc: StarletteHTTPException):
        detail = exc.detail
        if isinstance(detail, dict):
            error = detail.get("error", "HTTP error")
            code = detail.get("code", "HTTP_ERROR")
        else:
            error = str(detail)
            code = "HTTP_ERROR"
        return _json(error=error, code=code, status=exc.status_code)

    @app.exception_handler(Exception)
    async def generic_handler(request: Request, exc: Exception):
        logger.error(
            f"Unhandled error on {request.method} {request.url.path}",
            exception=exc,
        )
        return _json(
            error="Internal server error",
            code="INTERNAL_ERROR",
            detail=str(exc),
            status=500,
        )