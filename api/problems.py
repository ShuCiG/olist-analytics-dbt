"""Errors as RFC 7807 `application/problem+json`."""

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

MEDIA_TYPE = "application/problem+json"
TYPE_PREFIX = "urn:olist-api:problem:"


class ApiProblem(Exception):
    def __init__(
        self,
        status: int,
        slug: str,
        title: str,
        detail: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status = status
        self.slug = slug
        self.title = title
        self.detail = detail
        self.headers = headers or {}


def problem_response(
    request: Request,
    status: int,
    slug: str,
    title: str,
    detail: str,
    headers: dict[str, str] | None = None,
    **extra: Any,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": TYPE_PREFIX + slug,
        "title": title,
        "status": status,
        "detail": detail,
        "instance": request.url.path,
        **extra,
    }
    return JSONResponse(body, status_code=status, media_type=MEDIA_TYPE, headers=headers)


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiProblem)
    async def api_problem(request: Request, exc: ApiProblem) -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        return problem_response(request, exc.status, exc.slug, exc.title, exc.detail, exc.headers)

    @app.exception_handler(RequestValidationError)
    async def invalid(request: Request, exc: RequestValidationError) -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        errors = [
            {"where": ".".join(str(part) for part in err["loc"]), "message": err["msg"]}
            for err in exc.errors()
        ]
        return problem_response(
            request, 422, "invalid-request", "Invalid request", f"{len(errors)} invalid field(s)", errors=errors
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        slug = {404: "not-found", 405: "method-not-allowed"}.get(exc.status_code, "http-error")
        return problem_response(request, exc.status_code, slug, str(exc.detail), str(exc.detail))
