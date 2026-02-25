"""
User routes — require a valid USER (or SUPER_USER) API key.

Endpoints:
- POST /post_processing_request
- GET  /usage_statistics[/<from>[/<to>]]
- POST /upload_image/<request_id>/<page_name>
- GET  /request_status/<request_id>
- GET  /get_engines
- GET  /download_results/<request_id>/<page_name>/<format>
- POST /cancel_request/<request_id>
"""

import logging
import os
import traceback
from typing import Optional
from urllib.parse import urlparse

import dateutil.parser
from fastapi import APIRouter, Depends, Header, Request, UploadFile, File
from fastapi.responses import JSONResponse, Response
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ApiKey, PageState
from app.config import get_settings, Settings
from app.dependencies import get_db, get_current_user, guard_request_ownership
from app.exceptions import NotFoundError, BadRequestError, ValidationError
from app.schemas.requests import ProcessingRequestCreate
from app.schemas.responses import (
    EngineListResponse,
    ErrorResponse,
    RequestCreatedResponse,
    RequestStatusResponse,
    StatusResponse,
    UsageStatisticsResponse,
    PageStatusItem,
)
from app.crud.request import (
    create_request,
    cancel_request_by_id,
    get_document_pages,
)
from app.crud.page import get_page_and_state, change_page_path
from app.crud.engine import get_engine_dict
from app.crud.statistics import get_usage_statistics
from app.services.file_service import save_uploaded_image, read_result_from_zip

logger = logging.getLogger(__name__)

router = APIRouter(tags=["User"])

# Reusable response definitions for OpenAPI documentation
_auth_error = {401: {"model": ErrorResponse, "description": "Missing or invalid API key, or insufficient permissions."}}
_not_found = {404: {"model": ErrorResponse, "description": "Requested resource was not found."}}
_bad_request = {400: {"model": ErrorResponse, "description": "Bad request — invalid parameters."}}
_validation_error = {422: {"model": ErrorResponse, "description": "Validation error — malformed input."}}


# ---------------------------------------------------------------------------
# POST /post_processing_request
# ---------------------------------------------------------------------------

@router.post(
    "/post_processing_request",
    response_model=RequestCreatedResponse,
    tags=["User"],
    summary="Create a new OCR processing request",
    description=(
        "Submit a batch of images for OCR processing. Each image entry maps a page name "
        "to either a URL (the page starts as **WAITING** and is fetched by the worker) or "
        "`null` (the page starts as **CREATED** and must be uploaded via "
        "`POST /upload_image`).\n\n"
        "Requires a valid `api-key` header with USER or SUPER_USER permission."
    ),
    responses={
        **_auth_error,
        404: {"model": ErrorResponse, "description": "The specified engine was not found."},
        422: {"model": ErrorResponse, "description": "Malformed JSON body or invalid request structure."},
    },
)
async def post_processing_request(
    body: ProcessingRequestCreate,
    user: ApiKey = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Submit images for OCR processing.  Images with a URL start as
    WAITING; images with ``null`` URL start as CREATED (upload needed).
    """
    try:
        req = await create_request(db, user, body.engine, body.images)
    except Exception:
        tb = traceback.format_exc()
        logger.warning("Bad request in post_processing_request: %s", tb)
        raise ValidationError(f"Bad JSON formatting. {tb}")

    if req is None:
        raise NotFoundError(f"Engine {body.engine} has not been found.")

    return RequestCreatedResponse(status="success", request_id=req.id)


# ---------------------------------------------------------------------------
# GET /usage_statistics
# ---------------------------------------------------------------------------

@router.get(
    "/usage_statistics",
    response_model=UsageStatisticsResponse,
    tags=["User"],
    summary="Get usage statistics for the authenticated user",
    description=(
        "Return the number of pages processed by the authenticated user. "
        "Optionally filter by date range using ISO 8601 query parameters "
        "`from_datetime` and `to_datetime`.\n\n"
        "Requires a valid `api-key` header with USER or SUPER_USER permission."
    ),
    responses={
        **_auth_error,
        400: {"model": ErrorResponse, "description": "Date parameters are not in valid ISO 8601 format."},
    },
)
@router.get(
    "/usage_statistics/{from_datetime}",
    response_model=UsageStatisticsResponse,
    tags=["User"],
    include_in_schema=False,
)
@router.get(
    "/usage_statistics/{from_datetime}/{to_datetime}",
    response_model=UsageStatisticsResponse,
    tags=["User"],
    include_in_schema=False,
)
async def usage_statistics(
    from_datetime: Optional[str] = None,
    to_datetime: Optional[str] = None,
    user: ApiKey = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the number of processed pages, optionally filtered by date range."""
    parsed_from = None
    parsed_to = None

    if from_datetime:
        try:
            parsed_from = dateutil.parser.isoparse(from_datetime)
        except ValueError:
            raise BadRequestError("from_time is not in a valid ISO format.")

    if to_datetime:
        try:
            parsed_to = dateutil.parser.isoparse(to_datetime)
        except ValueError:
            raise BadRequestError("to_datetime is not in a valid ISO format.")

    count = await get_usage_statistics(
        db, user.api_string, from_datetime=parsed_from, to_datetime=parsed_to,
    )
    result = UsageStatisticsResponse(status="success", processed_pages=count)
    if parsed_from:
        result.from_date = parsed_from.isoformat()
    if parsed_to:
        result.to_date = parsed_to.isoformat()
    return result


# ---------------------------------------------------------------------------
# POST /upload_image/<request_id>/<page_name>
# ---------------------------------------------------------------------------

@router.post(
    "/upload_image/{request_id}/{page_name}",
    response_model=StatusResponse,
    tags=["User"],
    summary="Upload an image for a page in CREATED state",
    description=(
        "Upload an image file for a page that was created with a `null` URL in the "
        "processing request. The page must be in **CREATED** state. "
        "Supported image formats are configured server-side (e.g. jpg, png, tif).\n\n"
        "Requires a valid `api-key` header with USER or SUPER_USER permission. "
        "The request must belong to the authenticated user."
    ),
    responses={
        **_auth_error,
        404: {"model": ErrorResponse, "description": "Request or page not found, or request does not belong to the user."},
        400: {"model": ErrorResponse, "description": "Page is not in CREATED state, or the request body does not contain a file."},
        422: {"model": ErrorResponse, "description": "Unsupported image format."},
    },
)
async def upload_image(
    request_id: str,
    page_name: str,
    request: Request,
    file: UploadFile = File(...),
    user: ApiKey = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Upload an image file for a page that was created with a null URL."""
    _ = await guard_request_ownership(db, user, request_id)

    page, page_state = await get_page_and_state(db, request_id, page_name)
    if not page:
        raise NotFoundError(f"Page {page_name} does not exist.")
    if page_state != PageState.CREATED:
        raise BadRequestError(
            f"Page {page_name} is in {page_state.name} state. It should be in CREATED state."
        )

    if file is None or file.filename is None:
        raise BadRequestError("Request does not contain file.")

    extension = os.path.splitext(file.filename)[1][1:].lower()
    if extension not in settings.ALLOWED_IMAGE_EXTENSIONS:
        allowed = ", ".join(sorted(settings.ALLOWED_IMAGE_EXTENSIONS))
        raise ValidationError(
            f"{extension} is not supported format. Supported formats are {allowed}."
        )

    data = await file.read()
    await save_uploaded_image(
        settings.UPLOAD_IMAGES_FOLDER, request_id, page_name, extension, data,
    )

    # Build the download URL
    base_url = str(request.base_url).rstrip("/")
    path = f"{base_url}{settings.APPLICATION_ROOT}/download_image/{request_id}/{page_name}.{extension}"
    await change_page_path(db, request_id, page_name, path)

    return StatusResponse(status="success")


# ---------------------------------------------------------------------------
# GET /request_status/<request_id>
# ---------------------------------------------------------------------------

@router.get(
    "/request_status/{request_id}",
    response_model=RequestStatusResponse,
    tags=["User"],
    summary="Get the processing status of all pages in a request",
    description=(
        "Return the current state and quality score for every page in the specified request. "
        "Possible page states: CREATED, WAITING, PROCESSING, PROCESSED, FAILED, EXPIRED.\n\n"
        "Requires a valid `api-key` header. The request must belong to the authenticated user."
    ),
    responses={
        **_auth_error,
        404: {"model": ErrorResponse, "description": "Request not found or does not belong to the authenticated user."},
    },
)
async def request_status(
    request_id: str,
    user: ApiKey = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Returns state and quality score for every page in the request."""
    req = await guard_request_ownership(db, user, request_id)
    pages = await get_document_pages(db, req.id)
    return RequestStatusResponse(
        status="success",
        request_status={
            p.name: PageStatusItem(
                state=p.state.name,
                quality=p.score,
            )
            for p in pages
        },
    )


# ---------------------------------------------------------------------------
# GET /get_engines
# ---------------------------------------------------------------------------

@router.get(
    "/get_engines",
    response_model=EngineListResponse,
    tags=["User"],
    summary="List available OCR engines",
    description=(
        "Return a dictionary of all available OCR engines. Each entry includes the engine "
        "description, latest version string, and associated model information.\n\n"
        "Requires a valid `api-key` header with USER or SUPER_USER permission."
    ),
    responses={**_auth_error},
)
async def get_engines(
    user: ApiKey = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return all available engines with their latest version and models."""
    engines = await get_engine_dict(db)
    return EngineListResponse(status="success", engines=engines)


# ---------------------------------------------------------------------------
# GET /download_results/<request_id>/<page_name>/<format>
# ---------------------------------------------------------------------------

@router.get(
    "/download_results/{request_id}/{page_name}/{format}",
    tags=["User"],
    summary="Download OCR results for a page",
    description=(
        "Download the OCR results for a single page. The `format` path parameter must be "
        "one of: `alto` (ALTO XML), `page` (PAGE XML), or `txt` (plain text).\n\n"
        "The page must be in **PROCESSED** state. Processed results expire after one week.\n\n"
        "Requires a valid `api-key` header. The request must belong to the authenticated user."
    ),
    responses={
        200: {"content": {"application/octet-stream": {}}, "description": "The result file as a binary download."},
        **_auth_error,
        404: {"model": ErrorResponse, "description": "Page not found, results expired, or page not yet processed."},
        400: {"model": ErrorResponse, "description": "Unsupported export format. Must be `alto`, `page`, or `txt`."},
    },
)
async def download_results(
    request_id: str,
    page_name: str,
    format: str,
    user: ApiKey = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """
    Download results in ``alto`` (XML), ``page`` (XML), or ``txt`` format.
    Only available for pages in PROCESSED state.
    """
    req = await guard_request_ownership(db, user, request_id)

    page, page_state = await get_page_and_state(db, str(req.id), page_name)
    if not page:
        raise NotFoundError(f"Page {page_name} does not exist.")
    if page_state == PageState.EXPIRED:
        raise NotFoundError(
            f"Page {page_name} has expired. All processed pages are stored one week."
        )
    if page_state != PageState.PROCESSED:
        raise NotFoundError(f"Page {page_name} is not processed yet.")
    if format not in ("alto", "page", "txt"):
        raise BadRequestError("Bad export format. Supported formats are alto, page, txt.")

    data, ext = await read_result_from_zip(
        settings.PROCESSED_REQUESTS_FOLDER, str(req.id), page_name, format,
    )
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{page_name}.{ext}"'
        },
    )


# ---------------------------------------------------------------------------
# POST /cancel_request/<request_id>
# ---------------------------------------------------------------------------

@router.post(
    "/cancel_request/{request_id}",
    response_model=StatusResponse,
    tags=["User"],
    summary="Cancel a processing request",
    description=(
        "Cancel all pages in the request that are in CREATED, WAITING, or PROCESSING state. "
        "Pages already in PROCESSED, FAILED, or EXPIRED state are not affected.\n\n"
        "Requires a valid `api-key` header. The request must belong to the authenticated user."
    ),
    responses={
        **_auth_error,
        404: {"model": ErrorResponse, "description": "Request not found or does not belong to the authenticated user."},
    },
)
async def cancel_request(
    request_id: str,
    user: ApiKey = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Cancel all CREATED / WAITING / PROCESSING pages in the request."""
    req = await guard_request_ownership(db, user, request_id)
    await cancel_request_by_id(db, req.id)
    return StatusResponse(status="success")
