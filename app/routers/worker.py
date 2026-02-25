"""
Worker routes — require a SUPER_USER API key.

Endpoints:
- GET  /get_processing_request/<preferred_engine_id>
- POST /upload_results/<page_id>
- GET  /latest_engine_version/<engine_id>
- GET  /download_engine/<engine_id>
- POST /failed_processing/<page_id>
- GET  /page_statistics
- GET  /download_image/<request_id>/<page_name>
"""

import datetime
import logging
import os
import traceback as tb_module

from fastapi import APIRouter, Depends, Header, Request, UploadFile, File
from fastapi.responses import JSONResponse, Response
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ApiKey, PageState
from app.config import get_settings, Settings
from app.dependencies import get_db, get_super_user
from app.exceptions import NotFoundError, BadRequestError
from app.schemas.responses import (
    LatestEngineVersionResponse,
    PageStatisticsResponse,
    ProcessingTaskResponse,
    StatusResponse,
)
from app.crud.engine import get_engine, get_engine_version_by_name, get_latest_models
from app.crud.page import (
    change_page_to_failed,
    change_page_to_processed,
    get_engine_by_page_id,
    get_page_and_state,
    get_page_by_id,
    get_page_by_preferred_engine,
)
from app.crud.request import request_exists
from app.crud.statistics import (
    get_notification_timestamp,
    get_page_statistics,
    set_notification_timestamp,
)
from app.crud.api_key import get_api_key_by_id
from app.services.file_service import (
    build_engine_zip,
    delete_image,
    write_results_to_zip,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Worker"])


# ---------------------------------------------------------------------------
# GET /get_processing_request/<preferred_engine_id>
# ---------------------------------------------------------------------------

@router.get(
    "/get_processing_request/{preferred_engine_id}",
    tags=["Worker"],
    summary="Fetch the next page to process",
    responses={204: {"description": "No pages available"}},
)
async def get_processing_request(
    preferred_engine_id: int,
    user: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fair-scheduling dispatcher: returns the next WAITING page for processing.
    Excludes pages from suspended users. Returns 204 when nothing is available.
    """
    page, engine_id = await get_page_by_preferred_engine(db, preferred_engine_id)

    if page:
        return ProcessingTaskResponse(
            status="success",
            page_id=page.id,
            page_url=page.url,
            engine_id=engine_id,
        )
    else:
        return JSONResponse(
            status_code=204,
            content={"status": "failure", "message": "No page available for processing."},
        )


# ---------------------------------------------------------------------------
# POST /upload_results/<page_id>
# ---------------------------------------------------------------------------

@router.post(
    "/upload_results/{page_id}",
    response_model=StatusResponse,
    tags=["Worker"],
    summary="Upload OCR results for a page",
)
async def upload_results(
    page_id: str,
    request: Request,
    alto: UploadFile = File(...),
    page: UploadFile = File(...),
    txt: UploadFile = File(...),
    score: str = Header(...),
    engine_version: str = Header(..., alias="engine-version"),
    user: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """
    Upload ALTO XML, PAGE XML, and TXT results for a processed page.
    The page is transitioned to PROCESSED state.
    """
    page_row = await get_page_by_id(db, page_id)
    if not page_row:
        raise NotFoundError(f"Page {page_id} does not exist.")

    score_value = round(float(score) * 100, 2)
    engine_version_str = str(engine_version)

    engine_obj = await get_engine_by_page_id(db, page_id)
    ev = await get_engine_version_by_name(db, engine_obj.id, engine_version_str)

    # Read uploaded file data
    alto_data = await alto.read()
    page_data = await page.read()  # 'page' here is the UploadFile, not the DB row
    txt_data = await txt.read()

    # Write results to ZIP archive
    await write_results_to_zip(
        settings.PROCESSED_REQUESTS_FOLDER,
        str(page_row.request_id),
        page_row.name,
        alto_data, page_data, txt_data,
    )

    # Update DB state
    await change_page_to_processed(db, page_id, score_value, ev.id)

    # Remove uploaded image if it exists
    if page_row.url:
        extension = page_row.url.split(".")[-1]
        await delete_image(
            settings.UPLOAD_IMAGES_FOLDER,
            str(page_row.request_id),
            page_row.name,
            extension,
        )

    return StatusResponse(status="success")


# ---------------------------------------------------------------------------
# GET /latest_engine_version/<engine_id>
# ---------------------------------------------------------------------------

@router.get(
    "/latest_engine_version/{engine_id}",
    response_model=LatestEngineVersionResponse,
    tags=["Worker"],
    summary="Get the latest engine version filename",
)
async def latest_engine_version(
    engine_id: int,
    user: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the filename of the latest engine version package."""
    engine_obj = await get_engine(db, engine_id)
    if not engine_obj:
        raise NotFoundError(f"Engine {engine_id} has not been found.")

    ev, models = await get_latest_models(db, engine_id)

    if len(models) not in (2, 3):
        raise BadRequestError("Unexpected number of models for engine.")

    filename = f"{engine_obj.name}#{ev.version}.zip"
    return LatestEngineVersionResponse(status="success", filename=filename)


# ---------------------------------------------------------------------------
# GET /download_engine/<engine_id>
# ---------------------------------------------------------------------------

@router.get(
    "/download_engine/{engine_id}",
    tags=["Worker"],
    summary="Download the engine model package",
)
async def download_engine(
    engine_id: int,
    user: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """
    Package engine model files into a ZIP with a generated ``config.ini``
    and return it as a download.
    """
    engine_obj = await get_engine(db, engine_id)
    if not engine_obj:
        raise NotFoundError(f"Engine {engine_id} has not been found.")

    ev, models = await get_latest_models(db, engine_id)

    if len(models) == 2:
        config_header = (
            "[PAGE_PARSER]\n"
            "RUN_LAYOUT_PARSER = yes\n"
            "RUN_LINE_CROPPER = yes\n"
            "RUN_OCR = yes\n"
            "RUN_DECODER = no\n"
            "\n\n"
        )
    elif len(models) == 3:
        config_header = (
            "[PAGE_PARSER]\n"
            "RUN_LAYOUT_PARSER = yes\n"
            "RUN_LINE_CROPPER = yes\n"
            "RUN_OCR = yes\n"
            "RUN_DECODER = yes\n"
            "\n\n"
        )
    else:
        raise BadRequestError("Unexpected number of models for engine.")

    zip_data = await build_engine_zip(settings.MODELS_FOLDER, models, config_header)

    return Response(
        content=zip_data,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{engine_obj.name}#{ev.version}.zip"'
        },
    )


# ---------------------------------------------------------------------------
# POST /failed_processing/<page_id>
# ---------------------------------------------------------------------------

@router.post(
    "/failed_processing/{page_id}",
    response_model=StatusResponse,
    tags=["Worker"],
    summary="Report a processing failure",
)
async def report_failed_processing(
    page_id: str,
    request: Request,
    user: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
    fail_type: str = Header(..., alias="type"),
    engine_version_str: str = Header(..., alias="engine_version"),
    hostname: str = Header("unknown", alias="hostname"),
    ip_address: str = Header("unknown", alias="ip-address"),
):
    """
    Report that processing of a page failed.
    Supported types: NOT_FOUND, INVALID_FILE, PROCESSING_FAILED.
    """
    traceback_str = (await request.body()).decode("utf-8", errors="replace")

    engine_obj = await get_engine_by_page_id(db, page_id)
    ev = await get_engine_version_by_name(db, engine_obj.id, engine_version_str)

    await change_page_to_failed(db, page_id, fail_type, traceback_str, ev.id)

    # Rate-limited email notification for PROCESSING_FAILED
    if fail_type == "PROCESSING_FAILED" and settings.EMAIL_NOTIFICATION_ADDRESSES:
        notification_ts = await get_notification_timestamp(db)
        now = datetime.datetime.now(datetime.UTC)
        elapsed = (now - notification_ts).total_seconds() if notification_ts else float("inf")

        if elapsed > settings.MAX_EMAIL_FREQUENCY:
            page_row = await get_page_by_id(db, page_id)
            req_obj = await request_exists(db, str(page_row.request_id))
            api_key_obj = await get_api_key_by_id(db, req_obj.api_key_id)

            message_body = (
                f"processing_client_hostname: {hostname}<br>"
                f"processing_client_ip_address: {ip_address}<br>"
                f"owner_api_key: {api_key_obj.api_string}<br>"
                f"owner_description: {api_key_obj.owner}<br>"
                f"engine_id: {engine_obj.id}<br>"
                f"engine_name: {engine_obj.name}<br>"
                f"request_id: {req_obj.id}<br>"
                f"page_id: {page_row.id}<br>"
                f"page_name: {page_row.name}<br>"
                f"page_url: {page_row.url}<br>"
                "####################<br>"
                f"traceback:<br>{traceback_str.replace(chr(10), '<br>')}"
            )

            from app.services.mail_service import send_notification_mail_async
            await send_notification_mail_async(
                subject="API Bot - PROCESSING_FAILED",
                body=message_body,
                settings=settings,
            )
            await set_notification_timestamp(db)

    return StatusResponse(status="success")


# ---------------------------------------------------------------------------
# GET /page_statistics
# ---------------------------------------------------------------------------

@router.get(
    "/page_statistics",
    response_model=PageStatisticsResponse,
    tags=["Worker"],
    summary="Get page processing statistics",
)
async def page_statistics(
    user: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Return page counts by state for the last 24 hours."""
    stats = await get_page_statistics(db)
    return PageStatisticsResponse(status="success", state_stats=stats)


# ---------------------------------------------------------------------------
# GET /download_image/<request_id>/<page_name>
# ---------------------------------------------------------------------------

@router.get(
    "/download_image/{request_id}/{page_name}",
    tags=["Worker"],
    summary="Download an uploaded image",
)
async def download_image(
    request_id: str,
    page_name: str,
    user: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """
    Download an image uploaded by a user. The page_name includes the extension
    (e.g. ``img.jpg``).
    """
    # Parse extension from page_name
    extension = page_name.split(".")[-1]
    base_name = page_name[:-(len(extension) + 1)]

    req = await request_exists(db, request_id)
    if not req:
        raise NotFoundError(f"Request {request_id} does not exist.")

    page_row, page_state = await get_page_and_state(db, request_id, base_name)
    if not page_row:
        raise NotFoundError(f"Page {base_name} does not exist.")
    if page_state == PageState.CREATED:
        raise NotFoundError(f"Page {base_name} has not been uploaded yet.")
    if page_state == PageState.PROCESSED:
        return JSONResponse(
            status_code=405,
            content={
                "status": "failure",
                "message": f"Page {base_name} has been already processed.",
            },
        )

    file_path = os.path.join(
        settings.UPLOAD_IMAGES_FOLDER, str(req.id),
        f"{page_row.name}.{extension}",
    )
    from app.services.file_service import read_image_file
    data = await read_image_file(file_path)

    # Guess content type
    ct = "image/jpeg" if extension in ("jpg", "jpeg") else f"image/{extension}"
    return Response(content=data, media_type=ct)
