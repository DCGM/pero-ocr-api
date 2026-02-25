"""
Async file operations for images, result ZIPs, and engine packages.

Uses ``aiofiles`` for async reads/writes and ``asyncio.to_thread()``
for ``zipfile`` and ``shutil`` operations that have no native async API.
"""

import asyncio
import logging
import os
import shutil
import zipfile
from io import BytesIO

import aiofiles
import aiofiles.os
from filelock import FileLock, Timeout

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

async def ensure_directory(path: str) -> None:
    """Create a directory (and parents) if it doesn't exist."""
    await aiofiles.os.makedirs(path, exist_ok=True)


# ---------------------------------------------------------------------------
# Image upload / download / delete
# ---------------------------------------------------------------------------

async def save_uploaded_image(
    folder: str,
    request_id: str,
    page_name: str,
    extension: str,
    data: bytes,
) -> str:
    """
    Save uploaded image bytes to disk.
    Returns the full path of the saved file.
    """
    dir_path = os.path.join(folder, request_id)
    await ensure_directory(dir_path)
    file_path = os.path.join(dir_path, f"{page_name}.{extension}")
    async with aiofiles.open(file_path, "wb") as f:
        await f.write(data)
    return file_path


async def delete_image(
    folder: str, request_id: str, page_name: str, extension: str,
) -> None:
    """Delete an uploaded image from disk (if it exists)."""
    path = os.path.join(folder, str(request_id), f"{page_name}.{extension}")
    try:
        await aiofiles.os.remove(path)
    except FileNotFoundError:
        pass


async def read_image_file(path: str) -> bytes:
    """Read an image file from disk."""
    async with aiofiles.open(path, "rb") as f:
        return await f.read()


# ---------------------------------------------------------------------------
# ZIP result archives
# ---------------------------------------------------------------------------

def _write_to_zip_sync(
    zip_path: str,
    lock_path: str,
    entries: dict[str, bytes],
) -> None:
    """Write entries to a ZIP archive with file-level locking (synchronous)."""
    try:
        with FileLock(lock_path, timeout=5):
            with zipfile.ZipFile(zip_path, "a", zipfile.ZIP_DEFLATED) as zf:
                for name, data in entries.items():
                    zf.writestr(name, data)
    except Timeout:
        # Fallback: write without lock (matching original behaviour)
        logger.warning("FileLock timeout on %s — writing without lock", zip_path)
        with zipfile.ZipFile(zip_path, "a", zipfile.ZIP_DEFLATED) as zf:
            for name, data in entries.items():
                zf.writestr(name, data)


async def write_results_to_zip(
    results_folder: str,
    request_id: str,
    page_name: str,
    alto_data: bytes,
    page_data: bytes,
    txt_data: bytes,
) -> None:
    """Append OCR results for one page into the request's result ZIP."""
    dir_path = os.path.join(results_folder, request_id)
    await ensure_directory(dir_path)

    zip_path = os.path.join(dir_path, f"{request_id}.zip")
    lock_path = os.path.join(dir_path, f"{request_id}_lock")
    entries = {
        f"{page_name}_alto.xml": alto_data,
        f"{page_name}_page.xml": page_data,
        f"{page_name}.txt": txt_data,
    }
    await asyncio.to_thread(_write_to_zip_sync, zip_path, lock_path, entries)


def _read_from_zip_sync(
    zip_path: str, lock_path: str, entry_name: str,
) -> bytes:
    """Read a single entry from a ZIP archive with locking (synchronous)."""
    try:
        with FileLock(lock_path, timeout=1):
            with zipfile.ZipFile(zip_path, "r") as zf:
                return zf.read(entry_name)
    except Timeout:
        with zipfile.ZipFile(zip_path, "r") as zf:
            return zf.read(entry_name)


async def read_result_from_zip(
    results_folder: str,
    request_id: str,
    page_name: str,
    fmt: str,
) -> tuple[bytes, str]:
    """
    Read an OCR result from the request ZIP.
    Returns ``(data_bytes, file_extension)``.
    """
    dir_path = os.path.join(results_folder, request_id)
    zip_path = os.path.join(dir_path, f"{request_id}.zip")
    lock_path = os.path.join(dir_path, f"{request_id}_lock")

    if fmt == "alto":
        entry = f"{page_name}_alto.xml"
        ext = "xml"
    elif fmt == "page":
        entry = f"{page_name}_page.xml"
        ext = "xml"
    else:  # txt
        entry = f"{page_name}.txt"
        ext = "txt"

    data = await asyncio.to_thread(_read_from_zip_sync, zip_path, lock_path, entry)
    return data, ext


# ---------------------------------------------------------------------------
# Engine packaging
# ---------------------------------------------------------------------------

def _build_engine_zip_sync(
    models_folder: str, models: list, engine_config_header: str,
) -> bytes:
    """Build an in-memory ZIP of engine model files + config.ini (synchronous)."""
    engine_config = engine_config_header
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        for model in models:
            model_dir = os.path.join(models_folder, model.name)
            for root, _dirs, files in os.walk(model_dir):
                for file in files:
                    zf.write(
                        os.path.join(root, file),
                        os.path.join(model.name, file),
                    )
            engine_config += model.config + "\n\n"
        zf.writestr("config.ini", engine_config)
    return buf.getvalue()


async def build_engine_zip(
    models_folder: str, models: list, engine_config_header: str,
) -> bytes:
    """Build engine ZIP asynchronously."""
    return await asyncio.to_thread(
        _build_engine_zip_sync, models_folder, models, engine_config_header,
    )


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

async def delete_request_folders(
    results_folder: str, images_folder: str, request_id: str,
) -> None:
    """Remove both results and images directories for a request."""
    for folder in [results_folder, images_folder]:
        dir_path = os.path.join(folder, request_id)
        if await asyncio.to_thread(os.path.isdir, dir_path):
            await asyncio.to_thread(shutil.rmtree, dir_path)
