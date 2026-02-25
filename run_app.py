"""Entry point for PERO-OCR-API. Run with: python run_app.py"""

import uvicorn

from app import create_app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "run_app:app",
        host="0.0.0.0",
        port=5000,
        reload=True,
    )
