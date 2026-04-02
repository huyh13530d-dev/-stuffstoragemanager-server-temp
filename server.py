"""
Standalone server entry point for Railway / cloud deployment.
Run from the backend directory: python server.py
"""
import os
import uvicorn

from api import app  # noqa: F401

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    uvicorn.run(app, host=host, port=port)
