"""
Main entrypoint alias for cloud platforms (Render, Zeabur, Railway, etc.)
Supports both:
  - uvicorn main:app --host 0.0.0.0 --port $PORT
  - python main.py
"""
import os
import uvicorn
from app import app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
