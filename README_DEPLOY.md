# Backend Deploy (Railway)

Thư mục `backend/` đã được dọn để dùng trực tiếp cho deploy Railway.

## File cần cho deploy
- `api.py` - FastAPI app
- `database.py` - SQLAlchemy models + `DATABASE_URL`
- `server.py` - local/entry helper
- `requirements.txt` - Python dependencies
- `Procfile` - start command cho platform đọc Procfile
- `railway.toml` - config build/deploy Railway

## Start command
`uvicorn api:app --host 0.0.0.0 --port $PORT`

## Ghi chú
- Script Excel/data và file tiện ích không deploy đã được loại khỏi `backend/` để tránh nhầm lẫn.
- Khi cần công cụ data nội bộ, đặt ở thư mục riêng ngoài `backend/`.
