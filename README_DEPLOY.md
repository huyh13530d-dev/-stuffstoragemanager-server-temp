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

## Cron backup (Telegram)
Nếu cần cron service riêng để backup PostgreSQL lên Telegram, dùng start command sau trong `railway.toml` của repo cron:
`bash -lc "apt-get update && apt-get install -y wget ca-certificates gnupg && wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg && echo deb [signed-by=/usr/share/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt noble-pgdg main > /etc/apt/sources.list.d/pgdg.list && apt-get update && apt-get install -y postgresql-client-18 && python backup_postgres_to_telegram.py --label railway"`

## Ghi chú
- Script Excel/data và file tiện ích không deploy đã được loại khỏi `backend/` để tránh nhầm lẫn.
- Khi cần công cụ data nội bộ, đặt ở thư mục riêng ngoài `backend/`.
