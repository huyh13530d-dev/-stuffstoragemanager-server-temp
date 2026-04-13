import argparse
import os
import sys
import tempfile
import subprocess
from datetime import datetime, timezone, timedelta

import requests


DEFAULT_PG_DUMP_TIMEOUT_SECONDS = 900
DEFAULT_TELEGRAM_CONNECT_TIMEOUT_SECONDS = 15
DEFAULT_TELEGRAM_READ_TIMEOUT_SECONDS = 300


def _get_db_url(cli_url: str | None) -> str:
    candidates = [
        cli_url,
        os.environ.get("DATABASE_URL"),
        os.environ.get("POSTGRES_URL"),
        os.environ.get("POSTGRESQL_URL"),
    ]
    for value in candidates:
        if value and value.strip():
            return value.strip()
    raise RuntimeError("Missing PostgreSQL URL. Provide --db-url or set DATABASE_URL.")


def _get_bot_token() -> str:
    token = os.environ.get("TELEGRAM_DB_BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing Telegram bot token. Set TELEGRAM_DB_BOT_TOKEN or TELEGRAM_BOT_TOKEN.")
    return token.strip()


def _get_chat_id() -> str:
    chat_id = os.environ.get("TELEGRAM_DB_CHAT_ID")
    if not chat_id:
        raise RuntimeError("Missing Telegram DB chat id. Set TELEGRAM_DB_CHAT_ID.")
    return chat_id.strip()


def _run_pg_dump(db_url: str, output_path: str) -> None:
    timeout_seconds = int(os.environ.get("PG_DUMP_TIMEOUT_SECONDS", DEFAULT_PG_DUMP_TIMEOUT_SECONDS))
    cmd = [
        "pg_dump",
        db_url,
        "--no-owner",
        "--no-privileges",
        "--format=custom",
        "--lock-wait-timeout=30s",
        "--file",
        output_path,
    ]
    env = os.environ.copy()
    env.setdefault("PGCONNECT_TIMEOUT", "15")
    print(f"Running pg_dump (timeout: {timeout_seconds}s)...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds, env=env)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"pg_dump timed out after {timeout_seconds}s")
    if result.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {result.stderr.strip() or result.stdout.strip()}")
    if not os.path.exists(output_path) or os.path.getsize(output_path) <= 0:
        raise RuntimeError("pg_dump completed but output file is missing or empty.")


def _send_to_telegram(token: str, chat_id: str, file_path: str, caption: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    print("Uploading backup to Telegram...")
    with open(file_path, "rb") as fh:
        files = {"document": fh}
        data = {"chat_id": chat_id, "caption": caption}
        resp = requests.post(
            url,
            files=files,
            data=data,
            timeout=(DEFAULT_TELEGRAM_CONNECT_TIMEOUT_SECONDS, DEFAULT_TELEGRAM_READ_TIMEOUT_SECONDS),
        )
    if resp.status_code != 200:
        raise RuntimeError(f"Telegram upload failed: {resp.status_code} {resp.text}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Backup PostgreSQL and send to Telegram channel.")
    parser.add_argument("--db-url", help="PostgreSQL connection string. If omitted, use DATABASE_URL env.")
    parser.add_argument("--label", default="railway", help="Label to include in backup caption/file name.")
    args = parser.parse_args()

    db_url = _get_db_url(args.db_url)
    token = _get_bot_token()
    chat_id = _get_chat_id()

    vn_tz = timezone(timedelta(hours=7))
    stamp = datetime.now(vn_tz).strftime("%Y%m%d_%H%M%S")
    label = args.label.strip() or "backup"
    file_name = f"{label}_backup_{stamp}.dump"

    with tempfile.TemporaryDirectory() as tmp:
        output_path = os.path.join(tmp, file_name)
        _run_pg_dump(db_url, output_path)
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"Backup size: {size_mb:.2f} MB")
        caption = f"DB backup {label} • {stamp} VN • {size_mb:.2f} MB"
        _send_to_telegram(token, chat_id, output_path, caption)

    print("Backup sent to Telegram successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
