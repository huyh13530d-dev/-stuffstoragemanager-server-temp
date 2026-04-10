import argparse
import os
import sys
import tempfile
import subprocess
from datetime import datetime, timezone

import requests


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
    cmd = [
        "pg_dump",
        db_url,
        "--no-owner",
        "--no-privileges",
        "--format=custom",
        "--file",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {result.stderr.strip() or result.stdout.strip()}")


def _send_to_telegram(token: str, chat_id: str, file_path: str, caption: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    with open(file_path, "rb") as fh:
        files = {"document": fh}
        data = {"chat_id": chat_id, "caption": caption}
        resp = requests.post(url, files=files, data=data, timeout=60)
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

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    label = args.label.strip() or "backup"
    file_name = f"{label}_backup_{stamp}.dump"

    with tempfile.TemporaryDirectory() as tmp:
        output_path = os.path.join(tmp, file_name)
        _run_pg_dump(db_url, output_path)
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        caption = f"DB backup {label} • {stamp} UTC • {size_mb:.2f} MB"
        _send_to_telegram(token, chat_id, output_path, caption)

    print("Backup sent to Telegram successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
