import argparse
import json
import os
from pathlib import Path

import requests
from requests import HTTPError


def parse_args():
    p = argparse.ArgumentParser(description='Sync delivery proof photos from Railway API to local folder.')
    p.add_argument('--api', required=True, help='Base API URL, ex: https://your-app.up.railway.app')
    p.add_argument('--out', default='delivery_proofs', help='Output directory for downloaded photos')
    p.add_argument('--state-file', default='.delivery_sync_state.json', help='State file to store last synced order id')
    p.add_argument('--limit', type=int, default=200, help='Batch size per request')
    return p.parse_args()


def load_state(state_path: Path) -> int:
    if not state_path.exists():
        return 0
    try:
        data = json.loads(state_path.read_text(encoding='utf-8'))
        return int(data.get('last_order_id', 0))
    except Exception:
        return 0


def save_state(state_path: Path, last_order_id: int):
    state_path.write_text(json.dumps({'last_order_id': int(last_order_id)}, ensure_ascii=False, indent=2), encoding='utf-8')


def main():
    args = parse_args()
    api_base = args.api.rstrip('/')

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    state_path = Path(args.state_file).resolve()
    last_order_id = load_state(state_path)

    pending_url = f"{api_base}/delivery-proofs/pending"
    resp = requests.get(
        pending_url,
        params={'since_order_id': last_order_id, 'limit': args.limit},
        timeout=30,
    )
    resp.raise_for_status()

    payload = resp.json()
    rows = payload.get('data', []) if isinstance(payload, dict) else []
    if not rows:
        print('Không có ảnh mới để đồng bộ.')
        return

    max_seen = last_order_id
    downloaded = 0
    missing = 0

    for row in rows:
        if not isinstance(row, dict):
            continue
        order_id = int(row.get('order_id') or 0)
        file_name = str(row.get('file_name') or '').strip()
        download_url = str(row.get('download_url') or '').strip()
        if order_id <= 0 or not file_name or not download_url:
            continue

        if order_id > max_seen:
            max_seen = order_id

        target_name = f"order_{order_id}_{file_name}"
        target_path = out_dir / target_name
        if target_path.exists() and target_path.stat().st_size > 0:
            continue

        if download_url.startswith('http://') or download_url.startswith('https://'):
            photo_url = download_url
        else:
            photo_url = f"{api_base}{download_url}"

        try:
            r = requests.get(photo_url, timeout=60)
            r.raise_for_status()
            target_path.write_bytes(r.content)
            downloaded += 1
            print(f"Đã tải: #{order_id} -> {target_path.name}")
        except HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 404:
                missing += 1
                print(f"Bỏ qua #{order_id}: ảnh không còn trên server ({photo_url})")
                continue
            raise

    save_state(state_path, max_seen)
    print(f"Hoàn tất. Tải mới: {downloaded}, thiếu trên server: {missing}, last_order_id={max_seen}")


if __name__ == '__main__':
    main()
