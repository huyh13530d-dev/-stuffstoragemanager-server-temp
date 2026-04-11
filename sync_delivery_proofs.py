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
    synced_by_order = {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        order_id = int(row.get('order_id') or 0)
        file_name = str(row.get('file_name') or '').strip()
        download_url = str(row.get('download_url') or '').strip()
        download_urls = [str(x).strip() for x in (row.get('download_urls') or []) if str(x).strip()]
        file_names = [str(x).strip() for x in (row.get('file_names') or []) if str(x).strip()]
        if order_id <= 0 or (not download_urls and (not file_name or not download_url)):
            continue

        if order_id > max_seen:
            max_seen = order_id

        urls_to_fetch = download_urls if download_urls else [download_url]
        names_to_use = file_names if file_names else [file_name]

        for i, url in enumerate(urls_to_fetch):
            name = names_to_use[i] if i < len(names_to_use) and names_to_use[i] else file_name
            if not url or not name:
                continue

            target_name = f"order_{order_id}_{name}"
            synced_by_order.setdefault(order_id, set()).add(target_name)
            target_path = out_dir / target_name
            if target_path.exists() and target_path.stat().st_size > 0:
                continue

            if url.startswith('http://') or url.startswith('https://'):
                photo_url = url
            else:
                photo_url = f"{api_base}{url}"

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

    ack_url = f"{api_base}/delivery-proofs/ack-local"
    for order_id, names in synced_by_order.items():
        if not names:
            continue
        has_all = True
        for name in names:
            f = out_dir / f"order_{order_id}_{name}"
            if (not f.exists()) or f.stat().st_size <= 0:
                has_all = False
                break
        if not has_all:
            continue

        try:
            ack_resp = requests.post(
                ack_url,
                json={'order_id': int(order_id), 'local_file_names': sorted(names)},
                timeout=30,
            )
            if ack_resp.status_code not in (200, 404):
                print(f"Ack local proof thất bại #{order_id}: {ack_resp.status_code} {ack_resp.text}")
        except Exception as e:
            print(f"Ack local proof lỗi #{order_id}: {e}")

    save_state(state_path, max_seen)
    print(f"Hoàn tất. Tải mới: {downloaded}, thiếu trên server: {missing}, last_order_id={max_seen}")


if __name__ == '__main__':
    main()
