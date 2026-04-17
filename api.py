from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timezone, timedelta
from sqlalchemy import desc, func, Column, Integer, String, DateTime
from sqlalchemy.orm import Session
import os
import uuid
import json
import requests
import threading
try:
    from backend import database as _db
except ModuleNotFoundError:
    import database as _db

SessionLocal = _db.SessionLocal
Product = _db.Product
Variant = _db.Variant
Area = _db.Area
Order = _db.Order
OrderItem = _db.OrderItem
Customer = _db.Customer
DebtLog = _db.DebtLog
Employee = getattr(_db, "Employee", None)
engine = _db.engine
is_sqlite = _db.is_sqlite
Base = _db.Base
VN_TZ = timezone(timedelta(hours=7))


def _now_vn() -> datetime:
    return datetime.now(VN_TZ).replace(tzinfo=None)


def _now_vn_ts() -> int:
    return int(datetime.now(VN_TZ).timestamp() * 1000)


def _period_start_vn(days: int) -> Optional[datetime]:
    if days <= 0:
        return None
    now = _now_vn()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return day_start - timedelta(days=max(0, days - 1))


if Employee is None:
    class Employee(Base):
        __tablename__ = "employees"
        id = Column(Integer, primary_key=True, index=True)
        name = Column(String, index=True)
        phone = Column(String, default="")
        email = Column(String, default="")
        address = Column(String, default="")
        notes = Column(String, default="")
        role = Column(String, index=True)
        pin = Column(String, unique=True, index=True)
        is_active = Column(Integer, default=1)
        created_at = Column(DateTime, default=_now_vn)
from sqlalchemy import text


class DebtLogCreate(BaseModel):
    change_amount: int
    note: str = ""
    created_at: Optional[str] = None  # format: YYYY-MM-DD HH:MM
    actor_employee_id: Optional[int] = None

class DebtLogUpdate(BaseModel):
    change_amount: int
    note: str = ""
    created_at: Optional[str] = None

class OrderDateUpdate(BaseModel):
    created_at: str  # YYYY-MM-DD HH:MM


class DeliveryProofAckRequest(BaseModel):
    order_id: int
    local_file_names: List[str] = []

app = FastAPI()

_cors_raw = os.environ.get("CORS_ALLOWED_ORIGINS", "*").strip()
_cors_origins = [x.strip() for x in _cors_raw.split(",") if x.strip()]
_cors_allow_all = (not _cors_origins) or ("*" in _cors_origins)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _cors_allow_all else _cors_origins,
    allow_credentials=False if _cors_allow_all else True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_delivery_upload_dir = os.environ.get("DELIVERY_UPLOAD_DIR", "").strip()
if not _delivery_upload_dir:
    _delivery_upload_dir = "/tmp/delivery_proofs" if not is_sqlite else os.path.join(os.path.dirname(__file__), "delivery_proofs")
os.makedirs(_delivery_upload_dir, exist_ok=True)

_MAX_DELIVERY_PHOTO_BYTES = int(os.environ.get("MAX_DELIVERY_PHOTO_MB", "8")) * 1024 * 1024
_product_upload_dir = _delivery_upload_dir


def _save_delivery_photo_file(order_id: int, photo: UploadFile) -> str:
    filename = (photo.filename or "proof.jpg").strip()
    ext = os.path.splitext(filename)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".heic"):
        raise HTTPException(status_code=400, detail="Ảnh giao hàng phải là jpg/png/webp/heic")

    safe_name = f"order_{order_id}_{_now_vn().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}{ext}"
    abs_path = os.path.join(_delivery_upload_dir, safe_name)

    total = 0
    with open(abs_path, "wb") as out:
        while True:
            chunk = photo.file.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > _MAX_DELIVERY_PHOTO_BYTES:
                out.close()
                try:
                    os.remove(abs_path)
                except Exception:
                    pass
                raise HTTPException(status_code=400, detail=f"Ảnh vượt giới hạn {_MAX_DELIVERY_PHOTO_BYTES // (1024 * 1024)}MB")
            out.write(chunk)

    return f"/delivery-proofs/{safe_name}"


def _save_product_image_file(photo: UploadFile) -> str:
    filename = (photo.filename or "product.jpg").strip()
    ext = os.path.splitext(filename)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".heic", ".bmp"):
        raise HTTPException(status_code=400, detail="Ảnh sản phẩm phải là jpg/png/webp/heic/bmp")

    safe_name = f"product_{_now_vn().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}{ext}"
    abs_path = os.path.join(_product_upload_dir, safe_name)

    with open(abs_path, "wb") as out:
        while True:
            chunk = photo.file.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)

    return f"/product-images/{safe_name}"


def _parse_delivery_photo_paths(raw) -> list:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, str):
        t = raw.strip()
        if not t:
            return []
        if t.startswith('['):
            try:
                data = json.loads(t)
                if isinstance(data, list):
                    return [str(x).strip() for x in data if str(x).strip()]
            except Exception:
                pass
        if '|' in t:
            return [p.strip() for p in t.split('|') if p.strip()]
        return [t]
    return [str(raw).strip()]


def _normalize_picker_confirm_items(raw_items: list) -> List['PickerConfirmItem']:
    normalized = []
    for x in raw_items:
        if not isinstance(x, dict):
            continue
        normalized.append(PickerConfirmItem(
            order_item_id=x.get('order_item_id'),
            variant_id=x.get('variant_id'),
            picked_qty=int(x.get('picked_qty') or 0),
        ))
    return normalized

def ensure_created_ts_columns():
    if not is_sqlite:
        return  # Only needed for SQLite migration
    try:
        with engine.connect() as conn:
            for table in ("orders", "debt_logs"):
                info = conn.execute(text(f"PRAGMA table_info('{table}')")).fetchall()
                cols = [r[1] for r in info]
                if 'created_ts' not in cols:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN created_ts INTEGER"))
                conn.execute(text(f"UPDATE {table} SET created_ts = (CAST(strftime('%s', created_at) AS INTEGER) * 1000) + id WHERE created_ts IS NULL OR created_ts % 1000 == 0"))
            conn.commit()
    except Exception as e:
        print("Warning: ensure_created_ts_columns failed:", e)

def ensure_is_draft_column():
    """Add is_draft column to orders table if not exists"""
    try:
        with engine.connect() as conn:
            if is_sqlite:
                info = conn.execute(text("PRAGMA table_info('orders')")).fetchall()
                cols = [r[1] for r in info]
                if 'is_draft' not in cols:
                    conn.execute(text("ALTER TABLE orders ADD COLUMN is_draft INTEGER DEFAULT 0"))
                    conn.execute(text("UPDATE orders SET is_draft = 0 WHERE is_draft IS NULL"))
                    conn.commit()
            else:
                # PostgreSQL
                try:
                    conn.execute(text("ALTER TABLE orders ADD COLUMN is_draft INTEGER DEFAULT 0"))
                    conn.execute(text("UPDATE orders SET is_draft = 0 WHERE is_draft IS NULL"))
                    conn.commit()
                except:
                    conn.rollback()
                    # Column might already exist; still normalize NULL values
                    try:
                        conn.execute(text("UPDATE orders SET is_draft = 0 WHERE is_draft IS NULL"))
                        conn.commit()
                    except:
                        pass
    except Exception as e:
        print("Warning: ensure_is_draft_column failed:", e)

# Create tables (needed for first deploy on new DB)
Base.metadata.create_all(bind=engine)
ensure_created_ts_columns()
ensure_is_draft_column()

def ensure_status_column():
    """Migrate orders table from is_draft to status column"""
    try:
        with engine.connect() as conn:
            if is_sqlite:
                info = conn.execute(text("PRAGMA table_info('orders')")).fetchall()
                cols = [r[1] for r in info]
                if 'status' not in cols:
                    conn.execute(text("ALTER TABLE orders ADD COLUMN status VARCHAR DEFAULT 'completed'"))
                    conn.execute(text("UPDATE orders SET status = 'pending' WHERE is_draft = 1"))
                    conn.execute(text("UPDATE orders SET status = 'completed' WHERE is_draft = 0 OR is_draft IS NULL"))
                    conn.commit()
            else:
                # PostgreSQL: try adding column (fails silently if already exists)
                try:
                    conn.execute(text("ALTER TABLE orders ADD COLUMN status VARCHAR DEFAULT 'completed'"))
                    conn.commit()
                except Exception:
                    conn.rollback()
                # Migrate existing data from is_draft
                try:
                    conn.execute(text(
                        "UPDATE orders SET status = 'pending' WHERE is_draft = 1 AND (status IS NULL OR status = 'completed')"
                    ))
                    conn.execute(text(
                        "UPDATE orders SET status = 'completed' WHERE (is_draft = 0 OR is_draft IS NULL) AND status IS NULL"
                    ))
                    conn.commit()
                except Exception:
                    conn.rollback()
    except Exception as e:
        print("Warning: ensure_status_column failed:", e)

def ensure_picker_note_column():
    try:
        with engine.connect() as conn:
            if is_sqlite:
                info = conn.execute(text("PRAGMA table_info('orders')")).fetchall()
                cols = [r[1] for r in info]
                if 'picker_note' not in cols:
                    conn.execute(text("ALTER TABLE orders ADD COLUMN picker_note VARCHAR DEFAULT ''"))
                    conn.commit()
            else:
                try:
                    conn.execute(text("ALTER TABLE orders ADD COLUMN picker_note VARCHAR DEFAULT ''"))
                    conn.commit()
                except Exception:
                    conn.rollback()
    except Exception as e:
        print("Warning: ensure_picker_note_column failed:", e)

def ensure_telegram_columns():
    try:
        with engine.connect() as conn:
            if is_sqlite:
                info = conn.execute(text("PRAGMA table_info('orders')")).fetchall()
                cols = [r[1] for r in info]
                if 'telegram_file_id' not in cols:
                    conn.execute(text("ALTER TABLE orders ADD COLUMN telegram_file_id VARCHAR DEFAULT ''"))
                if 'telegram_message_id' not in cols:
                    conn.execute(text("ALTER TABLE orders ADD COLUMN telegram_message_id VARCHAR DEFAULT ''"))
                conn.commit()
            else:
                try:
                    conn.execute(text("ALTER TABLE orders ADD COLUMN IF NOT EXISTS telegram_file_id VARCHAR DEFAULT ''"))
                    conn.execute(text("ALTER TABLE orders ADD COLUMN IF NOT EXISTS telegram_message_id VARCHAR DEFAULT ''"))
                    conn.commit()
                except Exception:
                    conn.rollback()
    except Exception as e:
        print("Warning: ensure_telegram_columns failed:", e)

def ensure_area_schema_and_seed():
    seed_areas = ["Chợ đêm", "Chợ hàn", "Hội An", "Nha Trang"]
    try:
        with engine.connect() as conn:
            if is_sqlite:
                conn.execute(text("CREATE TABLE IF NOT EXISTS areas (id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)"))

                info = conn.execute(text("PRAGMA table_info('customers')")).fetchall()
                cols = [r[1] for r in info]
                if 'area_id' not in cols:
                    conn.execute(text("ALTER TABLE customers ADD COLUMN area_id INTEGER"))

                for n in seed_areas:
                    conn.execute(text("INSERT OR IGNORE INTO areas (name) VALUES (:n)"), {"n": n})

                default_area_id = conn.execute(text("SELECT id FROM areas WHERE name = 'Chợ hàn' LIMIT 1")).scalar()
                if default_area_id is not None:
                    conn.execute(text("UPDATE customers SET area_id = :aid WHERE area_id IS NULL"), {"aid": int(default_area_id)})
                conn.commit()
            else:
                conn.execute(text("CREATE TABLE IF NOT EXISTS areas (id SERIAL PRIMARY KEY, name VARCHAR UNIQUE)"))
                conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS area_id INTEGER"))

                for n in seed_areas:
                    conn.execute(text("INSERT INTO areas (name) VALUES (:n) ON CONFLICT (name) DO NOTHING"), {"n": n})

                default_area_id = conn.execute(text("SELECT id FROM areas WHERE name = 'Chợ hàn' LIMIT 1")).scalar()
                if default_area_id is not None:
                    conn.execute(text("UPDATE customers SET area_id = :aid WHERE area_id IS NULL"), {"aid": int(default_area_id)})
                conn.commit()
    except Exception as e:
        print("Warning: ensure_area_schema_and_seed failed:", e)

def ensure_employee_schema_and_seed():
    try:
        with engine.connect() as conn:
            if is_sqlite:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS employees (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR,
                        phone VARCHAR,
                        email VARCHAR DEFAULT '',
                        address VARCHAR DEFAULT '',
                        notes VARCHAR DEFAULT '',
                        role VARCHAR,
                        pin VARCHAR UNIQUE,
                        is_active INTEGER DEFAULT 1,
                        created_at DATETIME
                    )
                """))
                info = conn.execute(text("PRAGMA table_info('employees')")).fetchall()
                existing = [r[1] for r in info]
                if 'email' not in existing:
                    conn.execute(text("ALTER TABLE employees ADD COLUMN email VARCHAR DEFAULT ''"))
                if 'address' not in existing:
                    conn.execute(text("ALTER TABLE employees ADD COLUMN address VARCHAR DEFAULT ''"))
                if 'notes' not in existing:
                    conn.execute(text("ALTER TABLE employees ADD COLUMN notes VARCHAR DEFAULT ''"))
                if 'is_active' not in existing:
                    conn.execute(text("ALTER TABLE employees ADD COLUMN is_active INTEGER DEFAULT 1"))
                conn.execute(text("UPDATE employees SET email = '' WHERE email IS NULL"))
                conn.execute(text("UPDATE employees SET address = '' WHERE address IS NULL"))
                conn.execute(text("UPDATE employees SET notes = '' WHERE notes IS NULL"))
                conn.execute(text("UPDATE employees SET is_active = 1 WHERE is_active IS NULL"))
                conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_employees_pin ON employees(pin)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_employees_role ON employees(role)"))
                conn.execute(text("INSERT OR IGNORE INTO employees (name, phone, role, pin, created_at) VALUES ('Orderer mặc định', '', 'orderer', '0000', CURRENT_TIMESTAMP)"))
                conn.execute(text("INSERT OR IGNORE INTO employees (name, phone, role, pin, created_at) VALUES ('Picker mặc định', '', 'picker', '1111', CURRENT_TIMESTAMP)"))
                conn.commit()
            else:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS employees (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR,
                        phone VARCHAR,
                        email VARCHAR DEFAULT '',
                        address VARCHAR DEFAULT '',
                        notes VARCHAR DEFAULT '',
                        role VARCHAR,
                        pin VARCHAR UNIQUE,
                        is_active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """))
                conn.execute(text("ALTER TABLE employees ADD COLUMN IF NOT EXISTS email VARCHAR DEFAULT ''"))
                conn.execute(text("ALTER TABLE employees ADD COLUMN IF NOT EXISTS address VARCHAR DEFAULT ''"))
                conn.execute(text("ALTER TABLE employees ADD COLUMN IF NOT EXISTS notes VARCHAR DEFAULT ''"))
                conn.execute(text("ALTER TABLE employees ADD COLUMN IF NOT EXISTS is_active INTEGER DEFAULT 1"))
                conn.execute(text("UPDATE employees SET email = '' WHERE email IS NULL"))
                conn.execute(text("UPDATE employees SET address = '' WHERE address IS NULL"))
                conn.execute(text("UPDATE employees SET notes = '' WHERE notes IS NULL"))
                conn.execute(text("UPDATE employees SET is_active = 1 WHERE is_active IS NULL"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_employees_role ON employees(role)"))
                conn.execute(text("INSERT INTO employees (name, phone, role, pin) VALUES ('Orderer mặc định', '', 'orderer', '0000') ON CONFLICT (pin) DO NOTHING"))
                conn.execute(text("INSERT INTO employees (name, phone, role, pin) VALUES ('Picker mặc định', '', 'picker', '1111') ON CONFLICT (pin) DO NOTHING"))
                conn.commit()
    except Exception as e:
        print("Warning: ensure_employee_schema_and_seed failed:", e)

def ensure_order_flow_columns():
    cols = {
        'created_by_employee_id': 'INTEGER',
        'assigned_picker_id': 'INTEGER',
        'assigned_at': 'TIMESTAMP',
        'delivered_by_id': 'INTEGER',
        'delivered_at': 'TIMESTAMP',
        'delivery_photo_path': 'VARCHAR',
    }
    try:
        with engine.connect() as conn:
            if is_sqlite:
                info = conn.execute(text("PRAGMA table_info('orders')")).fetchall()
                existing = [r[1] for r in info]
                for c, t in cols.items():
                    if c not in existing:
                        conn.execute(text(f"ALTER TABLE orders ADD COLUMN {c} {t}"))
                conn.execute(text("UPDATE orders SET status = 'approved' WHERE status = 'accepted'"))
                conn.commit()
            else:
                for c, t in cols.items():
                    conn.execute(text(f"ALTER TABLE orders ADD COLUMN IF NOT EXISTS {c} {t}"))
                conn.execute(text("UPDATE orders SET status = 'approved' WHERE status = 'accepted'"))
                conn.commit()
    except Exception as e:
        print("Warning: ensure_order_flow_columns failed:", e)


def ensure_activity_tracking_columns():
    try:
        with engine.connect() as conn:
            if is_sqlite:
                info = conn.execute(text("PRAGMA table_info('debt_logs')")).fetchall()
                existing = [r[1] for r in info]
                if 'actor_employee_id' not in existing:
                    conn.execute(text("ALTER TABLE debt_logs ADD COLUMN actor_employee_id INTEGER"))
                conn.commit()
            else:
                conn.execute(text("ALTER TABLE debt_logs ADD COLUMN IF NOT EXISTS actor_employee_id INTEGER"))
                conn.commit()
    except Exception as e:
        print("Warning: ensure_activity_tracking_columns failed:", e)

ensure_status_column()
ensure_picker_note_column()
ensure_telegram_columns()
ensure_area_schema_and_seed()
ensure_employee_schema_and_seed()
ensure_order_flow_columns()
ensure_activity_tracking_columns()


def _get_telegram_config():
    token = os.environ.get('TELEGRAM_DB_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_DB_CHAT_ID') or os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat_id:
        return None
    return token, chat_id


def _send_photo_to_telegram(photo_path: str, caption: str) -> dict:
    config = _get_telegram_config()
    if not config:
        return {}
    token, chat_id = config
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    try:
        with open(photo_path, 'rb') as f:
            files = {'photo': f}
            data = {'chat_id': chat_id, 'caption': caption}
            r = requests.post(url, files=files, data=data, timeout=30)
        if r.status_code == 200:
            return r.json().get('result') or {}
        print("Warning: telegram send failed:", r.status_code, r.text)
    except Exception as e:
        print("Warning: telegram send failed:", e)
    return {}


def _send_photos_to_telegram(photo_paths: list, caption: str) -> list:
    config = _get_telegram_config()
    if not config:
        return []
    token, chat_id = config
    paths = [p for p in photo_paths if p]
    if not paths:
        return []
    if len(paths) == 1:
        result = _send_photo_to_telegram(paths[0], caption)
        return [result] if result else []

    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    files = {}
    media = []
    opened_files = []
    try:
        for i, path in enumerate(paths):
            key = f'file{i}'
            f = open(path, 'rb')
            opened_files.append(f)
            files[key] = f
            payload = {'type': 'photo', 'media': f'attach://{key}'}
            if i == 0 and caption:
                payload['caption'] = caption
            media.append(payload)

        data = {'chat_id': chat_id, 'media': json.dumps(media, ensure_ascii=False)}
        r = requests.post(url, files=files, data=data, timeout=60)
        if r.status_code == 200:
            result = r.json().get('result')
            return result if isinstance(result, list) else []
        print("Warning: telegram sendMediaGroup failed:", r.status_code, r.text)
        fallback_results = []
        for i, p in enumerate(paths):
            msg = _send_photo_to_telegram(p, caption if i == 0 else '')
            if msg:
                fallback_results.append(msg)
        return fallback_results
    except Exception as e:
        print("Warning: telegram sendMediaGroup failed:", e)
        fallback_results = []
        for i, p in enumerate(paths):
            msg = _send_photo_to_telegram(p, caption if i == 0 else '')
            if msg:
                fallback_results.append(msg)
        return fallback_results
    finally:
        for f in opened_files:
            try:
                f.close()
            except Exception:
                pass
    return []


def _send_product_image_to_telegram(photo_path: str, caption: str) -> None:
    token = os.environ.get('TELEGRAM_DB_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_DB_CHAT_ID')
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    try:
        with open(photo_path, 'rb') as f:
            files = {'photo': f}
            data = {'chat_id': chat_id, 'caption': caption}
            requests.post(url, files=files, data=data, timeout=30)
    except Exception as e:
        print("Warning: telegram product image send failed:", e)
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    try:
        with open(photo_path, 'rb') as f:
            files = {'photo': f}
            data = {'chat_id': chat_id, 'caption': caption}
            r = requests.post(url, files=files, data=data, timeout=30)
        if r.status_code == 200:
            return r.json().get('result') or {}
    except Exception as e:
        print("Warning: telegram send failed:", e)
    return {}


def _order_status_label_vi(status: str) -> str:
    s = (status or '').strip().lower()
    if s == 'pending':
        return 'Đợi duyệt'
    if s == 'approved':
        return 'Đã duyệt'
    if s in ('assigned', 'accepted'):
        return 'Đã nhận'
    if s == 'completed':
        return 'Hoàn thành'
    return (status or '').upper()


def _send_delivery_backup_async(order_id: int, abs_photo_paths: list, caption: str):
    try:
        telegram_results = _send_photos_to_telegram(abs_photo_paths, caption)
        if not telegram_results:
            return
        db = SessionLocal()
        try:
            order = db.query(Order).filter(Order.id == order_id).first()
            if not order:
                return
            for telegram_result in telegram_results:
                if not telegram_result:
                    continue
                photos = telegram_result.get('photo') or []
                if photos:
                    order.telegram_file_id = photos[-1].get('file_id', '') or ''
                order.telegram_message_id = str(telegram_result.get('message_id') or '')
            db.commit()
        finally:
            db.close()
    except Exception as e:
        print('Warning: telegram backup failed:', e)

def _get_default_area_id(db: Session):
    area = db.query(Area).filter(Area.name == "Chợ hàn").first()
    if area:
        return area.id
    first_area = db.query(Area).order_by(Area.id).first()
    return first_area.id if first_area else None

def _generate_unique_pin(db: Session):
    import random
    for _ in range(50):
        pin = f"{random.randint(0, 9999):04d}"
        existed = db.query(Employee).filter(Employee.pin == pin).first()
        if not existed:
            return pin
    raise HTTPException(status_code=500, detail="Không tạo được PIN duy nhất")


def _normalize_employee_pin(raw_pin: str) -> str:
    pin = (raw_pin or '').strip()
    if not pin:
        raise HTTPException(status_code=400, detail='PIN không được để trống')
    if not pin.isdigit():
        raise HTTPException(status_code=400, detail='PIN chỉ được chứa chữ số')
    if len(pin) < 4 or len(pin) > 8:
        raise HTTPException(status_code=400, detail='PIN phải có 4-8 chữ số')
    return pin


def _serialize_employee(e: Employee, delivered_count: int = 0):
    last_delivered = None
    if getattr(e, 'delivered_orders', None):
        delivered_dates = [o.delivered_at for o in e.delivered_orders if getattr(o, 'delivered_at', None) is not None]
        if delivered_dates:
            last_delivered = max(delivered_dates)
    return {
        'id': e.id,
        'name': e.name,
        'phone': e.phone,
        'email': (getattr(e, 'email', '') or ''),
        'address': (getattr(e, 'address', '') or ''),
        'notes': (getattr(e, 'notes', '') or ''),
        'role': e.role,
        'pin': e.pin,
        'is_active': int(getattr(e, 'is_active', 1) or 0),
        'created_at': e.created_at.strftime('%Y-%m-%d %H:%M') if e.created_at else '',
        'delivered_count': int(delivered_count or 0),
        'last_delivered_at': last_delivered.strftime('%Y-%m-%d %H:%M') if last_delivered else '',
    }

def _serialize_order(o: Order):
    items_list = []
    calc_qty = 0
    if o.items:
        for i in o.items:
            q = int(i.quantity or 0)
            calc_qty += q
            items_list.append({
                "order_item_id": i.id,
                "product_name": i.product_name,
                "variant_id": i.variant_id,
                "variant_info": i.variant_info,
                "quantity": q,
                "price": int(i.price or 0),
                "current_stock": None,
                "enough_stock": True,
            })
    delivery_paths = _parse_delivery_photo_paths(o.delivery_photo_path)
    return {
        "id": o.id,
        "created_at": o.created_at.strftime("%Y-%m-%d %H:%M") if o.created_at else "",
        "customer_name": o.customer_name or "Khách lẻ",
        "customer_id": o.customer_id,
        "total_amount": int(o.total_amount or 0),
        "total_qty": calc_qty,
        "status": o.status,
        "picker_note": (o.picker_note or ""),
        "created_by_employee_id": o.created_by_employee_id,
        "created_by_employee_name": (o.created_by_employee.name if getattr(o, 'created_by_employee', None) else ""),
        "assigned_picker_id": o.assigned_picker_id,
        "assigned_picker_name": (o.assigned_picker.name if o.assigned_picker else ""),
        "assigned_at": o.assigned_at.strftime("%Y-%m-%d %H:%M") if o.assigned_at else "",
        "delivered_by_id": o.delivered_by_id,
        "delivered_by_name": (o.delivered_by.name if o.delivered_by else ""),
        "delivered_at": o.delivered_at.strftime("%Y-%m-%d %H:%M") if o.delivered_at else "",
        "delivery_photo_path": (o.delivery_photo_path or ""),
        "delivery_photo_paths": delivery_paths,
        "items": items_list,
    }

# --- DEPENDENCY: KẾT NỐI DB ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- MODELS ---
class CustomerCreate(BaseModel):
    name: str
    phone: str = ""
    debt: int = 0
    area_id: int

class AreaCreate(BaseModel):
    name: str

class AreaUpdate(BaseModel):
    name: str

class VariantUpdate(BaseModel):
    id: Optional[int] = None
    color: str
    size: str
    price: int
    stock: int

class ProductUpdate(BaseModel):
    code: str = ""
    name: str
    image_path: str
    variants: List[VariantUpdate]

class ProductCreate(BaseModel):
    code: str = ""
    name: str
    description: str
    image_path: str
    variants: List[VariantUpdate]

class CartItem(BaseModel):
    variant_id: int
    quantity: int
    price: int
    product_name: str
    color: str
    size: str

class CheckoutRequest(BaseModel):
    customer_name: str
    customer_phone: str = ""
    employee_id: Optional[int] = None
    cart: List[CartItem]

class PickerConfirmItem(BaseModel):
    order_item_id: Optional[int] = None
    variant_id: Optional[int] = None
    picked_qty: int = 0

class PickerConfirmRequest(BaseModel):
    items: List[PickerConfirmItem] = []

class CustomerUpdate(BaseModel):
    name: str
    phone: str
    debt: int
    area_id: int

class EmployeeCreate(BaseModel):
    name: str
    phone: str = ""
    email: str = ""
    address: str = ""
    notes: str = ""
    role: str

class EmployeeUpdate(BaseModel):
    name: str
    phone: str = ""
    email: str = ""
    address: str = ""
    notes: str = ""
    role: str
    pin: Optional[str] = None
    is_active: int = 1

class PinLoginRequest(BaseModel):
    pin: str
    requested_role: str

class ReceiveOrderRequest(BaseModel):
    picker_id: int

class DeliverOrderRequest(BaseModel):
    picker_id: int
    photo_path: str
    items: List[PickerConfirmItem] = []
    picker_note: str = ""


def _deliver_order_internal(order_id: int, picker_id: int, photo_paths, items: List[PickerConfirmItem], db: Session, picker_note: str = ""):
    normalized_paths = _parse_delivery_photo_paths(photo_paths)
    if not normalized_paths:
        raise HTTPException(status_code=400, detail='Bắt buộc chụp ảnh xác nhận giao hàng')

    abs_photo_paths = []
    normalized_photo_paths = []
    for raw_path in normalized_paths:
        if raw_path.startswith('/delivery-proofs/'):
            file_name = os.path.basename(raw_path)
            abs_path = os.path.join(_delivery_upload_dir, file_name)
            if not os.path.exists(abs_path):
                raise HTTPException(status_code=400, detail='Ảnh xác nhận không tồn tại trên server, vui lòng chụp và gửi lại')
            normalized_photo_paths.append(f'/delivery-proofs/{file_name}')
            abs_photo_paths.append(abs_path)
        elif raw_path.startswith('http://') or raw_path.startswith('https://'):
            normalized_photo_paths.append(raw_path)
        else:
            raise HTTPException(status_code=400, detail='Ứng dụng mobile cũ chưa hỗ trợ upload ảnh. Vui lòng cập nhật app mobile mới nhất')

    picker = db.query(Employee).filter(Employee.id == picker_id).first()
    picker_role = (picker.role.strip().lower() if picker and picker.role else '')
    if not picker or picker_role not in ('picker', 'manager'):
        raise HTTPException(status_code=400, detail='Picker không hợp lệ')

    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail='Hóa đơn không tồn tại')
    if order.status != 'assigned':
        raise HTTPException(status_code=400, detail='Chỉ giao đơn đã nhận')
    if order.assigned_picker_id != picker.id:
        raise HTTPException(status_code=403, detail='Bạn không phải người đã nhận đơn này')

    proxy = PickerConfirmRequest(items=items)
    confirm_result = confirm_order(order_id, proxy if items else None, db, picker_note=picker_note)
    order.delivered_by_id = picker.id
    order.delivered_at = _now_vn()
    if len(normalized_photo_paths) > 1:
        order.delivery_photo_path = json.dumps(normalized_photo_paths, ensure_ascii=False)
    else:
        order.delivery_photo_path = normalized_photo_paths[0] if normalized_photo_paths else ''
    db.commit()

    if abs_photo_paths:
        caption_parts = [
            f"Đơn #{order.id} • {order.customer_name or 'Khách lẻ'}",
            f"Picker: {picker.name}",
            order.delivered_at.strftime('%Y-%m-%d %H:%M') if order.delivered_at else '',
        ]
        if order.picker_note:
            caption_parts.append(f"Ghi chú: {order.picker_note}")
        caption = "\n".join([p for p in caption_parts if p])
        threading.Thread(
            target=_send_delivery_backup_async,
            args=(order.id, abs_photo_paths, caption),
            daemon=True,
        ).start()
    return confirm_result

# --- API NHÂN VIÊN / PHÂN QUYỀN ---
@app.post('/auth/pin-login')
def pin_login(data: PinLoginRequest, db: Session = Depends(get_db)):
    req_role = data.requested_role.strip().lower()
    if req_role not in ('orderer', 'picker', 'manager'):
        raise HTTPException(status_code=400, detail='Vai trò không hợp lệ')
    emp = db.query(Employee).filter(Employee.pin == data.pin.strip()).first()
    if not emp:
        raise HTTPException(status_code=401, detail='PIN không đúng')
    if int(getattr(emp, 'is_active', 1) or 0) != 1:
        raise HTTPException(status_code=403, detail='Tài khoản nhân viên đang bị khóa')
    if emp.role != req_role:
        raise HTTPException(status_code=403, detail='PIN không thuộc vai trò này')
    return {
        'id': emp.id,
        'name': emp.name,
        'phone': emp.phone,
        'role': emp.role,
    }

@app.get('/employees')
def get_employees(db: Session = Depends(get_db)):
    emps = db.query(Employee).order_by(desc(Employee.id)).all()
    delivered_stats = dict(
        db.query(Order.delivered_by_id, func.count(Order.id))
        .filter(Order.delivered_by_id.isnot(None), Order.status == 'completed')
        .group_by(Order.delivered_by_id)
        .all()
    )
    return [_serialize_employee(e, delivered_count=delivered_stats.get(e.id, 0)) for e in emps]

@app.post('/employees')
def create_employee(data: EmployeeCreate, db: Session = Depends(get_db)):
    role = data.role.strip().lower()
    if role not in ('orderer', 'picker', 'manager'):
        raise HTTPException(status_code=400, detail='Vai trò không hợp lệ')
    pin = _generate_unique_pin(db)
    emp = Employee(
        name=data.name.strip(),
        phone=data.phone.strip(),
        email=data.email.strip(),
        address=data.address.strip(),
        notes=data.notes.strip(),
        role=role,
        pin=pin,
        is_active=1,
    )
    db.add(emp)
    db.commit()
    db.refresh(emp)
    return {'status': 'created', 'id': emp.id, 'pin': emp.pin, 'employee': _serialize_employee(emp)}

@app.put('/employees/{emp_id}')
def update_employee(emp_id: int, data: EmployeeUpdate, db: Session = Depends(get_db)):
    emp = db.query(Employee).filter(Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail='Nhân viên không tồn tại')
    role = data.role.strip().lower()
    if role not in ('orderer', 'picker', 'manager'):
        raise HTTPException(status_code=400, detail='Vai trò không hợp lệ')
    emp.name = data.name.strip()
    emp.phone = data.phone.strip()
    emp.email = data.email.strip()
    emp.address = data.address.strip()
    emp.notes = data.notes.strip()
    emp.role = role
    emp.is_active = 1 if int(data.is_active or 0) == 1 else 0
    if data.pin is not None:
        normalized_pin = _normalize_employee_pin(data.pin)
        existed = db.query(Employee).filter(Employee.pin == normalized_pin, Employee.id != emp_id).first()
        if existed:
            raise HTTPException(status_code=400, detail='PIN đã tồn tại, vui lòng chọn PIN khác')
        emp.pin = normalized_pin
    db.commit()
    db.refresh(emp)
    return {'status': 'updated', 'employee': _serialize_employee(emp)}

@app.delete('/employees/{emp_id}')
def delete_employee(emp_id: int, db: Session = Depends(get_db)):
    emp = db.query(Employee).filter(Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail='Nhân viên không tồn tại')
    db.delete(emp)
    db.commit()
    return {'status': 'deleted'}


@app.get('/employees/{emp_id}/deliveries')
def get_employee_deliveries(emp_id: int, q: str = '', days: int = 0, limit: int = 200, db: Session = Depends(get_db)):
    emp = db.query(Employee).filter(Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail='Nhân viên không tồn tại')

    lim = 200 if limit <= 0 else min(limit, 500)
    query = db.query(Order).filter(
        Order.delivered_by_id == emp_id,
        Order.status == 'completed',
    )

    keyword = q.strip()
    if keyword:
        if keyword.isdigit():
            query = query.filter((Order.id == int(keyword)) | (Order.customer_name.ilike(f"%{keyword}%")))
        else:
            query = query.filter(Order.customer_name.ilike(f"%{keyword}%"))

    period_start = _period_start_vn(days)
    if period_start is not None:
        query = query.filter(Order.delivered_at.isnot(None), Order.delivered_at >= period_start)

    orders = query.order_by(desc(Order.delivered_at), desc(Order.id)).limit(lim).all()
    return {
        'employee': _serialize_employee(emp),
        'data': [_serialize_order(o) for o in orders],
        'count': len(orders),
    }


@app.get('/employees/{emp_id}/activities')
def get_employee_activities(emp_id: int, q: str = '', days: int = 0, limit: int = 300, db: Session = Depends(get_db)):
    emp = db.query(Employee).filter(Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail='Nhân viên không tồn tại')

    lim = 300 if limit <= 0 else min(limit, 1000)
    period_start = _period_start_vn(days)
    keyword = q.strip().lower()

    order_query = db.query(Order).filter(Order.created_by_employee_id == emp_id)
    if period_start is not None:
        order_query = order_query.filter(Order.created_at >= period_start)
    if keyword:
        if keyword.isdigit():
            order_query = order_query.filter((Order.id == int(keyword)) | (Order.customer_name.ilike(f"%{keyword}%")))
        else:
            order_query = order_query.filter(Order.customer_name.ilike(f"%{keyword}%"))
    orders = order_query.order_by(desc(Order.created_ts), desc(Order.id)).limit(lim).all()

    log_query = db.query(DebtLog, Customer.name).outerjoin(Customer, Customer.id == DebtLog.customer_id).filter(DebtLog.actor_employee_id == emp_id)
    if period_start is not None:
        log_query = log_query.filter(DebtLog.created_at >= period_start)
    if keyword:
        if keyword.isdigit():
            log_query = log_query.filter((DebtLog.id == int(keyword)) | (Customer.name.ilike(f"%{keyword}%")) | (DebtLog.note.ilike(f"%{keyword}%")))
        else:
            log_query = log_query.filter((Customer.name.ilike(f"%{keyword}%")) | (DebtLog.note.ilike(f"%{keyword}%")))
    logs = log_query.order_by(desc(DebtLog.created_ts), desc(DebtLog.id)).limit(lim).all()

    activities = []
    for o in orders:
        ts = int(o.created_ts or 0)
        if ts <= 0 and o.created_at:
            ts = int(o.created_at.timestamp() * 1000)
        activities.append({
            'type': 'ORDER',
            'sort_ts': ts,
            'date': o.created_at.strftime('%Y-%m-%d %H:%M') if o.created_at else '',
            'title': f"Đơn #{o.id} • {o.customer_name or 'Khách lẻ'}",
            'subtitle': f"{_order_status_label_vi(o.status)} • SL {sum((i.quantity or 0) for i in (o.items or []))}",
            'amount': int(o.total_amount or 0),
            'order': _serialize_order(o),
        })

    for row in logs:
        log, customer_name = row
        ts = int(log.created_ts or 0)
        if ts <= 0 and log.created_at:
            ts = int(log.created_at.timestamp() * 1000)
        change = int(log.change_amount or 0)
        is_collect = change < 0
        activities.append({
            'type': 'DEBT_LOG',
            'sort_ts': ts,
            'date': log.created_at.strftime('%Y-%m-%d %H:%M') if log.created_at else '',
            'title': ('Thu tiền' if is_collect else 'Điều chỉnh công nợ') + f" • {customer_name or 'Khách'}",
            'subtitle': (log.note or '').strip(),
            'amount': change,
            'log_id': log.id,
            'customer_id': log.customer_id,
        })

    activities.sort(key=lambda x: int(x.get('sort_ts') or 0), reverse=True)
    activities = activities[:lim]
    return {
        'employee': _serialize_employee(emp),
        'data': activities,
        'count': len(activities),
    }

# --- API SẢN PHẨM ---
@app.get("/products")
def get_products(search: str = "", db: Session = Depends(get_db)):
    query = db.query(Product)
    if search:
        s = f"%{search}%"
        query = query.filter((Product.name.ilike(s)) | (Product.code.ilike(s)))
    
    products = query.order_by(desc(Product.id)).all()
    results = []
    for p in products:
        prices = [v.price for v in p.variants]
        price_range = "Hết hàng"
        if prices:
            min_p, max_p = min(prices), max(prices)
            price_range = f"{min_p:,} - {max_p:,}" if min_p != max_p else f"{min_p:,}"
        
        results.append({
            "id": p.id, 
            "code": p.code or p.name,
            "name": p.name, 
            "image": p.image_path, 
            "price_range": price_range,
            "variants": [{"id": v.id, "color": v.color, "size": v.size, "price": v.price, "stock": v.stock} for v in p.variants]
        })
    return results

@app.post("/products")
def create_product(p: ProductCreate, db: Session = Depends(get_db)):
    code = (p.code or "").strip() or p.name
    new_prod = Product(code=code, name=p.name, description=p.description, image_path=p.image_path)
    db.add(new_prod)
    db.commit()
    db.refresh(new_prod)
    
    for v in p.variants:
        db.add(Variant(product_id=new_prod.id, color=v.color, size=v.size, price=v.price, stock=v.stock))
    db.commit()
    return {"status": "ok"}


@app.post("/product-images/upload")
def upload_product_image(file: UploadFile = File(...)):
    path = _save_product_image_file(file)
    abs_path = None
    if path.startswith('/product-images/'):
        abs_path = os.path.join(_product_upload_dir, os.path.basename(path))
    if abs_path:
        try:
            caption = f"Ảnh sản phẩm • {_now_vn().strftime('%Y-%m-%d %H:%M')}"
            _send_product_image_to_telegram(abs_path, caption)
        except Exception as e:
            print("Warning: product image telegram backup failed:", e)
    return {"path": path}


@app.get('/product-images/{file_name}')
def get_product_image_file(file_name: str):
    safe_name = os.path.basename(file_name)
    if safe_name != file_name:
        raise HTTPException(status_code=400, detail='Tên file không hợp lệ')
    abs_path = os.path.join(_product_upload_dir, safe_name)
    if not os.path.exists(abs_path):
        raise HTTPException(status_code=404, detail='Không tìm thấy ảnh')
    return FileResponse(abs_path)

@app.put("/products/{product_id}")
def update_product(product_id: int, p_data: ProductUpdate, db: Session = Depends(get_db)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404)
    product.code = (p_data.code or "").strip() or p_data.name
    product.name = p_data.name
    product.image_path = p_data.image_path
    
    # Logic update variants (giữ nguyên)
    current_variants_map = {v.id: v for v in product.variants}
    current_ids = set(current_variants_map.keys())
    incoming_ids = {v.id for v in p_data.variants if v.id is not None}
    to_delete_ids = current_ids - incoming_ids
    for vid in to_delete_ids:
        variant_to_delete = current_variants_map.get(vid)
        if variant_to_delete:
            db.delete(variant_to_delete)
    for v_data in p_data.variants:
        if v_data.id and v_data.id in current_ids:
            var = current_variants_map[v_data.id] 
            var.color = v_data.color
            var.size = v_data.size
            var.price = v_data.price
            var.stock = v_data.stock
        else:
            new_var = Variant(
                product_id=product.id, 
                color=v_data.color, 
                size=v_data.size, 
                price=v_data.price, 
                stock=v_data.stock
            )
            db.add(new_var)
    db.commit()
    return {"status": "updated"}

@app.delete("/products/{product_id}")
def delete_product(product_id: int, db: Session = Depends(get_db)):
    p = db.query(Product).filter(Product.id == product_id).first()
    if p:
        db.query(Variant).filter(Variant.product_id == product_id).delete()
        db.delete(p)
        db.commit()
    return {"status": "deleted"}

@app.get("/areas")
def get_areas(db: Session = Depends(get_db)):
    areas = db.query(Area).order_by(Area.id).all()
    result = []
    for a in areas:
        custs = db.query(Customer).filter(Customer.area_id == a.id).all()
        result.append({
            "id": a.id,
            "name": a.name,
            "customer_count": len(custs),
            "total_debt": sum(int(c.debt or 0) for c in custs),
        })
    return result

@app.post("/areas")
def create_area(data: AreaCreate, db: Session = Depends(get_db)):
    name = data.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Tên khu vực không hợp lệ")
    existed = db.query(Area).filter(func.lower(Area.name) == func.lower(name)).first()
    if existed:
        raise HTTPException(status_code=400, detail="Khu vực đã tồn tại")
    area = Area(name=name)
    db.add(area)
    db.commit()
    db.refresh(area)
    return {"status": "created", "id": area.id}

@app.put("/areas/{area_id}")
def update_area(area_id: int, data: AreaUpdate, db: Session = Depends(get_db)):
    area = db.query(Area).filter(Area.id == area_id).first()
    if not area:
        raise HTTPException(status_code=404, detail="Khu vực không tồn tại")
    name = data.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Tên khu vực không hợp lệ")
    dup = db.query(Area).filter(func.lower(Area.name) == func.lower(name), Area.id != area_id).first()
    if dup:
        raise HTTPException(status_code=400, detail="Tên khu vực đã tồn tại")
    area.name = name
    db.commit()
    return {"status": "updated"}

@app.delete("/areas/{area_id}")
def delete_area(area_id: int, db: Session = Depends(get_db)):
    area = db.query(Area).filter(Area.id == area_id).first()
    if not area:
        raise HTTPException(status_code=404, detail="Khu vực không tồn tại")

    target_id = _get_default_area_id(db)
    if target_id == area_id:
        fallback = db.query(Area).filter(Area.id != area_id).order_by(Area.id).first()
        target_id = fallback.id if fallback else None
    if target_id is None:
        raise HTTPException(status_code=400, detail="Không thể xóa khu vực duy nhất")

    db.query(Customer).filter(Customer.area_id == area_id).update({Customer.area_id: target_id})
    db.delete(area)
    db.commit()
    return {"status": "deleted", "moved_to_area_id": target_id}

# --- API KHÁCH HÀNG ---
@app.post("/customers")
def create_customer_manual(data: CustomerCreate, db: Session = Depends(get_db)):
    try:
        if db.query(Customer).filter(Customer.name == data.name).first():
            raise HTTPException(status_code=400, detail="Tên đã tồn tại!")

        area = db.query(Area).filter(Area.id == data.area_id).first()
        if not area:
            raise HTTPException(status_code=400, detail="Khu vực không tồn tại")

        new_cust = Customer(name=data.name, phone=data.phone, debt=data.debt, area_id=data.area_id)
        db.add(new_cust)
        db.flush()
        
        if data.debt != 0:
            db.add(DebtLog(customer_id=new_cust.id, change_amount=data.debt, new_balance=data.debt, note="Khởi tạo thủ công", created_ts=_now_vn_ts()))
        
        db.commit()
        db.refresh(new_cust)
        return {"status": "created", "id": new_cust.id}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/orders/{order_id}/cancel")
def cancel_order(order_id: int, db: Session = Depends(get_db)):
    """
    Staff cancels an order in pending/approved/assigned states.
    The order is deleted and should disappear from picker queues.
    """
    try:
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            raise HTTPException(status_code=404, detail="Hóa đơn không tồn tại")

        if order.status not in ("pending", "approved", "assigned"):
            raise HTTPException(status_code=400, detail="Chỉ hủy đơn ở trạng thái chờ duyệt/đã duyệt/đã nhận")

        assigned_picker_id = order.assigned_picker_id
        db.query(OrderItem).filter(OrderItem.order_id == order_id).delete()
        db.delete(order)
        db.commit()

        return {
            "status": "success",
            "message": f"Đơn #{order_id} đã bị hủy",
            "assigned_picker_id": assigned_picker_id,
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/checkout/desktop-dispatch")
def checkout_desktop_dispatch(data: CheckoutRequest, db: Session = Depends(get_db)):
    """
    Desktop dispatch flow:
    - Skip manager approval step
    - Send order directly to picker queue (status='approved')
    - Stock/debt are still applied only when picker delivers/confirms
    """
    try:
        total = sum([item.quantity * item.price for item in data.cart])

        c_name = data.customer_name.strip()
        customer = None

        if c_name:
            customer = db.query(Customer).filter(func.lower(Customer.name) == func.lower(c_name)).first()

            if not customer:
                customer = Customer(name=c_name, phone=data.customer_phone, debt=0, area_id=_get_default_area_id(db))
                db.add(customer)
                db.flush()

        new_order = Order(
            total_amount=total,
            customer_name=customer.name if customer else "Khách lẻ",
            customer_id=customer.id if customer else None,
            is_draft=1,
            status='approved'
        )
        new_order.created_ts = _now_vn_ts()
        db.add(new_order)
        db.flush()

        for item in data.cart:
            db.add(OrderItem(
                order_id=new_order.id,
                product_name=item.product_name,
                variant_id=item.variant_id,
                variant_info=f"{item.color}-{item.size}",
                quantity=item.quantity,
                price=item.price
            ))

        db.commit()
        return {
            "status": "success",
            "order_id": new_order.id,
            "message": "Đơn desktop đã gửi picker, chờ nhận xử lý"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/customers")
def get_customers(db: Session = Depends(get_db)):
    custs = db.query(Customer).order_by(desc(Customer.id)).all()
    return [{
        "id": c.id,
        "name": c.name,
        "phone": c.phone,
        "debt": c.debt,
        "area_id": c.area_id,
        "area_name": (c.area_rel.name if c.area_rel else ""),
    } for c in custs]

@app.put("/customers/{cid}")
def update_customer_excel(cid: int, data: CustomerUpdate, db: Session = Depends(get_db)):
    cust = db.query(Customer).filter(Customer.id == cid).first()
    if not cust:
        raise HTTPException(status_code=404)

    area = db.query(Area).filter(Area.id == data.area_id).first()
    if not area:
        raise HTTPException(status_code=400, detail="Khu vực không tồn tại")
    
    diff = data.debt - cust.debt
    cust.name = data.name
    cust.phone = data.phone
    cust.debt = data.debt
    cust.area_id = data.area_id
    
    if diff != 0:
        db.add(DebtLog(customer_id=cust.id, change_amount=diff, new_balance=cust.debt, note="Điều chỉnh thủ công", created_ts=_now_vn_ts()))
        
    db.commit()
    return {"status": "ok"}

def _delete_order_with_business_logic(order: Order, db: Session):
    """
    Delete an order with the same business behavior as invoice deletion:
    - completed: restore stock + revert customer debt
    - pending/accepted: just remove order items + order (no stock/debt applied yet)
    """
    if order.status == 'completed':
        for item in order.items:
            if item.variant_id:
                var = db.query(Variant).filter(Variant.id == item.variant_id).first()
                if var:
                    var.stock = (var.stock or 0) + (item.quantity or 0)

        if order.customer_id:
            cust = db.query(Customer).filter(Customer.id == order.customer_id).first()
            if cust and order.total_amount:
                try:
                    cust.debt = (cust.debt or 0) - int(order.total_amount or 0)
                except Exception:
                    cust.debt = (cust.debt or 0) - (order.total_amount or 0)

    db.query(OrderItem).filter(OrderItem.order_id == order.id).delete()
    db.delete(order)

@app.delete("/customers/{customer_id}")
def delete_customer(customer_id: int, db: Session = Depends(get_db)):
    customer = db.query(Customer).filter(Customer.id == customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Khách hàng không tồn tại")
    try:
        orders = db.query(Order).filter(Order.customer_id == customer_id).all()
        for order in orders:
            _delete_order_with_business_logic(order, db)

        db.delete(customer)
        db.commit()
        return {"detail": "Đã xóa khách hàng và toàn bộ lịch sử đơn hàng liên quan"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/customers/{cid}/history")
def get_customer_history(cid: int, db: Session = Depends(get_db)):
    orders = db.query(Order).filter(Order.customer_id == cid).all()
    history = []
    
    for o in orders:
        # Lấy timestamp thực tế để sắp xếp
        ts = int(o.created_ts) if (hasattr(o, 'created_ts') and o.created_ts) else int(o.created_at.timestamp() * 1000)
        
        # CHUẨN HÓA DỮ LIỆU ITEMS: Phải có variant_id thì UI mới sửa được
        items_detail = []
        for i in o.items:
            items_detail.append({
                "product_name": i.product_name,
                "variant_id": i.variant_id,  # TRƯỜNG QUAN TRỌNG NHẤT
                "variant_info": i.variant_info,
                "quantity": i.quantity,
                "price": i.price
            })
            
        history.append({
            "type": "ORDER",
            "date": o.created_at.strftime("%Y-%m-%d %H:%M"),
            "sort_ts": ts,
            "desc": f"Xuất đơn hàng #{o.id}",
            "amount": o.total_amount,
            "data": {
                "id": o.id, # ID đơn hàng
                "customer": o.customer_name,
                "customer_name": o.customer_name,
                "date": o.created_at.strftime("%d/%m %H:%M"),
                "total_money": o.total_amount,
                "total_qty": sum(i.quantity for i in o.items),
                "delivery_photo_path": (o.delivery_photo_path or ""),
                "delivery_photo_paths": _parse_delivery_photo_paths(o.delivery_photo_path),
                "items": items_detail # Danh sách item đầy đủ ID
            }
        })
    
    logs = db.query(DebtLog).filter(DebtLog.customer_id == cid).all()
    for l in logs:
        ts_log = int(l.created_ts) if (hasattr(l, 'created_ts') and l.created_ts) else int(l.created_at.timestamp() * 1000)
        history.append({
            "type": "LOG",
            "date": l.created_at.strftime("%Y-%m-%d %H:%M"),
            "sort_ts": ts_log,
            "desc": l.note,
            "amount": l.change_amount,
            "data": None,
            "log_id": l.id
        })
        
    return sorted(history, key=lambda x: x['sort_ts'], reverse=True)


@app.post("/customers/{cid}/history")
def create_debt_log(cid: int, data: DebtLogCreate, db: Session = Depends(get_db)):
    cust = db.query(Customer).filter(Customer.id == cid).first()
    if not cust:
        raise HTTPException(status_code=404, detail="Khách hàng không tồn tại")
    try:
        amt = data.change_amount
        now = _now_vn()
        actor_id = data.actor_employee_id
        if actor_id is not None:
            actor = db.query(Employee).filter(Employee.id == actor_id).first()
            if not actor:
                raise HTTPException(status_code=400, detail="Nhân viên thực hiện không tồn tại")

        display_dt = now
        if data.created_at:
            try:
                display_dt = datetime.strptime(data.created_at, "%Y-%m-%d %H:%M")
            except:
                pass

        cust.debt += amt

        ts_ms = int(now.timestamp() * 1000)
        
        db.add(DebtLog(
            customer_id=cust.id, 
            actor_employee_id=actor_id,
            change_amount=amt, 
            new_balance=cust.debt, 
            note=data.note, 
            created_at=display_dt, 
            created_ts=ts_ms
        ))
        db.commit()
        return {"status": "created"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/customers/{cid}/history/{log_id}")
def update_debt_log(cid: int, log_id: int, data: DebtLogUpdate, db: Session = Depends(get_db)):
    cust = db.query(Customer).filter(Customer.id == cid).first()
    if not cust:
        raise HTTPException(status_code=404, detail="Khách hàng không tồn tại")
    log = db.query(DebtLog).filter(DebtLog.id == log_id, DebtLog.customer_id == cid).first()
    if not log:
        raise HTTPException(status_code=404, detail="Log không tồn tại")
    try:
        old_amt = log.change_amount
        new_amt = data.change_amount
        diff = new_amt - old_amt
        cust.debt += diff
        log.change_amount = new_amt
        log.note = data.note
        if data.created_at:
            new_dt = datetime.strptime(data.created_at, "%Y-%m-%d %H:%M")
            log.created_at = new_dt
            log.created_ts = int(new_dt.timestamp() * 1000)
        log.new_balance = cust.debt
        db.commit()
        return {"status": "updated"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/customers/{cid}/history/{log_id}")
def delete_debt_log(cid: int, log_id: int, db: Session = Depends(get_db)):
    cust = db.query(Customer).filter(Customer.id == cid).first()
    if not cust:
        raise HTTPException(status_code=404, detail="Khách hàng không tồn tại")
    log = db.query(DebtLog).filter(DebtLog.id == log_id, DebtLog.customer_id == cid).first()
    if not log:
        raise HTTPException(status_code=404, detail="Log không tồn tại")
    try:
        cust.debt -= log.change_amount
        db.delete(log)
        db.commit()
        return {"status": "deleted"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/orders/{order_id}/date")
def update_order_date(order_id: int, data: OrderDateUpdate, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Đơn hàng không tồn tại")
    try:
        new_dt = datetime.strptime(data.created_at, "%Y-%m-%d %H:%M")
        order.created_at = new_dt
        order.created_ts = int(new_dt.timestamp() * 1000)
        db.commit()
        return {"status": "updated"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# --- API CHECKOUT & ORDERS ---
@app.post("/checkout")
def checkout(data: CheckoutRequest, db: Session = Depends(get_db)):
    try:
        total = sum([item.quantity * item.price for item in data.cart])
        for item in data.cart:
            variant = db.query(Variant).filter(Variant.id == item.variant_id).first()
            if not variant or variant.stock < item.quantity:
                raise HTTPException(status_code=400, detail=f"SP {item.product_name} thiếu hàng")
            variant.stock -= item.quantity

        c_name = data.customer_name.strip() # Cắt khoảng trắng đầu cuối
        customer = None
        
        if c_name:
            customer = db.query(Customer).filter(func.lower(Customer.name) == func.lower(c_name)).first()
            
            if not customer:
                customer = Customer(name=c_name, phone=data.customer_phone, debt=0, area_id=_get_default_area_id(db))
                db.add(customer)
                db.flush()
            
            customer.debt += total
        new_order = Order(
            total_amount=total,
            customer_name=customer.name if customer else "Khách lẻ",
            customer_id=customer.id if customer else None,
            is_draft=0,
            status='completed',
            created_by_employee_id=data.employee_id,
        )
        # set high-resolution timestamp
        new_order.created_ts = _now_vn_ts()
        db.add(new_order)
        db.flush()
        
        for item in data.cart:
            db.add(OrderItem(
                order_id=new_order.id, 
                product_name=item.product_name, 
                variant_id=item.variant_id,
                variant_info=f"{item.color}-{item.size}", 
                quantity=item.quantity, 
                price=item.price
            ))
            
        db.commit()
        return {"status": "success"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/orders/{order_id}")
def update_order_api(order_id: int, data: CheckoutRequest, db: Session = Depends(get_db)):
    try:
        old_order = db.query(Order).filter(Order.id == order_id).first()
        if not old_order:
            raise HTTPException(status_code=404, detail="Không tìm thấy đơn hàng")
        if old_order.status != 'completed':
            raise HTTPException(status_code=400, detail="Chỉ có thể sửa đơn hàng đã hoàn thành")
        
        # 1. Hoàn tác đơn cũ
        for item in old_order.items:
            if item.variant_id:
                var = db.query(Variant).filter(Variant.id == item.variant_id).first()
                if var:
                    var.stock += item.quantity
        
        if old_order.customer_id:
            cust = db.query(Customer).filter(Customer.id == old_order.customer_id).first()
            if cust:
                cust.debt -= old_order.total_amount

        # 2. Xóa chi tiết đơn cũ
        db.query(OrderItem).filter(OrderItem.order_id == order_id).delete()

        # 3. Áp dụng đơn mới
        total_new = sum([item.quantity * item.price for item in data.cart])
        
        for item in data.cart:
            variant = db.query(Variant).filter(Variant.id == item.variant_id).first()
            if not variant or variant.stock < item.quantity:
                raise HTTPException(status_code=400, detail=f"SP {item.product_name} không đủ hàng để cập nhật")
            variant.stock -= item.quantity

        c_name = data.customer_name.strip()
        customer = None
        if c_name:
            customer = db.query(Customer).filter(func.lower(Customer.name) == func.lower(c_name)).first()
            if not customer:
                customer = Customer(name=c_name, phone=data.customer_phone, debt=0, area_id=_get_default_area_id(db))
                db.add(customer)
                db.flush()
            customer.debt += total_new
        
        old_order.customer_name = c_name if c_name else "Khách lẻ"
        old_order.customer_id = customer.id if customer else None
        old_order.total_amount = total_new
        now_dt = _now_vn()
        old_order.created_at = now_dt
        old_order.created_ts = int(now_dt.timestamp() * 1000)

        for item in data.cart:
            db.add(OrderItem(
                order_id=old_order.id,
                product_name=item.product_name,
                variant_id=item.variant_id,
                variant_info=f"{item.color}-{item.size}",
                quantity=item.quantity,
                price=item.price
            ))

        db.commit()
        return {"status": "updated"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders")
def get_orders(page: int = 1, limit: int = 20, db: Session = Depends(get_db)):
    try:
        skip = (page - 1) * limit
        total = db.query(Order).filter(Order.status == 'completed').count()
        orders = db.query(Order).filter(Order.status == 'completed').order_by(desc(Order.id)).offset(skip).limit(limit).all()

        result = []
        for o in orders:
            items_list = []
            calc_qty = 0
            if o.items:
                for i in o.items:
                    q = int(i.quantity or 0)
                    calc_qty += q
                    items_list.append({
                        "order_item_id": i.id,
                        "product_name": i.product_name,
                        "variant_id": i.variant_id,
                        "variant_info": i.variant_info,
                        "quantity": q,
                        "price": int(i.price or 0)
                    })

            amt = getattr(o, "total_amount", getattr(o, "total_money", 0))
            created_at_str = ""
            if o.created_at:
                if hasattr(o.created_at, "strftime"):
                    created_at_str = o.created_at.strftime("%Y-%m-%d %H:%M")
                else:
                    created_at_str = str(o.created_at)

            result.append({
                "id": o.id,
                "created_at": created_at_str,
                "customer_name": o.customer_name or "Khách lẻ",
                "total_amount": int(amt or 0),
                "total_qty": calc_qty,
                "picker_note": (o.picker_note or ""),
                "items": items_list
            })
        return {"data": result, "total": total, "page": page, "limit": limit}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi tải hóa đơn: {e}")

@app.delete("/orders/{order_id}")
def delete_order_only(order_id: int, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Hóa đơn không tồn tại")
    if order.status != 'completed':
        raise HTTPException(status_code=400, detail="Chỉ có thể xóa/hoàn tác đơn hàng đã hoàn thành")
    try:
        _delete_order_with_business_logic(order, db)
        db.commit()
        return {"detail": "Đã xóa hóa đơn và hoàn tác kho + công nợ"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ───────────────────────────────────────────────────────────────
# NEW: DRAFT ORDERS & APPROVAL SYSTEM (FOR STAFF APP)
# ───────────────────────────────────────────────────────────────

@app.post("/checkout/draft")
def checkout_draft(data: CheckoutRequest, db: Session = Depends(get_db)):
    """
    Create a PENDING order from orderer app.
    Stock and debt are NOT applied yet — waiting for staff accept → picker confirm.
    """
    try:
        total = sum([item.quantity * item.price for item in data.cart])

        c_name = data.customer_name.strip()
        customer = None

        if c_name:
            customer = db.query(Customer).filter(func.lower(Customer.name) == func.lower(c_name)).first()

            if not customer:
                customer = Customer(name=c_name, phone=data.customer_phone, debt=0, area_id=_get_default_area_id(db))
                db.add(customer)
                db.flush()

        new_order = Order(
            total_amount=total,
            customer_name=customer.name if customer else "Khách lẻ",
            customer_id=customer.id if customer else None,
            is_draft=1,
            status='pending',
            created_by_employee_id=data.employee_id,
        )
        new_order.created_ts = _now_vn_ts()
        db.add(new_order)
        db.flush()

        for item in data.cart:
            db.add(OrderItem(
                order_id=new_order.id,
                product_name=item.product_name,
                variant_id=item.variant_id,
                variant_info=f"{item.color}-{item.size}",
                quantity=item.quantity,
                price=item.price
            ))

        db.commit()
        return {
            "status": "success",
            "order_id": new_order.id,
            "message": "Đơn hàng đã gửi chờ staff tiếp nhận"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/orders/pending")
def get_pending_orders(db: Session = Depends(get_db)):
    """Get all PENDING orders (status='pending') for staff to accept/reject."""
    try:
        orders = db.query(Order).filter(Order.status == 'pending').order_by(desc(Order.created_ts)).all()

        result = []
        for o in orders:
            items_list = []
            calc_qty = 0
            has_stock_conflict = False
            if o.items:
                for i in o.items:
                    calc_qty += i.quantity
                    current_stock = None
                    enough_stock = True
                    if i.variant_id:
                        var = db.query(Variant).filter(Variant.id == i.variant_id).first()
                        current_stock = int(var.stock or 0) if var else 0
                        enough_stock = current_stock >= int(i.quantity or 0)
                        if not enough_stock:
                            has_stock_conflict = True
                    items_list.append({
                        "order_item_id": i.id,
                        "product_name": i.product_name,
                        "variant_id": i.variant_id,
                        "variant_info": i.variant_info,
                        "quantity": i.quantity,
                        "price": i.price,
                        "current_stock": current_stock,
                        "enough_stock": enough_stock,
                    })

            result.append({
                "id": o.id,
                "created_at": o.created_at.strftime("%Y-%m-%d %H:%M") if o.created_at else "",
                "customer_name": o.customer_name or "Khách lẻ",
                "customer_id": o.customer_id,
                "total_amount": o.total_amount,
                "total_qty": calc_qty,
                "status": o.status,
                "picker_note": (o.picker_note or ""),
                "created_by_employee_id": o.created_by_employee_id,
                "created_by_employee_name": (o.created_by_employee.name if o.created_by_employee else ""),
                "has_stock_conflict": has_stock_conflict,
                "items": items_list
            })

        return {"data": result, "count": len(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/orders/approved')
def get_approved_orders(db: Session = Depends(get_db)):
    try:
        orders = db.query(Order).filter(Order.status == 'approved').order_by(desc(Order.created_ts)).all()
        return {'data': [_serialize_order(o) for o in orders], 'count': len(orders)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/orders/{order_id}/receive')
def receive_order(order_id: int, data: ReceiveOrderRequest, db: Session = Depends(get_db)):
    try:
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            raise HTTPException(status_code=404, detail='Hóa đơn không tồn tại')
        if order.status != 'approved':
            raise HTTPException(status_code=400, detail='Chỉ nhận đơn ở trạng thái đã duyệt')
        picker = db.query(Employee).filter(Employee.id == data.picker_id).first()
        picker_role = (picker.role.strip().lower() if picker and picker.role else '')
        if not picker or picker_role not in ('picker', 'manager'):
            raise HTTPException(status_code=400, detail='Picker không hợp lệ')
        order.status = 'assigned'
        order.assigned_picker_id = picker.id
        order.assigned_at = _now_vn()
        db.commit()
        return {'status': 'success', 'message': f'Đã nhận đơn #{order_id}'}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/orders/assigned')
def get_assigned_orders(picker_id: int, db: Session = Depends(get_db)):
    try:
        orders = db.query(Order).filter(Order.status == 'assigned', Order.assigned_picker_id == picker_id).order_by(desc(Order.created_ts)).all()
        return {'data': [_serialize_order(o) for o in orders], 'count': len(orders)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/orders/{order_id}/deliver')
def deliver_order(order_id: int, data: DeliverOrderRequest, db: Session = Depends(get_db)):
    return _deliver_order_internal(order_id, data.picker_id, [data.photo_path], data.items, db, picker_note=data.picker_note)


@app.put('/orders/{order_id}/deliver-with-photo')
async def deliver_order_with_photo(
    order_id: int,
    picker_id: int = Form(...),
    items_json: str = Form('[]'),
    picker_note: str = Form(''),
    photo: Optional[UploadFile] = File(None),
    photos: Optional[List[UploadFile]] = File(None),
    db: Session = Depends(get_db),
):
    try:
        raw_items = json.loads(items_json or '[]')
        if not isinstance(raw_items, list):
            raw_items = []
    except Exception:
        raise HTTPException(status_code=400, detail='Dữ liệu items không hợp lệ')

    normalized_items = _normalize_picker_confirm_items(raw_items)
    upload_files = []
    if photos:
        upload_files = [p for p in photos if p is not None]
    elif photo is not None:
        upload_files = [photo]
    if not upload_files:
        raise HTTPException(status_code=400, detail='Thiếu ảnh xác nhận giao hàng')

    photo_paths = [_save_delivery_photo_file(order_id, f) for f in upload_files]
    return _deliver_order_internal(order_id, picker_id, photo_paths, normalized_items, db, picker_note=picker_note)


@app.get('/delivery-proofs/pending')
def list_pending_delivery_proofs(since_order_id: int = 0, limit: int = 200, db: Session = Depends(get_db)):
    lim = 200 if limit <= 0 else min(limit, 500)
    q = db.query(Order).filter(
        Order.status == 'completed',
        Order.delivery_photo_path.isnot(None),
        Order.delivery_photo_path != '',
        Order.id > since_order_id,
    ).order_by(Order.id.asc()).limit(lim)

    orders = q.all()
    data = []
    for o in orders:
        rels = _parse_delivery_photo_paths(o.delivery_photo_path)
        rels = [r for r in rels if r and not r.startswith('local://')]
        if not rels:
            continue
        rel = (rels[0] if rels else '').strip()
        file_name = os.path.basename(rel)
        download_urls = []
        file_names = []
        for r in rels:
            name = os.path.basename(r)
            file_names.append(name)
            download_urls.append(r if r.startswith('/delivery-proofs/') else f'/delivery-proofs/{name}')
        data.append({
            'order_id': o.id,
            'delivered_at': o.delivered_at.strftime('%Y-%m-%d %H:%M') if o.delivered_at else '',
            'customer_name': o.customer_name or 'Khách lẻ',
            'photo_path': rel,
            'photo_paths': rels,
            'file_name': file_name,
            'file_names': file_names,
            'download_url': rel if rel.startswith('/delivery-proofs/') else f'/delivery-proofs/{file_name}',
            'download_urls': download_urls,
        })
    return {'data': data, 'count': len(data)}


@app.post('/delivery-proofs/ack-local')
def ack_delivery_proof_local(data: DeliveryProofAckRequest, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == data.order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail='Đơn hàng không tồn tại')

    current_paths = _parse_delivery_photo_paths(order.delivery_photo_path)
    if not current_paths:
        return {'status': 'noop', 'message': 'Không có path ảnh để đồng bộ'}

    local_names = [os.path.basename(str(x).strip()) for x in (data.local_file_names or []) if str(x).strip()]
    if not local_names:
        local_names = [os.path.basename(p) for p in current_paths if str(p).strip()]

    local_paths = [f'local://delivery_proofs/{name}' for name in local_names if name]
    if local_paths:
        order.delivery_photo_path = json.dumps(local_paths, ensure_ascii=False) if len(local_paths) > 1 else local_paths[0]

    removed = 0
    for p in current_paths:
        raw = str(p).strip()
        if not raw or raw.startswith('local://'):
            continue
        if raw.startswith('http://') or raw.startswith('https://'):
            continue
        abs_path = os.path.join(_delivery_upload_dir, os.path.basename(raw))
        if os.path.exists(abs_path):
            try:
                os.remove(abs_path)
                removed += 1
            except Exception:
                pass

    db.commit()
    return {
        'status': 'ok',
        'order_id': order.id,
        'removed_remote_files': removed,
        'local_paths': local_paths,
    }


@app.get('/delivery-proofs/{file_name}')
def get_delivery_proof_file(file_name: str):
    safe_name = os.path.basename(file_name)
    if safe_name != file_name:
        raise HTTPException(status_code=400, detail='Tên file không hợp lệ')
    abs_path = os.path.join(_delivery_upload_dir, safe_name)
    if not os.path.exists(abs_path):
        raise HTTPException(status_code=404, detail='Không tìm thấy ảnh')
    return FileResponse(abs_path)


@app.get('/orders/management')
def get_orders_management(limit: int = 200, db: Session = Depends(get_db)):
    try:
        orders = db.query(Order).order_by(desc(Order.created_ts)).limit(limit).all()
        return {'data': [_serialize_order(o) for o in orders], 'count': len(orders)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/orders/accepted")
def get_accepted_orders(db: Session = Depends(get_db)):
    """Backward-compat endpoint: now returns APPROVED orders for picker receive step."""
    try:
        orders = db.query(Order).filter(Order.status == 'approved').order_by(desc(Order.created_ts)).all()

        result = []
        for o in orders:
            items_list = []
            calc_qty = 0
            has_stock_conflict = False
            if o.items:
                for i in o.items:
                    calc_qty += i.quantity
                    current_stock = None
                    enough_stock = True
                    if i.variant_id:
                        var = db.query(Variant).filter(Variant.id == i.variant_id).first()
                        current_stock = int(var.stock or 0) if var else 0
                        enough_stock = current_stock >= int(i.quantity or 0)
                        if not enough_stock:
                            has_stock_conflict = True
                    items_list.append({
                        "order_item_id": i.id,
                        "product_name": i.product_name,
                        "variant_id": i.variant_id,
                        "variant_info": i.variant_info,
                        "quantity": i.quantity,
                        "price": i.price,
                        "current_stock": current_stock,
                        "enough_stock": enough_stock,
                    })

            result.append({
                "id": o.id,
                "created_at": o.created_at.strftime("%Y-%m-%d %H:%M") if o.created_at else "",
                "customer_name": o.customer_name or "Khách lẻ",
                "customer_id": o.customer_id,
                "total_amount": o.total_amount,
                "total_qty": calc_qty,
                "status": o.status,
                "picker_note": (o.picker_note or ""),
                "has_stock_conflict": has_stock_conflict,
                "items": items_list
            })

        return {"data": result, "count": len(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/orders/{order_id}/status")
def get_order_status(order_id: int, db: Session = Depends(get_db)):
    """Lightweight status check for orderer polling. Returns 404 if deleted/rejected."""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Đơn hàng không tồn tại hoặc đã bị từ chối")
    return {"id": order_id, "status": order.status, "picker_note": (order.picker_note or "")}


@app.put("/orders/{order_id}/approve")
def approve_order(order_id: int, db: Session = Depends(get_db)):
    """
    Staff accepts a PENDING order → moves to ACCEPTED for picker.
    Stock and debt are NOT changed yet (picker confirm handles that).
    """
    try:
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            raise HTTPException(status_code=404, detail="Hóa đơn không tồn tại")

        if order.status != 'pending':
            raise HTTPException(status_code=400, detail="Chỉ có thể tiếp nhận đơn đang chờ duyệt")

        order.status = 'approved'
        order.is_draft = 1  # keep is_draft consistent (still not finalized)
        db.commit()

        return {
            "status": "success",
            "message": f"Đơn #{order_id} đã được duyệt, chờ picker nhận"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/orders/{order_id}/confirm")
def confirm_order(order_id: int, data: Optional[PickerConfirmRequest] = None, db: Session = Depends(get_db), picker_note: str = ""):
    """
    Picker confirms delivery → applies to database:
    - Deduct stock from variants (by picked quantities)
    - Add debt to customer by delivered amount
    - Mark status='completed'
    - Save picker_note if there are shortages
    """
    try:
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            raise HTTPException(status_code=404, detail="Hóa đơn không tồn tại")

        if order.status not in ('assigned', 'accepted'):
            raise HTTPException(status_code=400, detail="Chỉ có thể xác nhận đơn hàng đã được nhận")

        requested_map = {}
        for item in order.items:
            requested_map[item.id] = item

        picked_by_item_id = {}
        if data and data.items:
            for x in data.items:
                target_item = None
                if x.order_item_id and x.order_item_id in requested_map:
                    target_item = requested_map[x.order_item_id]
                elif x.variant_id is not None:
                    target_item = next((it for it in order.items if it.variant_id == x.variant_id), None)
                if not target_item:
                    continue
                q = int(x.picked_qty or 0)
                if q < 0:
                    q = 0
                if q > target_item.quantity:
                    q = int(target_item.quantity)
                picked_by_item_id[target_item.id] = q

        # default = full quantity (old behavior) if picker doesn't send custom picked quantities
        for item in order.items:
            if item.id not in picked_by_item_id:
                picked_by_item_id[item.id] = int(item.quantity or 0)

        delivered_total = 0
        shortage_parts = []
        stock_mismatch_parts = []

        # Apply stock and update order items to delivered qty
        for item in list(order.items):
            requested_qty = int(item.quantity or 0)
            picked_qty = int(picked_by_item_id.get(item.id, 0))

            if item.variant_id and picked_qty > 0:
                var = db.query(Variant).filter(Variant.id == item.variant_id).first()
                if var:
                    available = int(var.stock or 0)
                    if picked_qty > available:
                        stock_mismatch_parts.append(f"{item.product_name} (kho thiếu {picked_qty - available})")
                        var.stock = 0
                    else:
                        var.stock -= picked_qty

            if picked_qty < requested_qty:
                shortage_parts.append(f"{item.product_name} ({picked_qty}/{requested_qty})")

            if picked_qty <= 0:
                db.delete(item)
            else:
                item.quantity = picked_qty
                delivered_total += int(item.price or 0) * picked_qty

        # Add customer debt by delivered amount only
        if order.customer_id and delivered_total > 0:
            customer = db.query(Customer).filter(Customer.id == order.customer_id).first()
            if customer:
                customer.debt += delivered_total

        order.total_amount = delivered_total
        order.picker_note = ""
        shortage_note = ""
        combined_shortages = shortage_parts + stock_mismatch_parts
        if combined_shortages:
            shortage_note = "Thiếu hàng: " + "; ".join(combined_shortages)

        manual_note = (picker_note or "").strip()
        if shortage_note and manual_note:
            order.picker_note = f"{shortage_note} | Ghi chú picker: {manual_note}"
        elif shortage_note:
            order.picker_note = shortage_note
        elif manual_note:
            order.picker_note = manual_note

        order.status = 'completed'
        order.is_draft = 0
        db.commit()

        return {
            "status": "success",
            "message": f"Đơn #{order_id} đã xác nhận hoàn thành",
            "partial": len(shortage_parts) > 0,
            "picker_note": order.picker_note,
            "delivered_total": delivered_total,
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/orders/{order_id}/reject")
def reject_order(order_id: int, db: Session = Depends(get_db)):
    """
    Staff rejects a PENDING order — deletes it completely.
    No stock/debt changes (nothing was applied yet).
    """
    try:
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            raise HTTPException(status_code=404, detail="Hóa đơn không tồn tại")

        if order.status != 'pending':
            raise HTTPException(status_code=400, detail="Chỉ có thể từ chối đơn đang chờ duyệt")

        db.query(OrderItem).filter(OrderItem.order_id == order_id).delete()
        db.delete(order)
        db.commit()

        return {"status": "success", "message": f"Đơn #{order_id} đã bị từ chối và xóa"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))