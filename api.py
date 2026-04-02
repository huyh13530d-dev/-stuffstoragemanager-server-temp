from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import desc
from sqlalchemy.orm import Session
try:
    from database import SessionLocal, Product, Variant, Area, Order, OrderItem, Customer, DebtLog, engine, is_sqlite, Base
except ImportError:
    from backend.database import SessionLocal, Product, Variant, Area, Order, OrderItem, Customer, DebtLog, engine, is_sqlite, Base
from sqlalchemy import text
from datetime import datetime


class DebtLogCreate(BaseModel):
    change_amount: int
    note: str = ""
    created_at: Optional[str] = None  # format: YYYY-MM-DD HH:MM

class DebtLogUpdate(BaseModel):
    change_amount: int
    note: str = ""
    created_at: Optional[str] = None

class OrderDateUpdate(BaseModel):
    created_at: str  # YYYY-MM-DD HH:MM

app = FastAPI()

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
                    conn.execute(text("UPDATE customers SET area_id = :aid"), {"aid": int(default_area_id)})
                conn.commit()
            else:
                conn.execute(text("CREATE TABLE IF NOT EXISTS areas (id SERIAL PRIMARY KEY, name VARCHAR UNIQUE)"))
                conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS area_id INTEGER"))

                for n in seed_areas:
                    conn.execute(text("INSERT INTO areas (name) VALUES (:n) ON CONFLICT (name) DO NOTHING"), {"n": n})

                default_area_id = conn.execute(text("SELECT id FROM areas WHERE name = 'Chợ hàn' LIMIT 1")).scalar()
                if default_area_id is not None:
                    conn.execute(text("UPDATE customers SET area_id = :aid"), {"aid": int(default_area_id)})
                conn.commit()
    except Exception as e:
        print("Warning: ensure_area_schema_and_seed failed:", e)

ensure_status_column()
ensure_picker_note_column()
ensure_area_schema_and_seed()

def _get_default_area_id(db: Session):
    area = db.query(Area).filter(Area.name == "Chợ hàn").first()
    return area.id if area else None

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

class VariantUpdate(BaseModel):
    id: Optional[int] = None
    color: str
    size: str
    price: int
    stock: int

class ProductUpdate(BaseModel):
    name: str
    image_path: str
    variants: List[VariantUpdate]

class ProductCreate(BaseModel):
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

# --- API SẢN PHẨM ---
@app.get("/products")
def get_products(search: str = "", db: Session = Depends(get_db)):
    query = db.query(Product)
    if search:
        query = query.filter(Product.name.contains(search))
    
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
            "name": p.name, 
            "image": p.image_path, 
            "price_range": price_range,
            "variants": [{"id": v.id, "color": v.color, "size": v.size, "price": v.price, "stock": v.stock} for v in p.variants]
        })
    return results

@app.post("/products")
def create_product(p: ProductCreate, db: Session = Depends(get_db)):
    new_prod = Product(name=p.name, description=p.description, image_path=p.image_path)
    db.add(new_prod)
    db.commit()
    db.refresh(new_prod)
    
    for v in p.variants:
        db.add(Variant(product_id=new_prod.id, color=v.color, size=v.size, price=v.price, stock=v.stock))
    db.commit()
    return {"status": "ok"}

@app.put("/products/{product_id}")
def update_product(product_id: int, p_data: ProductUpdate, db: Session = Depends(get_db)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404)
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

# --- API KHÁCH HÀNG ---
@app.post("/customers")
def create_customer_manual(data: CustomerCreate, db: Session = Depends(get_db)):
    try:
        if db.query(Customer).filter(Customer.name == data.name).first():
            raise HTTPException(status_code=400, detail="Tên đã tồn tại!")
        
        default_area_id = _get_default_area_id(db)
        new_cust = Customer(name=data.name, phone=data.phone, debt=data.debt, area_id=default_area_id)
        db.add(new_cust)
        db.flush()
        
        if data.debt != 0:
            db.add(DebtLog(customer_id=new_cust.id, change_amount=data.debt, new_balance=data.debt, note="Khởi tạo thủ công", created_ts=int(datetime.utcnow().timestamp() * 1000)))
        
        db.commit()
        db.refresh(new_cust)
        return {"status": "created", "id": new_cust.id}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/customers")
def get_customers(db: Session = Depends(get_db)):
    custs = db.query(Customer).order_by(desc(Customer.id)).all()
    return [{"id": c.id, "name": c.name, "phone": c.phone, "debt": c.debt, "area_id": c.area_id} for c in custs]

@app.put("/customers/{cid}")
def update_customer_excel(cid: int, data: CustomerUpdate, db: Session = Depends(get_db)):
    cust = db.query(Customer).filter(Customer.id == cid).first()
    if not cust:
        raise HTTPException(status_code=404)
    
    diff = data.debt - cust.debt
    cust.name = data.name
    cust.phone = data.phone
    cust.debt = data.debt
    
    if diff != 0:
        db.add(DebtLog(customer_id=cust.id, change_amount=diff, new_balance=cust.debt, note="Điều chỉnh thủ công", created_ts=int(datetime.utcnow().timestamp() * 1000)))
        
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
        now = datetime.now() 

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
            from sqlalchemy import func
            customer = db.query(Customer).filter(func.lower(Customer.name) == func.lower(c_name)).first()
            
            if not customer:
                customer = Customer(name=c_name, phone=data.customer_phone, debt=0)
                db.add(customer)
                db.flush()
            
            customer.debt += total
        new_order = Order(
            total_amount=total,
            customer_name=customer.name if customer else "Khách lẻ",
            customer_id=customer.id if customer else None,
            is_draft=0,
            status='completed'
        )
        # set high-resolution timestamp
        new_order.created_ts = int(datetime.utcnow().timestamp() * 1000)
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
            customer = db.query(Customer).filter(Customer.name == c_name).first()
            if not customer:
                customer = Customer(name=c_name, phone=data.customer_phone, debt=0)
                db.add(customer)
                db.flush()
            customer.debt += total_new
        
        old_order.customer_name = c_name if c_name else "Khách lẻ"
        old_order.customer_id = customer.id if customer else None
        old_order.total_amount = total_new
        now_dt = datetime.now()
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
            from sqlalchemy import func
            customer = db.query(Customer).filter(func.lower(Customer.name) == func.lower(c_name)).first()

            if not customer:
                customer = Customer(name=c_name, phone=data.customer_phone, debt=0)
                db.add(customer)
                db.flush()

        new_order = Order(
            total_amount=total,
            customer_name=customer.name if customer else "Khách lẻ",
            customer_id=customer.id if customer else None,
            is_draft=1,
            status='pending'
        )
        new_order.created_ts = int(datetime.utcnow().timestamp() * 1000)
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
                "has_stock_conflict": has_stock_conflict,
                "items": items_list
            })

        return {"data": result, "count": len(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/orders/accepted")
def get_accepted_orders(db: Session = Depends(get_db)):
    """Get all ACCEPTED orders (status='accepted') for picker to confirm."""
    try:
        orders = db.query(Order).filter(Order.status == 'accepted').order_by(desc(Order.created_ts)).all()

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

        order.status = 'accepted'
        order.is_draft = 1  # keep is_draft consistent (still not finalized)
        db.commit()

        return {
            "status": "success",
            "message": f"Đơn #{order_id} đã được tiếp nhận, chuyển cho picker soạn hàng"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/orders/{order_id}/confirm")
def confirm_order(order_id: int, data: Optional[PickerConfirmRequest] = None, db: Session = Depends(get_db)):
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

        if order.status != 'accepted':
            raise HTTPException(status_code=400, detail="Chỉ có thể xác nhận đơn hàng đã được tiếp nhận")

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

        # Check real-time stock for picked quantities
        for item in order.items:
            picked_qty = int(picked_by_item_id.get(item.id, 0))
            if picked_qty <= 0 or not item.variant_id:
                continue
            var = db.query(Variant).filter(Variant.id == item.variant_id).first()
            if not var or int(var.stock or 0) < picked_qty:
                raise HTTPException(
                    status_code=400,
                    detail=f"SP {item.product_name} không đủ hàng thực tế ({int(var.stock or 0) if var else 0} tồn kho)"
                )

        delivered_total = 0
        shortage_parts = []

        # Apply stock and update order items to delivered qty
        for item in list(order.items):
            requested_qty = int(item.quantity or 0)
            picked_qty = int(picked_by_item_id.get(item.id, 0))

            if item.variant_id and picked_qty > 0:
                var = db.query(Variant).filter(Variant.id == item.variant_id).first()
                if var:
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
        if shortage_parts:
            order.picker_note = "Thiếu hàng: " + "; ".join(shortage_parts)

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