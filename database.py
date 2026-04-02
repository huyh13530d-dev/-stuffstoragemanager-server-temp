import os
import sys
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from datetime import datetime

def get_db_path():
    if getattr(sys, 'frozen', False):
        application_path = os.path.dirname(sys.executable)
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if os.path.basename(current_dir) == 'backend':
            application_path = os.path.dirname(current_dir)
        else:
            application_path = current_dir
    return os.path.join(application_path, "shop.db")

# Railway / cloud: set DATABASE_URL env var (e.g. postgresql://...)
# Local desktop: falls back to SQLite
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    db_path = get_db_path()
    DATABASE_URL = f"sqlite:///{db_path}"

is_sqlite = DATABASE_URL.startswith("sqlite")

Base = declarative_base()
connect_args = {"check_same_thread": False} if is_sqlite else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 1. Product & Variant (Giữ nguyên)
class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, index=True, default="")
    name = Column(String, index=True)
    description = Column(String, default="")
    image_path = Column(String, default="") 
    variants = relationship("Variant", back_populates="product", cascade="all, delete-orphan")

class Variant(Base):
    __tablename__ = "variants"
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"))
    color = Column(String)
    size = Column(String)
    price = Column(Integer)
    stock = Column(Integer)
    product = relationship("Product", back_populates="variants")

class Area(Base):
    __tablename__ = "areas"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    customers = relationship("Customer", back_populates="area_rel")

# 2. Customer & Debt (MỚI)
class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, unique=True) # Tên là định danh duy nhất để gợi ý
    phone = Column(String, default="")
    debt = Column(Integer, default=0) # Tổng nợ hiện tại
    area_id = Column(Integer, ForeignKey("areas.id"), nullable=True)
    
    area_rel = relationship("Area", back_populates="customers")
    logs = relationship("DebtLog", back_populates="customer", cascade="all, delete-orphan")
    orders = relationship("Order", back_populates="customer_rel")

class DebtLog(Base):
    __tablename__ = "debt_logs"
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    change_amount = Column(Integer) # Số tiền thay đổi (+ hoặc -)
    new_balance = Column(Integer) # Dư nợ sau khi đổi
    note = Column(String) # Lý do (vd: "Mua hàng đơn #10", "Trả nợ", "Điều chỉnh")
    created_at = Column(DateTime, default=datetime.now)
    # high-resolution epoch milliseconds for stable sorting when many entries share the same minute
    created_ts = Column(Integer, default=lambda: int(datetime.utcnow().timestamp() * 1000))
    
    customer = relationship("Customer", back_populates="logs")

# 3. Order (Cập nhật liên kết)
class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    customer_name = Column(String) # Vẫn giữ để hiển thị nhanh
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True) # Link vào hồ sơ khách
    created_at = Column(DateTime, default=datetime.now)
    # high-resolution epoch milliseconds for stable sorting
    created_ts = Column(Integer, default=lambda: int(datetime.utcnow().timestamp() * 1000))
    total_amount = Column(Integer)
    is_draft = Column(Integer, default=0)  # 1 = PENDING (chờ duyệt), 0 = APPROVED (đã apply)
    # status: 'pending' | 'accepted' | 'completed'
    status = Column(String, default='completed')
    picker_note = Column(String, default="")

    items = relationship("OrderItem", back_populates="order")
    customer_rel = relationship("Customer", back_populates="orders")

class OrderItem(Base):
    __tablename__ = "order_items"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id"))
    product_name = Column(String)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=True)
    variant_info = Column(String)
    quantity = Column(Integer)
    price = Column(Integer)
    order = relationship("Order", back_populates="items")