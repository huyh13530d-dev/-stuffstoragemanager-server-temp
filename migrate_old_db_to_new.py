import argparse
import sqlite3
from pathlib import Path


def create_new_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(
        """
        PRAGMA foreign_keys = OFF;

        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            description VARCHAR,
            image_path VARCHAR
        );

        CREATE TABLE variants (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            color VARCHAR,
            size VARCHAR,
            price INTEGER,
            stock INTEGER
        );

        CREATE TABLE areas (
            id INTEGER PRIMARY KEY,
            name VARCHAR UNIQUE
        );

        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR UNIQUE,
            phone VARCHAR,
            debt INTEGER,
            area_id INTEGER
        );

        CREATE TABLE debt_logs (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            change_amount INTEGER,
            new_balance INTEGER,
            note VARCHAR,
            created_at DATETIME,
            created_ts INTEGER
        );

        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_name VARCHAR,
            customer_id INTEGER,
            created_at DATETIME,
            created_ts INTEGER,
            total_amount INTEGER,
            is_draft INTEGER,
            status VARCHAR DEFAULT 'completed',
            picker_note VARCHAR DEFAULT ''
        );

        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_name VARCHAR,
            variant_id INTEGER,
            variant_info VARCHAR,
            quantity INTEGER,
            price INTEGER
        );

        PRAGMA foreign_keys = ON;
        """
    )
    conn.commit()


def copy_products(old: sqlite3.Connection, new: sqlite3.Connection) -> None:
    rows = old.execute("SELECT id, name, description, image_path FROM products").fetchall()
    new.executemany(
        "INSERT INTO products (id, name, description, image_path) VALUES (?, ?, ?, ?)",
        rows,
    )


def copy_variants(old: sqlite3.Connection, new: sqlite3.Connection) -> None:
    rows = old.execute("SELECT id, product_id, color, size, price, stock FROM variants").fetchall()
    new.executemany(
        "INSERT INTO variants (id, product_id, color, size, price, stock) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


def copy_customers(old: sqlite3.Connection, new: sqlite3.Connection) -> None:
    default_area_id = new.execute("SELECT id FROM areas WHERE name = 'Chợ hàn' LIMIT 1").fetchone()[0]
    rows = old.execute("SELECT id, name, phone, debt FROM customers").fetchall()
    new.executemany(
        "INSERT INTO customers (id, name, phone, debt, area_id) VALUES (?, ?, ?, ?, ?)",
        [(r[0], r[1], r[2], r[3], default_area_id) for r in rows],
    )


def seed_areas(new: sqlite3.Connection) -> None:
    areas = ["Chợ đêm", "Chợ hàn", "Hội An", "Nha Trang"]
    for i, name in enumerate(areas, start=1):
        new.execute("INSERT INTO areas (id, name) VALUES (?, ?)", (i, name))


def copy_debt_logs(old: sqlite3.Connection, new: sqlite3.Connection) -> None:
    rows = old.execute(
        "SELECT id, customer_id, change_amount, new_balance, note, created_at, created_ts FROM debt_logs"
    ).fetchall()
    new.executemany(
        "INSERT INTO debt_logs (id, customer_id, change_amount, new_balance, note, created_at, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def copy_orders(old: sqlite3.Connection, new: sqlite3.Connection) -> None:
    rows = old.execute(
        "SELECT id, customer_name, customer_id, created_at, created_ts, total_amount, COALESCE(is_draft,0) FROM orders"
    ).fetchall()
    transformed = []
    for r in rows:
        status = "pending" if (r[6] or 0) == 1 else "completed"
        transformed.append((r[0], r[1], r[2], r[3], r[4], r[5], r[6], status, ""))
    new.executemany(
        "INSERT INTO orders (id, customer_name, customer_id, created_at, created_ts, total_amount, is_draft, status, picker_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        transformed,
    )


def copy_order_items(old: sqlite3.Connection, new: sqlite3.Connection) -> None:
    rows = old.execute(
        "SELECT id, order_id, product_name, variant_id, variant_info, quantity, price FROM order_items"
    ).fetchall()
    new.executemany(
        "INSERT INTO order_items (id, order_id, product_name, variant_id, variant_info, quantity, price) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate old shop.db to new schema")
    parser.add_argument("--old", default="shop.db", help="Path to old SQLite DB")
    parser.add_argument("--new", default="shop_new.db", help="Path to output new SQLite DB")
    args = parser.parse_args()

    old_path = Path(args.old)
    new_path = Path(args.new)

    if not old_path.exists():
        raise FileNotFoundError(f"Old DB not found: {old_path}")

    if new_path.exists():
        raise FileExistsError(f"Output DB already exists: {new_path}")

    old_conn = sqlite3.connect(str(old_path))
    new_conn = sqlite3.connect(str(new_path))

    try:
        create_new_schema(new_conn)
        seed_areas(new_conn)
        copy_products(old_conn, new_conn)
        copy_variants(old_conn, new_conn)
        copy_customers(old_conn, new_conn)
        copy_debt_logs(old_conn, new_conn)
        copy_orders(old_conn, new_conn)
        copy_order_items(old_conn, new_conn)
        new_conn.commit()
        print(f"Done: migrated {old_path} -> {new_path}")
    finally:
        old_conn.close()
        new_conn.close()


if __name__ == "__main__":
    main()
