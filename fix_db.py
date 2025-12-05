import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    print("🛠 Исправляем базу данных...")

    # 1. Добавляем адрес в стол заказов (Исправляет ошибку со скриншота)
    cur.execute("ALTER TABLE stol_zakazov ADD COLUMN IF NOT EXISTS adres VARCHAR(255);")
    
    # 2. Убедимся, что есть таблица срочных задач
    cur.execute("""
        CREATE TABLE IF NOT EXISTS zadaci_srocno_all (
            id SERIAL PRIMARY KEY,
            id_terem INTEGER,
            adres VARCHAR(255),
            pricina TEXT,
            texnik VARCHAR(50),
            date_time_open TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) DEFAULT 'open'
        );
    """)

    # 3. Убедимся, что есть таблица заказов карт
    cur.execute("""
        CREATE TABLE IF NOT EXISTS zakazu_all (
            id SERIAL PRIMARY KEY,
            id_terem INTEGER,
            adres VARCHAR(255),
            zamovnuk VARCHAR(255),
            texnik VARCHAR(50),
            date_time_open TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) DEFAULT 'open'
        );
    """)
    
    conn.commit()
    print("✅ Все таблицы исправлены и колонки добавлены!")
    cur.close()
    conn.close()

except Exception as e:
    print(f"❌ Ошибка: {e}")