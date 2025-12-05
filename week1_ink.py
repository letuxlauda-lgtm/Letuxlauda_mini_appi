import pandas as pd
import psycopg2
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_report():
    try:
        db_config = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        conn = psycopg2.connect(**db_config)
        df = pd.read_sql_query("SELECT * FROM inki5nedel", conn)
        conn.close()

        if df.empty: return "📂 Таблиця порожня."

        # Фильтр 7 дней
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        cutoff = datetime.now() - timedelta(days=7)
        df = df[df['date'] >= cutoff].copy()
        
        if df.empty: return "📂 За останні 7 днів даних немає."

        df['banknotes'] = pd.to_numeric(df['banknotes'], errors='coerce').fillna(0)
        df['coins'] = pd.to_numeric(df['coins'], errors='coerce').fillna(0)
        df['total_sum'] = df['banknotes'] + df['coins']
        df['tech'] = df['tech'].fillna('Unknown')
        
        # === ЛОГИКА ФИЛЬТРАЦИИ (> 25000) ===
        suspicious_df = df[df['total_sum'] > 25000].copy()
        normal_df = df[df['total_sum'] <= 25000].copy()

        normal_df['tech_lower'] = normal_df['tech'].astype(str).str.lower().str.strip()

        lines = []
        lines.append("<b>📊 ЗВІТ ІНКАСАЦІЇ (7 ДНІВ)</b>")
        lines.append("=" * 30)

        known_techs = ['ruslan', 'igor', 'dmutro']
        
        for tech in known_techs:
            tech_df = normal_df[normal_df['tech_lower'] == tech]
            if tech_df.empty: continue
            
            lines.append(f"<br><b>👤 {tech.upper()}</b>")
            grouped = tech_df.groupby('device_id').agg({'banknotes':'sum', 'coins':'sum', 'address':'first'})
            
            tech_total = 0
            for did, row in grouped.iterrows():
                s = row['banknotes'] + row['coins']
                tech_total += s
                lines.append(f"ID {did}: {s:,.0f} грн ({row['address']})".replace(',', ' '))
            
            lines.append(f"👉 <b>Разом: {tech_total:,.0f} грн</b>".replace(',', ' '))

        # === БЛОК ПОДОЗРИТЕЛЬНЫХ ===
        if not suspicious_df.empty:
            lines.append("<br>" + "_" * 40)
            lines.append("<b>🚫 ПІДОЗРІЛІ ІНКІ (>25к)</b>")
            lines.append("_" * 40)
            
            for _, row in suspicious_df.iterrows():
                dev_id = row['device_id']
                addr = row['address'] if row['address'] else "Не вказано"
                tech = row['tech']
                s = row['total_sum']
                date_str = row['date'].strftime('%d.%m')
                lines.append(f"🔴 <b>{dev_id}</b> | {addr}")
                lines.append(f"&nbsp;&nbsp; Сума: <b>{s:,.0f}</b> грн | {tech} | {date_str}".replace(',', ' '))

        return "<br>".join(lines)

    except Exception as e:
        return f"❌ Помилка скрипта 1 тиждень: {e}"