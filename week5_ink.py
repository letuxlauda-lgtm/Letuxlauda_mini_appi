import pandas as pd
import psycopg2
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_report():
    try:
        # Параметры подключения
        db_config = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }

        conn = psycopg2.connect(**db_config)
        df = pd.read_sql_query("SELECT * FROM inki5nedel", conn)
        
        # Читаем привязку техников
        try:
            df_tech_map = pd.read_sql_query("SELECT id_terem, texnik FROM privyazka_aparat_texnik", conn)
        except:
            df_tech_map = pd.DataFrame(columns=['id_terem', 'texnik'])

        conn.close()

        if df.empty: return "📂 Таблиця inki5nedel порожня."

        # Нормализация
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        
        # Конвертация
        df['banknotes'] = pd.to_numeric(df['banknotes'], errors='coerce').fillna(0)
        df['coins'] = pd.to_numeric(df['coins'], errors='coerce').fillna(0)
        df['total_sum'] = df['banknotes'] + df['coins'] # Считаем общую сумму сразу
        df['tech'] = df['tech'].fillna('unknown')

        # Заполнение техников
        mask_missing = df['tech'].isin(['unknown', '   -   ', '', None])
        if mask_missing.any() and not df_tech_map.empty:
            df_tech_map['id_terem'] = pd.to_numeric(df_tech_map['id_terem'], errors='coerce').fillna(0).astype(int)
            tech_mapping = dict(zip(df_tech_map['id_terem'], df_tech_map['texnik']))
            for idx in df[mask_missing].index:
                try:
                    dev_id = int(df.at[idx, 'device_id'])
                    found = tech_mapping.get(dev_id)
                    if found: df.at[idx, 'tech'] = found
                except: pass

        # === ЛОГИКА ФИЛЬТРАЦИИ (> 25000) ===
        suspicious_df = df[df['total_sum'] > 25000].copy()
        normal_df = df[df['total_sum'] <= 25000].copy()

        # Генерация отчета
        lines = []
        lines.append("<b>📊 ЗВІТ ІНКАСАЦІЇ (5 ТИЖНІВ)</b>")
        lines.append("=" * 30)
        
        today = datetime.now().date()
        cutoff_date = pd.Timestamp(today) - pd.Timedelta(days=7)
        
        known_techs = ['ruslan', 'igor', 'dmutro']
        total_bank = 0
        total_coin = 0
        
        normal_df['tech_lower'] = normal_df['tech'].astype(str).str.lower().str.strip()

        for tech in known_techs:
            tech_df = normal_df[normal_df['tech_lower'] == tech].copy()
            if tech_df.empty: continue
            
            lines.append(f"<br><b>👤 {tech.upper()}</b>")
            lines.append("-" * 20)
            
            # Разделение: Сдано / На руках
            sdano = tech_df[tech_df['date'] < cutoff_date]
            na_rukah = tech_df[tech_df['date'] >= cutoff_date]
            
            if not sdano.empty:
                s_bank = sdano['banknotes'].sum()
                s_coin = sdano['coins'].sum()
                lines.append(f"✅ ЗДАНО: {(s_bank+s_coin):,.0f} грн".replace(',', ' '))
            
            if not na_rukah.empty:
                n_bank = na_rukah['banknotes'].sum()
                n_coin = na_rukah['coins'].sum()
                lines.append(f"💰 НА РУКАХ: {(n_bank+n_coin):,.0f} грн".replace(',', ' '))
                
                # Детализация по дням
                daily = na_rukah.groupby(na_rukah['date'].dt.date).agg({'banknotes':'sum', 'coins':'sum'})
                for day, row in daily.iterrows():
                    d_sum = row['banknotes'] + row['coins']
                    lines.append(f"&nbsp;&nbsp;📅 {day.strftime('%d.%m')}: {d_sum:,.0f} грн".replace(',', ' '))

            t_bank = tech_df['banknotes'].sum()
            t_coin = tech_df['coins'].sum()
            total_bank += t_bank
            total_coin += t_coin

        lines.append("<br>" + "=" * 30)
        lines.append(f"<b>ВСЬОГО (без підозрілих): {(total_bank + total_coin):,.0f} грн</b>".replace(',', ' '))

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
        return f"❌ Помилка скрипта 5 тижнів: {e}"