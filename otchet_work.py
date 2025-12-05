import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

# Загружаем переменные окружения из .env
load_dotenv()

# Параметры подключения к БД
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def download_from_db():
    """Скачивает таблицу idadres из PostgreSQL и сохраняет в Excel"""
    try:
        print("Подключение к базе данных...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        print("Загрузка таблицы idadres...")
        query = "SELECT * FROM idadres"
        df = pd.read_sql_query(query, conn)
        
        # Сохраняем в Excel
        df.to_excel('idadres.xlsx', index=False)
        print(f"✅ Таблица сохранена: idadres.xlsx ({len(df)} записей)")
        
        conn.close()
        return df
    
    except Exception as e:
        print(f"❌ Ошибка при работе с БД: {e}")
        return None


def merge_with_technicians():
    """Сопоставляет idadres.xlsx с privyazka_aparat_texnik.csv и добавляет столбец техников"""
    try:
        # Читаем idadres.xlsx
        print("\nЧтение idadres.xlsx...")
        df_idadres = pd.read_excel('idadres.xlsx')
        
        # Читаем privyazka_aparat_texnik.csv с автоопределением разделителя
        print("Чтение privyazka_aparat_texnik.csv...")
        try:
            # Пробуем с запятой
            df_tech = pd.read_csv('privyazka_aparat_texnik.csv', sep=',', encoding='utf-8')
            if len(df_tech.columns) == 1:
                # Если получилась одна колонка, пробуем точку с запятой
                df_tech = pd.read_csv('privyazka_aparat_texnik.csv', sep=';', encoding='utf-8')
        except:
            try:
                df_tech = pd.read_csv('privyazka_aparat_texnik.csv', sep=';', encoding='cp1251')
            except:
                df_tech = pd.read_csv('privyazka_aparat_texnik.csv', sep=',', encoding='cp1251')
        
        print(f"Загружено {len(df_tech)} привязок техников")
        print(f"Столбцы в privyazka_aparat_texnik.csv: {list(df_tech.columns)}")
        
        # Добавляем столбец texnik, если его нет
        if 'texnik' not in df_idadres.columns:
            df_idadres['texnik'] = None
        
        # Определяем правильные названия столбцов (убираем возможные пробелы)
        df_tech.columns = df_tech.columns.str.strip()
        
        # Ищем столбец с id (может называться id_terem, id, Id и т.д.)
        id_column = None
        for col in df_tech.columns:
            if 'id' in col.lower() and 'terem' in col.lower():
                id_column = col
                break
        
        if id_column is None:
            # Если не нашли id_terem, ищем просто id
            for col in df_tech.columns:
                if col.lower() == 'id':
                    id_column = col
                    break
        
        if id_column is None:
            print(f"❌ Не найден столбец с ID в файле. Доступные столбцы: {list(df_tech.columns)}")
            return None
        
        # Ищем столбец с техником
        tech_column = None
        for col in df_tech.columns:
            if 'tech' in col.lower() or 'texn' in col.lower():
                tech_column = col
                break
        
        if tech_column is None:
            print(f"❌ Не найден столбец с техником. Доступные столбцы: {list(df_tech.columns)}")
            return None
        
        print(f"Используем столбцы: ID='{id_column}', Техник='{tech_column}'")
        
        # ОТЛАДКА: Показываем первые 5 ID из каждой таблицы
        print(f"\nПервые 5 ID из idadres.xlsx:")
        print(df_idadres['id'].head().tolist())
        print(f"Тип данных: {df_idadres['id'].dtype}")
        
        print(f"\nПервые 5 ID из privyazka_aparat_texnik.csv:")
        print(df_tech[id_column].head().tolist())
        print(f"Тип данных: {df_tech[id_column].dtype}")
        
        # Приводим оба столбца с ID к одному типу (строка) для корректного сравнения
        df_idadres['id'] = df_idadres['id'].astype(str).str.strip()
        df_tech[id_column] = df_tech[id_column].astype(str).str.strip()
        
        print(f"\nПосле преобразования в строки:")
        print(f"idadres: {df_idadres['id'].head().tolist()}")
        print(f"privyazka: {df_tech[id_column].head().tolist()}")
        
        # Создаем словарь для быстрого поиска
        tech_dict = dict(zip(df_tech[id_column], df_tech[tech_column]))
        
        print(f"\nВсего в словаре техников: {len(tech_dict)} записей")
        print(f"Пример из словаря: {list(tech_dict.items())[:3]}")
        
        # Сопоставляем по id (idadres) и id_column (privyazka)
        print("\nСопоставление техников с аппаратами...")
        matched = 0
        not_matched = []
        for idx, row in df_idadres.iterrows():
            aparat_id = str(row.get('id')).strip()
            if aparat_id in tech_dict:
                df_idadres.at[idx, 'texnik'] = tech_dict[aparat_id]
                matched += 1
            else:
                if len(not_matched) < 5:
                    not_matched.append(aparat_id)
        
        print(f"Сопоставлено: {matched} аппаратов с техниками")
        if not_matched:
            print(f"Примеры ID без техников: {not_matched}")
        
        # Сохраняем обновленный файл
        df_idadres.to_excel('idadres.xlsx', index=False)
        
        tech_count = df_idadres['texnik'].notna().sum()
        print(f"✅ Добавлено {tech_count} техников к аппаратам")
        
        return df_idadres
    
    except Exception as e:
        print(f"❌ Ошибка при сопоставлении: {e}")
        return None


def generate_report(df):
    """Генерирует общий отчёт (только для аппаратов с техниками)"""
    # Фильтруем только аппараты с назначенным техником
    df = df[df['texnik'].notna()].copy()
    
    report = []
    
    # 1. DV3 неисправности
    dv3_issues = df[df['dv3r'] == 'nerabotaet']
    if not dv3_issues.empty:
        report.append("🔊🛑 DV3 неисправності🛑")
        for _, row in dv3_issues.iterrows():
            report.append(f"🛑{row['id']} {row['adress']} техник {row['texnik']}🛑")
        report.append("")
    
    # 2. DV6 требует внимания
    dv6_issues = df[df['dv6time'].notna() & (df['dv6time'] != '')]
    if not dv6_issues.empty:
        report.append("🟠DV6 потребує уваги🟠")
        for _, row in dv6_issues.iterrows():
            report.append(f"🟠{row['id']} {row['adress']} техник {row['texnik']}🟠")
        report.append("")
    
    # 3. Не хватает воды
    df['dv2week'] = pd.to_numeric(df['dv2week'], errors='coerce')
    water_issues = df[df['dv2week'] >= 9]
    if not water_issues.empty:
        report.append("🟣Не вистачає води🟣")
        for _, row in water_issues.iterrows():
            report.append(f"🟣{row['id']} {row['adress']} техник {row['texnik']}🟣")
        report.append("")
    
    # 4. TDS выше 40
    df['TDS'] = pd.to_numeric(df['TDS'], errors='coerce')
    TDS_issues = df[df['TDS'] > 40]
    if not TDS_issues.empty:
        report.append("⚫️TDS⚫️")
        for _, row in TDS_issues.iterrows():
            report.append(f"⚫️{row['id']} {row['adress']} техник {row['texnik']}⚫️")
        report.append("")
    
    # 5. DV1 неисправности
    dv1_issues = df[df['dv1r'] == 'nerabotaet']
    if not dv1_issues.empty:
        report.append("🟡DV1🟡")
        for _, row in dv1_issues.iterrows():
            report.append(f"🟡{row['id']} {row['adress']} техник {row['texnik']}🟡")
        report.append("")
    
    # 6. Низкая скорость
    df['pokazat.skoros'] = pd.to_numeric(df['pokazat.skoros'], errors='coerce')
    speed_issues = df[df['pokazat.skoros'] >= 9]
    if not speed_issues.empty:
        report.append("🟨Фільтра показують низьку швидкість🟨")
        for _, row in speed_issues.iterrows():
            report.append(f"🟨{row['id']} {row['adress']} техник {row['texnik']}🟨")
        report.append("")
    
    return "\n".join(report)


def generate_tech_summary(df):
    """Генерирует сводку по техникам"""
    # Фильтруем только аппараты с техниками
    df = df[df['texnik'].notna()].copy()
    
    summary = ["⚪️По техникам⚪️\n"]
    
    technicians = df['texnik'].unique()
    
    for tech in sorted(technicians):
        tech_df = df[df['texnik'] == tech].copy()
        
        dv3_count = len(tech_df[tech_df['dv3r'] == 'nerabotaet'])
        dv6_count = len(tech_df[tech_df['dv6time'].notna() & (tech_df['dv6time'] != '')])
        
        tech_df['dv2week'] = pd.to_numeric(tech_df['dv2week'], errors='coerce')
        water_count = len(tech_df[tech_df['dv2week'] >= 9])
        
        tech_df['TDS'] = pd.to_numeric(tech_df['TDS'], errors='coerce')
        TDS_count = len(tech_df[tech_df['TDS'] > 40])
        
        dv1_count = len(tech_df[tech_df['dv1r'] == 'nerabotaet'])
        
        tech_df['pokazat.skoros'] = pd.to_numeric(tech_df['pokazat.skoros'], errors='coerce')
        speed_count = len(tech_df[tech_df['pokazat.skoros'] >= 9])
        
        summary.append(
            f"{tech}        🛑{dv3_count}🛑🟠{dv6_count}🟠   "
            f"🟣{water_count}🟣⚫️{TDS_count}⚫️    🟡{dv1_count}🟡🟨{speed_count}🟨"
        )
    
    return "\n".join(summary)


def generate_tech_report(df, tech_name):
    """Генерирует отчёт для конкретного техника"""
    # Фильтруем по имени техника (регистронезависимо)
    tech_df = df[df['texnik'].str.lower() == tech_name.lower()].copy()
    
    if tech_df.empty:
        return f"Немає даних для техніка {tech_name}"
    
    report = []
    
    # 1. DV3 неисправности
    dv3_issues = tech_df[tech_df['dv3r'] == 'nerabotaet']
    if not dv3_issues.empty:
        report.append("🔊🛑 DV3 неисправності🛑")
        for _, row in dv3_issues.iterrows():
            report.append(f"🛑{row['id']} {row['adress']} техник {row['texnik']}🛑")
        report.append("")
    
    # 2. DV6 требует внимания
    dv6_issues = tech_df[tech_df['dv6time'].notna() & (tech_df['dv6time'] != '')]
    if not dv6_issues.empty:
        report.append("🟠DV6 потребує уваги🟠")
        for _, row in dv6_issues.iterrows():
            report.append(f"🟠{row['id']} {row['adress']} техник {row['texnik']}🟠")
        report.append("")
    
    # 3. Не хватает воды
    tech_df['dv2week'] = pd.to_numeric(tech_df['dv2week'], errors='coerce')
    water_issues = tech_df[tech_df['dv2week'] >= 9]
    if not water_issues.empty:
        report.append("🟣Не вистачає води🟣")
        for _, row in water_issues.iterrows():
            report.append(f"🟣{row['id']} {row['adress']} техник {row['texnik']}🟣")
        report.append("")
    
    # 4. TDS
    tech_df['TDS'] = pd.to_numeric(tech_df['TDS'], errors='coerce')
    TDS_issues = tech_df[tech_df['TDS'] > 40]
    if not TDS_issues.empty:
        report.append("⚫️TDS⚫️")
        for _, row in TDS_issues.iterrows():
            report.append(f"⚫️{row['id']} {row['adress']} техник {row['texnik']}⚫️")
        report.append("")
    
    # 5. DV1
    dv1_issues = tech_df[tech_df['dv1r'] == 'nerabotaet']
    if not dv1_issues.empty:
        report.append("🟡DV1🟡")
        for _, row in dv1_issues.iterrows():
            report.append(f"🟡{row['id']} {row['adress']} техник {row['texnik']}🟡")
        report.append("")
    
    # 6. Низкая скорость
    tech_df['pokazat.skoros'] = pd.to_numeric(tech_df['pokazat.skoros'], errors='coerce')
    speed_issues = tech_df[tech_df['pokazat.skoros'] >= 9]
    if not speed_issues.empty:
        report.append("🟨Фільтра показують низьку швидкість🟨")
        for _, row in speed_issues.iterrows():
            report.append(f"🟨{row['id']} {row['adress']} техник {row['texnik']}🟨")
        report.append("")
    
    return "\n".join(report)


def main():
    """Основная функция"""
    print("=" * 60)
    print("ГЕНЕРАЦІЯ ЗВІТІВ ПО АПАРАТАМ")
    print("=" * 60)
    
    # Шаг 1: Скачиваем таблицу из БД
    df = download_from_db()
    if df is None:
        print("\n❌ Не вдалося завантажити дані з БД")
        return
    
    # Шаг 2: Сопоставляем с техниками
    df = merge_with_technicians()
    if df is None:
        print("\n❌ Не вдалося зіставити техніків")
        return
    
    # Шаг 3: Генерируем общий отчёт
    print("\n" + "=" * 60)
    print("Генерація загального звіту...")
    general_report = generate_report(df)
    tech_summary = generate_tech_summary(df)
    
    full_report = general_report + "\n\n" + tech_summary
    
    with open('otchet_general.txt', 'w', encoding='utf-8') as f:
        f.write(full_report)
    print("✅ Загальний звіт збережено: otchet_general.txt")
    
    # Шаг 4: Генерируем отчёты для каждого техника
    technicians = ['ruslan', 'dmutro', 'igor']
    
    print("\n" + "=" * 60)
    for tech in technicians:
        print(f"Генерація звіту для {tech}...")
        tech_report = generate_tech_report(df, tech)
        
        filename = f'otchet_{tech}.txt'
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(tech_report)
        print(f"✅ Звіт збережено: {filename}")
    
    print("\n" + "=" * 60)
    print("✅ ВСІ ЗВІТИ УСПІШНО ЗГЕНЕРОВАНІ!")
    print("=" * 60)
    print("\nСтворені файли:")
    print("  • otchet_general.txt - загальний звіт")
    print("  • otchet_ruslan.txt - звіт для Руслана")
    print("  • otchet_dmutro.txt - звіт для Дмитра")
    print("  • otchet_igor.txt - звіт для Ігоря")
    print("  • idadres.xlsx - таблиця з техніками")


if __name__ == "__main__":
    main()