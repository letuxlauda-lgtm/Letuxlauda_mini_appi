import pandas as pd
from datetime import datetime
import statistics
import sys
import re

# Настройки имен (если нужно искать конкретно их, иначе скрипт найдет всех сам)
TARGET_NAMES = ['ruslan', 'dmutro', 'igor']

print("=" * 80)
print("🚀 ЗАПУСК АНАЛИЗА ЭФФЕКТИВНОСТИ ТЕХНИКОВ")
print("=" * 80)

try:
    # Пытаемся открыть с разными кодировками (частая проблема Excel CSV)
    try:
        df = pd.read_csv('mes_service_otchet.csv', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv('mes_service_otchet.csv', encoding='cp1251', sep=';') # Пробуем Windows кодировку
    
    print("✅ Файл загружен успешно.")
    
except FileNotFoundError:
    print("❌ ОШИБКА: Файл 'mes_service_otchet.csv' не найден!")
    sys.exit(1)

# --- ПРЕДОБРАБОТКА ДАННЫХ ---
try:
    df_work = df.copy()
    
    # Конвертация дат
    df_work['data_start'] = pd.to_datetime(df_work['data_start'], errors='coerce')
    df_work['data_end'] = pd.to_datetime(df_work['data_end'], errors='coerce')
    df_work = df_work.dropna(subset=['data_start', 'data_end']) # Удаляем пустые
    
    df_work['date'] = df_work['data_start'].dt.date
    
    # Функция парсинга времени из строки (например "13 хв")
    def parse_raznica(val):
        if pd.isna(val): return 0
        match = re.search(r'(\d+)', str(val))
        return int(match.group(1)) if match else 0
    
    df_work['raznica_minutes'] = df_work['raznica'].apply(parse_raznica)
    
    # Список всех техников в файле
    technicians = df_work['texnik_start'].unique()
    # Фильтруем только нужных, если они есть, или берем всех
    active_techs = [t for t in technicians if pd.notna(t) and str(t).strip() != '']
    
    print(f"📊 Найдено записей: {len(df_work)}")
    print(f"👨‍🔧 Техники: {', '.join(map(str, active_techs))}\n")

except Exception as e:
    print(f"❌ Ошибка обработки данных: {e}")
    sys.exit(1)

# --- АНАЛИЗ ---
with open('service_glub_analitik.txt', 'w', encoding='utf-8') as f:
    f.write(f"ОТЧЕТ ОТ {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
    f.write("=" * 80 + "\n\n")

    # 1. СРЕДНЕЕ НАЧАЛО РАБОТЫ
    f.write("⏰ 1. СРЕДНЕЕ НАЧАЛО РАБОТЫ (Первый Service ON)\n")
    f.write("-" * 50 + "\n")
    for tech in active_techs:
        # Берем минимальное время старта за каждый уникальный день
        starts = df_work[df_work['texnik_start'] == tech].groupby('date')['data_start'].min()
        if not starts.empty:
            # Считаем среднее время в секундах от начала дня
            seconds_from_midnight = starts.apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second).mean()
            avg_h = int(seconds_from_midnight // 3600)
            avg_m = int((seconds_from_midnight % 3600) // 60)
            f.write(f"{tech}: {avg_h:02d}:{avg_m:02d}\n")
    f.write("\n")

    # 2. САМЫЙ РАННИЙ ВЫХОД
    f.write("🌅 2. САМЫЙ РАННИЙ ВЫХОД (Рекорд)\n")
    f.write("-" * 50 + "\n")
    for tech in active_techs:
        tech_data = df_work[df_work['texnik_start'] == tech]
        if not tech_data.empty:
            earliest = tech_data.loc[tech_data['data_start'].idxmin()]
            f.write(f"{tech}: {earliest['data_start'].strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("\n")

    # 3. СРЕДНЕЕ ОКОНЧАНИЕ РАБОТЫ
    f.write("🏁 3. СРЕДНЕЕ ОКОНЧАНИЕ РАБОТЫ (Последний Service OFF)\n")
    f.write("-" * 50 + "\n")
    for tech in active_techs:
        ends = df_work[df_work['texnik_end'] == tech].groupby('date')['data_end'].max()
        if not ends.empty:
            seconds_from_midnight = ends.apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second).mean()
            avg_h = int(seconds_from_midnight // 3600)
            avg_m = int((seconds_from_midnight % 3600) // 60)
            f.write(f"{tech}: {avg_h:02d}:{avg_m:02d}\n")
    f.write("\n")

    # 4. САМОЕ ПОЗДНЕЕ ОКОНЧАНИЕ
    f.write("🌃 4. САМОЕ ПОЗДНЕЕ ОКОНЧАНИЕ (Рекорд)\n")
    f.write("-" * 50 + "\n")
    for tech in active_techs:
        tech_data = df_work[df_work['texnik_end'] == tech]
        if not tech_data.empty:
            latest = tech_data.loc[tech_data['data_end'].idxmax()]
            f.write(f"{tech}: {latest['data_end'].strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("\n")

    # 5. КОЛИЧЕСТВО АППАРАТОВ (Среднее и Максимум)
    f.write("📦 5. КОЛИЧЕСТВО АППАРАТОВ В ДЕНЬ\n")
    f.write("-" * 50 + "\n")
    for tech in active_techs:
        counts = df_work[df_work['texnik_start'] == tech].groupby('date')['device'].nunique()
        if not counts.empty:
            f.write(f"{tech}:\n")
            f.write(f"   - В среднем: {counts.mean():.1f} шт.\n")
            f.write(f"   - Максимум:  {counts.max()} шт. ({counts.idxmax()})\n")
    f.write("\n")

    # 6. СРЕДНЕЕ ВРЕМЯ НА АППАРАТЕ
    f.write("⏱️ 6. СРЕДНЕЕ ВРЕМЯ НА ТОЧКЕ (по столбику raznica)\n")
    f.write("-" * 50 + "\n")
    for tech in active_techs:
        avg_time = df_work[df_work['texnik_start'] == tech]['raznica_minutes'].mean()
        f.write(f"{tech}: {avg_time:.1f} мин.\n")
    f.write("\n")

    # 7. ВЫХОДНЫЕ ДНИ
    f.write("📅 7. КОЛИЧЕСТВО ВЫХОДНЫХ\n")
    f.write("-" * 50 + "\n")
    # Генерируем полный календарь от первой до последней записи в файле
    full_date_range = pd.date_range(start=df_work['date'].min(), end=df_work['date'].max()).date
    total_days = len(full_date_range)
    
    for tech in active_techs:
        worked_days = df_work[df_work['texnik_start'] == tech]['date'].unique()
        days_off = total_days - len(worked_days)
        f.write(f"{tech}: {days_off} дней (из {total_days} календарных)\n")
    f.write("\n")

    # 8. РЕАЛЬНЫЕ ОБСЛУЖИВАНИЯ (>30 мин)
    f.write("🔧 8. РЕАЛЬНЫЕ ОБСЛУЖИВАНИЯ (ТО > 30 мин)\n")
    f.write("-" * 50 + "\n")
    real_service_scores = {}
    for tech in active_techs:
        tech_df = df_work[df_work['texnik_start'] == tech]
        real_count = len(tech_df[tech_df['raznica_minutes'] > 30])
        total_count = len(tech_df)
        percent = (real_count / total_count * 100) if total_count > 0 else 0
        
        real_service_scores[tech] = real_count
        f.write(f"{tech}: {real_count} сложных ТО (из {total_count} выездов) — {percent:.1f}%\n")
    f.write("\n")

    # 9. РЕЙТИНГ
    f.write("🏆 ИТОГОВЫЙ РЕЙТИНГ\n")
    f.write("=" * 50 + "\n")
    # Формула рейтинга:
    # 1 балл за каждое сложное обслуживание (>30 мин)
    # 0.5 балла за обычный выезд
    # 0.1 балла за каждый рабочий день (бонус за стабильность)
    
    ratings = {}
    for tech in active_techs:
        tech_df = df_work[df_work['texnik_start'] == tech]
        
        points_real = len(tech_df[tech_df['raznica_minutes'] > 30]) * 1.0
        points_regular = len(tech_df) * 0.5
        points_days = len(tech_df['date'].unique()) * 0.2
        
        total_score = points_real + points_regular + points_days
        ratings[tech] = total_score

    sorted_techs = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
    
    medals = ["🥇 1 МЕСТО", "🥈 2 МЕСТО", "🥉 3 МЕСТО"]
    for i, (tech, score) in enumerate(sorted_techs):
        prefix = medals[i] if i < 3 else f"{i+1} МЕСТО"
        f.write(f"{prefix}: {tech.upper()} (Баллы: {score:.1f})\n")

print("✅ Готово! Результаты сохранены в файл 'service_glub_analitik.txt'")