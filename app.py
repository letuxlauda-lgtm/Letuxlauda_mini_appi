from flask import Flask, render_template, request, jsonify
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import sys
import pandas as pd
import threading

# --- ІМПОРТИ ВАШИХ СКРИПТІВ ---
try:
    import week5_ink
    import week1_ink
    import service_glub_analitik
    from otchet_work import download_from_db, merge_with_technicians, generate_tech_report, generate_report
    print("✅ Скрипти успішно підключені")
except ImportError as e:
    print(f"⚠️ Увага: Помилка імпорту скриптів: {e}")
    def generate_tech_report(df, name): return "Помилка скриптів"
    def generate_report(df): return "Помилка скриптів"
    def download_from_db(): return None
    def merge_with_technicians(): return None

load_dotenv() 

app = Flask(__name__)

# --- ФУНКЦІЯ ПІДКЛЮЧЕННЯ ДО БД ---
def get_db_connection():
    """Повертає об'єкт підключення до бази даних."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

# --- БАЗА ДАНИХ АПАРАТІВ ---
APARATS_DB = [
    {"id": 153, "addr": "Антонича, 6", "tech": "ruslan"},
    {"id": 240, "addr": "Багряного, 39", "tech": "ruslan"},
    {"id": 297, "addr": "Біберовича, 11", "tech": "ruslan"},
    {"id": 236, "addr": "Брюховицька, 143", "tech": "ruslan"},
    {"id": 156, "addr": "Брюховичі Івасюка, 1", "tech": "ruslan"},
    {"id": 243, "addr": "Брюховичі Львівська, 92", "tech": "ruslan"},
    {"id": 254, "addr": "Вашингтона, 4в", "tech": "ruslan"},
    {"id": 202, "addr": "Виговського, 5", "tech": "ruslan"},
    {"id": 52, "addr": "Виговського, 5б", "tech": "ruslan"},
    {"id": 178, "addr": "Генерала Тарнавського, 104б", "tech": "ruslan"},
    {"id": 305, "addr": "Гориня, 39", "tech": "ruslan"},
    {"id": 212, "addr": "Городоцька, 213", "tech": "ruslan"},
    {"id": 269, "addr": "Городоцька, 226а", "tech": "ruslan"},
    {"id": 114, "addr": "Демнянська, 26", "tech": "ruslan"},
    {"id": 226, "addr": "Дністерська, 1", "tech": "ruslan"},
    {"id": 87, "addr": "Довженка, 5", "tech": "ruslan"},
    {"id": 118, "addr": "Драгана, 4б", "tech": "ruslan"},
    {"id": 108, "addr": "Дунайська, 7", "tech": "ruslan"},
    {"id": 165, "addr": "Зелена, 204", "tech": "ruslan"},
    {"id": 280, "addr": "Зелена, 44", "tech": "ruslan"},
    {"id": 57, "addr": "Зимна Вода, Тичини, 9", "tech": "ruslan"},
    {"id": 282, "addr": "Йосифа Сліпого, 22", "tech": "ruslan"},
    {"id": 242, "addr": "Караджича, 29б", "tech": "ruslan"},
    {"id": 336, "addr": "Кубійовича, 31", "tech": "ruslan"},
    {"id": 184, "addr": "Кульпарківська, 135", "tech": "ruslan"},
    {"id": 109, "addr": "Кульпарківська, 230", "tech": "ruslan"},
    {"id": 335, "addr": "Лазаренка, 1", "tech": "ruslan"},
    {"id": 292, "addr": "Лапаївка, Геофізиків, 17", "tech": "ruslan"},
    {"id": 54, "addr": "Левицького, 43а", "tech": "ruslan"},
    {"id": 85, "addr": "Левицького, 106", "tech": "ruslan"},
    {"id": 232, "addr": "Липова алея, 1", "tech": "ruslan"},
    {"id": 203, "addr": "Медової печери, 65", "tech": "ruslan"},
    {"id": 60, "addr": "Мечнікова, 16е", "tech": "ruslan"},
    {"id": 298, "addr": "Освицька, 1", "tech": "ruslan"},
    {"id": 281, "addr": "Пасічна, 84а", "tech": "ruslan"},
    {"id": 227, "addr": "Пасічна, 171", "tech": "ruslan"},
    {"id": 208, "addr": "Петлюри, 2а", "tech": "ruslan"},
    {"id": 314, "addr": "Пулюя, 29", "tech": "ruslan"},
    {"id": 279, "addr": "Пулюя, 40", "tech": "ruslan"},
    {"id": 53, "addr": "Родини Крушельницьких, 1а", "tech": "ruslan"},
    {"id": 296, "addr": "Садівнича, 27", "tech": "ruslan"},
    {"id": 183, "addr": "Скорини, 44", "tech": "ruslan"},
    {"id": 302, "addr": "Сокільники, Г.Сковороди, 56", "tech": "ruslan"},
    {"id": 217, "addr": "Сокільники, Героїв Майдану, 17в", "tech": "ruslan"},
    {"id": 244, "addr": "Стрийська, 45в", "tech": "ruslan"},
    {"id": 127, "addr": "Стрийська, 51", "tech": "ruslan"},
    {"id": 316, "addr": "Стрийська, 108", "tech": "ruslan"},
    {"id": 56, "addr": "Тернопільська, 21", "tech": "ruslan"},
    {"id": 174, "addr": "Тернопільська, 8", "tech": "ruslan"},
    {"id": 200, "addr": "Трускавецька, 129", "tech": "ruslan"},
    {"id": 155, "addr": "Угорська, 12", "tech": "ruslan"},
    {"id": 206, "addr": "Угорська, 14б", "tech": "ruslan"},
    {"id": 104, "addr": "Шевченка, 111", "tech": "ruslan"},
    {"id": 211, "addr": "Яворницького, 8", "tech": "ruslan"},
    {"id": 249, "addr": "Віденська, 9", "tech": "ruslan"},
    {"id": 277, "addr": "Кавалерідзе, 23", "tech": "ruslan"},
    {"id": 58, "addr": "Куровця, 36", "tech": "ruslan"},
    {"id": 311, "addr": "Коломийська, 7", "tech": "ruslan"},
    {"id": 163, "addr": "Левицького, 15", "tech": "ruslan"},
    {"id": 164, "addr": "Бандери, 69", "tech": "igor"},
    {"id": 327, "addr": "Веливока, 9", "tech": "igor"},
    {"id": 205, "addr": "Винники, Винна гора, 10б", "tech": "igor"},
    {"id": 126, "addr": "Винники, Сахарова, 10", "tech": "igor"},
    {"id": 251, "addr": "Винники, Франка, 53", "tech": "igor"},
    {"id": 154, "addr": "Гайдамацька, 9а", "tech": "igor"},
    {"id": 268, "addr": "Городоцька, 45", "tech": "igor"},
    {"id": 51, "addr": "Грінченка, 6", "tech": "igor"},
    {"id": 195, "addr": "Грушевського, 7/9", "tech": "igor"},
    {"id": 55, "addr": "Довбуша, 1", "tech": "igor"},
    {"id": 225, "addr": "Замарстинівська, 55г", "tech": "igor"},
    {"id": 180, "addr": "Замарстинівська, 170б", "tech": "igor"},
    {"id": 258, "addr": "Замарстинівська, 170н", "tech": "igor"},
    {"id": 172, "addr": "Зарицьких, 5", "tech": "igor"},
    {"id": 326, "addr": "Зелена, 17", "tech": "igor"},
    {"id": 239, "addr": "Князя Романа, 9", "tech": "igor"},
    {"id": 230, "addr": "Котика, 9", "tech": "igor"},
    {"id": 233, "addr": "Липинського, 29", "tech": "igor"},
    {"id": 231, "addr": "Лисиничі, Шухевича, 5", "tech": "igor"},
    {"id": 193, "addr": "Личаківська, 4/6", "tech": "igor"},
    {"id": 157, "addr": "Личаківська, 70а", "tech": "igor"},
    {"id": 286, "addr": "Личаківська, 86", "tech": "igor"},
    {"id": 186, "addr": "Личаківська, 163", "tech": "igor"},
    {"id": 328, "addr": "Мазепи, 26", "tech": "igor"},
    {"id": 198, "addr": "Малоголосківська, 16", "tech": "igor"},
    {"id": 188, "addr": "Миколайчука, 4а", "tech": "igor"},
    {"id": 61, "addr": "Наливайка, 20", "tech": "igor"},
    {"id": 196, "addr": "Ніжинська, 16", "tech": "igor"},
    {"id": 59, "addr": "Очеретяна, 10", "tech": "igor"},
    {"id": 119, "addr": "Пекарська, 14", "tech": "igor"},
    {"id": 238, "addr": "Під Голоском, 24б", "tech": "igor"},
    {"id": 86, "addr": "просп. Свободи, 1/3", "tech": "igor"},
    {"id": 218, "addr": "просп.В.Чорновола, 7а", "tech": "igor"},
    {"id": 264, "addr": "просп.В.Чорновола, 55", "tech": "igor"},
    {"id": 192, "addr": "просп.В.Чорновола, 67ж", "tech": "igor"},
    {"id": 124, "addr": "просп.В.Чорновола, 69", "tech": "igor"},
    {"id": 113, "addr": "просп.В.Чорновола, 101", "tech": "igor"},
    {"id": 12, "addr": "Січових Стрільців, 13", "tech": "igor"},
    {"id": 122, "addr": "Тичини, 14", "tech": "igor"},
    {"id": 319, "addr": "Тракт Глинянський, 163", "tech": "igor"},
    {"id": 112, "addr": "Франка, 69", "tech": "igor"},
    {"id": 246, "addr": "Хмельницького, 257", "tech": "igor"},
    {"id": 185, "addr": "Хмельницького, 76", "tech": "igor"},
    {"id": 123, "addr": "Щурата, 9", "tech": "igor"},
    {"id": 283, "addr": "Під Дубом, 17", "tech": "igor"},
    {"id": 322, "addr": "Шолом-Алейхема, 20", "tech": "igor"},
    {"id": 107, "addr": "Кошиця, 1", "tech": "igor"},
    {"id": 190, "addr": "Братів Міхновських, 23", "tech": "dmutro"},
    {"id": 179, "addr": "В.Великого, 1", "tech": "dmutro"},
    {"id": 116, "addr": "В.Великого, 35а", "tech": "dmutro"},
    {"id": 221, "addr": "В.Великого, 75", "tech": "dmutro"},
    {"id": 18, "addr": "В.Великого, 103", "tech": "dmutro"},
    {"id": 234, "addr": "Залізнична, 21", "tech": "dmutro"},
    {"id": 209, "addr": "Золота, 25", "tech": "dmutro"},
    {"id": 224, "addr": "Кн.Ольги, 98л", "tech": "dmutro"},
    {"id": 175, "addr": "Кн.Ольги, 100к", "tech": "dmutro"},
    {"id": 293, "addr": "Коновальця, 50", "tech": "dmutro"},
    {"id": 197, "addr": "Кропивницького, 7/9", "tech": "dmutro"},
    {"id": 187, "addr": "Кульпарківська, 93", "tech": "dmutro"},
    {"id": 213, "addr": "Кульпарківська, 145", "tech": "dmutro"},
    {"id": 306, "addr": "Кульпарківська, 172", "tech": "dmutro"},
    {"id": 294, "addr": "Кульпарківська, 59", "tech": "dmutro"},
    {"id": 337, "addr": "Любінська, 4", "tech": "dmutro"},
    {"id": 287, "addr": "Марка Вовчка, 24", "tech": "dmutro"},
    {"id": 199, "addr": "Мундяк Марії, 8", "tech": "dmutro"},
    {"id": 229, "addr": "Наукова, 59", "tech": "dmutro"},
    {"id": 245, "addr": "Наукова, 96", "tech": "dmutro"},
    {"id": 343, "addr": "Наукова, 10", "tech": "dmutro"},
    {"id": 182, "addr": "Повітряна, 78", "tech": "dmutro"},
    {"id": 276, "addr": "Рудненська, 8ж", "tech": "dmutro"},
    {"id": 321, "addr": "Федьковича, 24", "tech": "dmutro"},
    {"id": 176, "addr": "Федьковича, 38", "tech": "dmutro"},
    {"id": 256, "addr": "Художня, 4", "tech": "dmutro"},
    {"id": 317, "addr": "Цегельского, 10", "tech": "dmutro"},
    {"id": 278, "addr": "Чупринки, 84", "tech": "dmutro"},
    {"id": 247, "addr": "Шевченка, 31б", "tech": "dmutro"},
    {"id": 189, "addr": "Шевченка, 45", "tech": "dmutro"},
    {"id": 177, "addr": "Шевченка, 80", "tech": "dmutro"},
    {"id": 210, "addr": "Широка, 96а", "tech": "dmutro"},
    {"id": 259, "addr": "Васильківського, 9", "tech": "dmutro"},
    {"id": 275, "addr": "Героїв УПА, 73в", "tech": "dmutro"},
    {"id": 253, "addr": "Золота, 30", "tech": "dmutro"},
    {"id": 260, "addr": "Юнаківа, 9б", "tech": "dmutro"},
    {"id": 214, "addr": "Суботівська, 7", "tech": "dmutro"},
    {"id": 323, "addr": "Суботівська, 10а", "tech": "dmutro"},
    {"id": 204, "addr": "Роксоляни, 57", "tech": "dmutro"},
    {"id": 301, "addr": "Коперніка, 56", "tech": "dmutro"},
    {"id": 241, "addr": "Дзиндри, 1а", "tech": "dmutro"},
    {"id": 121, "addr": "Сахарова, 60", "tech": "dmutro"},
    {"id": 228, "addr": "Сокільники, Весняна, 18", "tech": "dmutro"},
    {"id": 341, "addr": "Сокільники, Збройних сил України, 2", "tech": "dmutro"},
    {"id": 120, "addr": "Мікльоша, 17", "tech": "dmutro"},
    {"id": 340, "addr": "Гашека, 17", "tech": "dmutro"},
    {"id": 50, "addr": "Стрийська, 61", "tech": "dmutro"},
    {"id": 265, "addr": "Стрийська, 115", "tech": "dmutro"},
    {"id": 344, "addr": "Брюховичі, Весняна, 1а", "tech": "ruslan"},
    {"id": 235, "addr": "Лисеницька, 9", "tech": "igor"},
    {"id": 107, "addr": "Ветеранів, 5", "tech": "igor"}
]

# ==========================================================
# ФОНОВА ОБРОБКА - ЕКСПОРТ CSV
# ==========================================================

def run_csv_export():
    """Фонова функція для експорту CSV."""
    print("⏳ Починаємо фоновий експорт CSV...")
    try:
        conn = get_db_connection()
        df = pd.read_sql_query("SELECT * FROM mes_service_otchet", conn)
        conn.close()
        
        df.to_csv('mes_service_otchet.csv', index=False, encoding='utf-8-sig') 
        print(f"✅ CSV оновлено. Рядків: {len(df)}")
    except Exception as e:
        print(f"❌ Помилка експорту CSV: {e}", file=sys.stderr)


@app.route('/api/super/export_mes_csv', methods=['POST'])
def export_mes_csv():
    """Запускає експорт CSV у фоновому режимі."""
    thread = threading.Thread(target=run_csv_export)
    thread.start()
    return jsonify({'status': 'ok', 'message': 'Експорт CSV запущено у фоновому режимі.'}), 200

# ==========================================================
# ДАШБОРД СУПЕРВІЗОРА - ЄДИНА ФУНКЦІЯ
# ==========================================================

@app.route('/api/super_earnings', methods=['GET'])
def get_super_earnings():
    """
    Повертає дані про заробіток (готівка/безготівка) на поточний момент.
    
    TODO: Замінити мокіровані дані на реальний запит до БД.
    """
    try:
        # === ЗАМІНІТЬ ЦЕЙ БЛОК НА РЕАЛЬНИЙ ЗАПИТ ДО БД ===
        # Приклад SQL запиту (закоментований):
        # conn = get_db_connection()
        # cursor = conn.cursor()
        # cursor.execute("""
        #     SELECT 
        #         SUM(CASE WHEN payment_type = 'cash' THEN amount ELSE 0 END) as cash,
        #         SUM(CASE WHEN payment_type = 'card' THEN amount ELSE 0 END) as noncash
        #     FROM transactions 
        #     WHERE DATE(created_at) = CURRENT_DATE
        # """)
        # result = cursor.fetchone()
        # cash_earnings = float(result[0]) if result[0] else 0.0
        # noncash_earnings = float(result[1]) if result[1] else 0.0
        # cursor.close()
        # conn.close()
        
        # Мокіровані дані для тестування
        cash_earnings = 18500.50
        noncash_earnings = 7450.00
        # === КІНЕЦЬ БЛОКУ, ЩО ПОТРЕБУЄ ЗАМІНИ ===
        
        current_time_str = datetime.now().strftime("%H:%M")
        
        return jsonify({
            'status': 'ok',
            'cash': f"{cash_earnings:,.2f} UAH",
            'noncash': f"{noncash_earnings:,.2f} UAH",
            'time': current_time_str
        })
    except Exception as e:
        print(f"Помилка при отриманні заробітку: {e}", file=sys.stderr)
        return jsonify({'status': 'error', 'message': 'Не вдалося отримати дані про заробіток'}), 500

# ==========================================================
# ОСНОВНІ МАРШРУТИ
# ==========================================================

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/addresses', methods=['GET'])
def get_addresses():
    """Повертає список всіх апаратів."""
    return jsonify(APARATS_DB)

@app.route('/api/create_task', methods=['POST'])
def create_task():
    """Створює нове термінове завдання."""
    # TODO: Додати вашу логіку створення завдання
    data = request.get_json()
    print(f"Створення завдання: {data}")
    return jsonify({'status': 'ok', 'message': 'Завдання створено'}), 200

@app.route('/api/update_task_status', methods=['POST'])
def update_task_status():
    """Оновлює статус завдання."""
    # TODO: Додати вашу логіку оновлення статусу
    data = request.get_json()
    print(f"Оновлення статусу: {data}")
    return jsonify({'status': 'ok', 'message': 'Статус оновлено'}), 200

# TODO: Додайте інші ваші маршрути (create_termin_task, звіти тощо)

if __name__ == '__main__':
    # В production використовуйте gunicorn або інший WSGI-сервер
    app.run(debug=True, host='0.0.0.0', threaded=True)