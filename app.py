from flask import Flask, render_template, request, jsonify
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import sys
import pandas as pd

# --- ИМПОРТЫ ТВОИХ СКРИПТОВ ---
try:
    import week5_ink
    import week1_ink
    import service_glub_analitik
    from otchet_work import download_from_db, merge_with_technicians, generate_tech_report, generate_report
    print("✅ Скрипты успешно подключены")
except ImportError as e:
    print(f"⚠️ Внимание: Ошибка импорта скриптов: {e}")
    # Заглушки на случай ошибки
    def generate_tech_report(df, name): return "Ошибка скриптов"
    def generate_report(df): return "Ошибка скриптов"
    def download_from_db(): return None
    def merge_with_technicians(): return None

load_dotenv() 

app = Flask(__name__)

# --- БАЗА ДАННЫХ АППАРАТОВ (ПОЛНЫЙ СПИСОК) ---
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
    {"id": 302, "addr": "Сокільники, Г.Сковороди, 56", "tech": "dmutro"},
    {"id": 120, "addr": "Мікльоша, 17", "tech": "dmutro"},
    {"id": 340, "addr": "Гашека, 17", "tech": "dmutro"},
    {"id": 50, "addr": "Стрийська, 61", "tech": "dmutro"},
    {"id": 265, "addr": "Стрийська, 115", "tech": "dmutro"},
    {"id": 344, "addr": "Брюховичі, Весняна, 1а", "tech": "ruslan"},
    {"id": 235, "addr": "Лисеницька, 9", "tech": "igor"},
    {"id": 107, "addr": "Ветеранів, 5", "tech": "igor"}
]

# --- ПОДКЛЮЧЕНИЕ К БД ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")
        return None

# ==========================================
# ОСНОВНЫЕ МАРШРУТЫ
# ==========================================

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/addresses', methods=['GET'])
def get_addresses():
    return jsonify(APARATS_DB)

@app.route('/api/login', methods=['POST'])
def login():
    return jsonify({'status': 'success'})

# ==========================================
# ФУНКЦИИ CALL-CENTER
# ==========================================

@app.route('/api/create_task', methods=['POST'])
def create_task():
    data = request.json
    aparat_id = data.get('id')
    problem = data.get('problem')
    
    aparat = next((item for item in APARATS_DB if item["id"] == int(aparat_id)), None)
    if not aparat: return jsonify({'status': 'error', 'message': 'Апарат не знайдено'})

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO zadaci_all (id_terem, adres, zadaca, texnik, date_time_open, status) VALUES (%s, %s, %s, %s, NOW(), 'open')", 
                   (aparat['id'], aparat['addr'], problem, aparat['tech']))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': f'Завдання створено!\nТехнік: {aparat["tech"]}'})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/create_urgent_task', methods=['POST'])
def create_urgent_task():
    data = request.json
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO zadaci_srocno_all (id_terem, adres, pricina, texnik, date_time_open, status) VALUES (%s, %s, %s, %s, NOW(), 'open')",
                   (data.get('id_terem'), data.get('adres'), data.get('pricina'), data.get('texnik')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': 'Термінове завдання створено!'})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/create_card_order', methods=['POST'])
def create_card_order():
    data = request.json
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO zakazu_all (id_terem, adres, zamovnuk, texnik, date_time_open, status) VALUES (%s, %s, %s, %s, NOW(), 'open')",
                   (data.get('id_terem'), data.get('adres'), data.get('zamovnuk'), data.get('texnik')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': 'Замовлення карти створено!'})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

# ==========================================
# ФУНКЦИИ ТЕХНИКА
# ==========================================

@app.route('/api/create_order', methods=['POST'])
def create_order():
    data = request.json
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO stol_zakazov (texnik, adres, zakaz, date_time_open, status) VALUES (%s, %s, %s, NOW(), 'open')",
                   (data.get('texnik'), data.get('adres'), data.get('zakaz')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': 'Замовлення створено!'})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/create_expense', methods=['POST'])
def create_expense():
    data = request.json
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO vutratu (texnik, vutratu, summa, date_time_open, status) VALUES (%s, %s, %s, NOW(), 'open')",
                   (data.get('texnik'), data.get('vutratu'), data.get('summa')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': 'Витрату додано!'})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/get_tech_tasks', methods=['POST'])
def get_tech_tasks():
    data = request.json
    tech_login = data.get('tech')
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        tasks = []
        # 1. СРОЧНЫЕ
        cur.execute("SELECT id, id_terem, adres, pricina, date_time_open FROM zadaci_srocno_all WHERE status = 'open' AND texnik = %s ORDER BY id DESC", (tech_login,))
        for row in cur.fetchall(): tasks.append({'id': row[0], 'terem': row[1], 'adres': row[2], 'info': row[3], 'date': row[4].strftime('%d.%m %H:%M'), 'type': 'urgent', 'icon': '🔴', 'table': 'zadaci_srocno_all'})
        # 2. ОБЫЧНЫЕ
        cur.execute("SELECT id, id_terem, adres, zadaca, date_time_open FROM zadaci_all WHERE status = 'open' AND texnik = %s ORDER BY id DESC", (tech_login,))
        for row in cur.fetchall(): tasks.append({'id': row[0], 'terem': row[1], 'adres': row[2], 'info': row[3], 'date': row[4].strftime('%d.%m %H:%M'), 'type': 'normal', 'icon': '🟠', 'table': 'zadaci_all'})
        # 3. КАРТЫ
        cur.execute("SELECT id, id_terem, adres, zamovnuk, date_time_open FROM zakazu_all WHERE status = 'open' AND texnik = %s ORDER BY id DESC", (tech_login,))
        for row in cur.fetchall(): tasks.append({'id': row[0], 'terem': row[1], 'adres': row[2], 'info': f"Замовник: {row[3]}", 'date': row[4].strftime('%d.%m %H:%M'), 'type': 'order', 'icon': '✉️', 'table': 'zakazu_all'})
        conn.close()
        return jsonify({'status': 'success', 'tasks': tasks})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/complete_task', methods=['POST'])
def complete_task():
    data = request.json
    task_id = data.get('task_id')
    table_name = data.get('table')
    if table_name not in ['zadaci_all', 'zadaci_srocno_all', 'zakazu_all']: return jsonify({'status': 'error'})
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT date_time_open FROM {table_name} WHERE id = %s", (task_id,))
        result = cur.fetchone()
        duration_str = "Виконано"
        if result:
            diff = datetime.now() - result[0]
            duration_str = f"{diff.days} дн. {diff.seconds // 60} хв."
        
        if table_name == 'zadaci_all':
            cur.execute(f"UPDATE {table_name} SET status = 'closed', date_time_closed = NOW(), day_time_vupolnyalos = %s WHERE id = %s", (duration_str, task_id))
        else:
            cur.execute(f"UPDATE {table_name} SET status = 'closed' WHERE id = %s", (task_id,))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': 'Виконано!', 'duration': duration_str})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

# ==========================================
# ОБЩИЕ ФУНКЦИИ (ОТЧЕТЫ, СТАТУСЫ)
# ==========================================

@app.route('/api/get_report', methods=['POST'])
def get_report():
    data = request.json
    tech_login = data.get('tech')
    report_type = data.get('type')
    try:
        df = download_from_db()
        if df is None: return jsonify({'status': 'error', 'message': 'Ошибка БД'})
        df = merge_with_technicians()
        if df is None: return jsonify({'status': 'error', 'message': 'Ошибка обработки'})
        
        if report_type == 'general': report_text = generate_report(df)
        else: report_text = generate_tech_report(df, tech_login)
        
        html_report = report_text.replace('\n', '<br>')
        if not html_report: html_report = "<b>Чудово! Немає проблем.</b>"
        return jsonify({'status': 'success', 'html': html_report})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/get_all_active_tasks', methods=['GET'])
def get_all_active_tasks():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        tasks = []
        cur.execute("SELECT id, adres, zadaca, texnik FROM zadaci_all WHERE status = 'open'")
        for row in cur.fetchall(): tasks.append({'id': row[0], 'adres': row[1], 'info': row[2], 'who': row[3], 'type': 'task', 'table': 'zadaci_all'})
        cur.execute("SELECT id, adres, zamovnuk, texnik FROM zakazu_all WHERE status = 'open'")
        for row in cur.fetchall(): tasks.append({'id': row[0], 'adres': row[1], 'info': f"Карта: {row[2]}", 'who': row[3], 'type': 'card', 'table': 'zakazu_all'})
        cur.execute("SELECT id, adres, pricina, texnik FROM zadaci_srocno_all WHERE status = 'open'")
        for row in cur.fetchall(): tasks.append({'id': row[0], 'adres': row[1], 'info': f"🔥 {row[2]}", 'who': row[3], 'type': 'urgent', 'table': 'zadaci_srocno_all'})
        conn.close()
        return jsonify({'status': 'success', 'tasks': tasks})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/cancel_task', methods=['POST'])
def cancel_task():
    data = request.json
    table = data.get('table')
    task_id = data.get('id')
    if table not in ['zadaci_all', 'zakazu_all', 'zadaci_srocno_all']: return jsonify({'status': 'error'})
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"UPDATE {table} SET status = 'closed' WHERE id = %s", (task_id,))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': 'Скасовано!'})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

# ==========================================
# ФУНКЦИИ СУПЕРВИЗОРА
# ==========================================

@app.route('/api/get_all_tasks', methods=['POST'])
def get_all_tasks():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, id_terem, adres, zadaca, texnik, date_time_open, status FROM zadaci_all ORDER BY id DESC")
        rows = cur.fetchall()
        tasks = []
        for row in rows:
            tasks.append({'id': row[0], 'id_terem': row[1], 'adres': row[2], 'zadaca': row[3], 'texnik': row[4], 'date': row[5].strftime('%Y-%m-%d %H:%M') if row[5] else '', 'status': row[6]})
        conn.close()
        return jsonify({'status': 'success', 'tasks': tasks})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/super/get_orders', methods=['GET'])
def get_super_orders():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, texnik, adres, zakaz, date_time_open FROM stol_zakazov WHERE status = 'open' ORDER BY id DESC")
        rows = cur.fetchall()
        orders = []
        for row in rows:
            orders.append({'id': row[0], 'texnik': row[1], 'adres': row[2] if row[2] else 'Не вказано', 'zakaz': row[3], 'date': row[4].strftime('%d.%m %H:%M') if row[4] else ''})
        conn.close()
        return jsonify({'status': 'success', 'orders': orders})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/super/close_order', methods=['POST'])
def close_super_order():
    data = request.json
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE stol_zakazov SET status = 'closed' WHERE id = %s", (data.get('id'),))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/super/run_script', methods=['POST'])
@app.route('/api/super/run_script', methods=['POST'])
def run_super_script():
    data = request.json
    script_type = data.get('type')
    html_result = "Невідомий скрипт"
    
    try:
        if script_type == 'week5': 
            html_result = week5_ink.get_report()
        elif script_type == 'week1': 
            html_result = week1_ink.get_report()
        elif script_type == 'service':
            # 1. Запускаем скрипт (предполагаем, что он генерирует файл)
            if hasattr(service_glub_analitik, 'get_html_report'):
                # Если у скрипта есть функция возврата текста - используем её
                html_result = service_glub_analitik.get_html_report()
            else:
                # Если функции нет, просто импортируем (он выполнится при импорте или если вызвать run)
                # Если в скрипте нет функции main/run, он выполнится при import. 
                # Но import кэшируется, поэтому лучше использовать reload или вынести логику в функцию.
                # ДЛЯ ТЕБЯ: Читаем файл, который он создал
                
                # Пытаемся запустить функцию main() если есть, иначе просто читаем файл
                if hasattr(service_glub_analitik, 'main'):
                    service_glub_analitik.main()
                
                # Читаем файл результата
                try:
                    with open('service_glub_analitik.txt', 'r', encoding='utf-8') as f:
                        text_content = f.read()
                    # Превращаем переносы строк в <br> для HTML
                    html_result = f"<pre>{text_content}</pre>"
                except FileNotFoundError:
                    html_result = "⚠️ Скрипт спрацював, але файл service_glub_analitik.txt не знайдено."

        return jsonify({'status': 'success', 'html': html_result})
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

# --- НОВАЯ ФУНКЦИЯ: ЭКСПОРТ CSV ПРИ ВХОДЕ ---
@app.route('/api/super/export_mes_csv', methods=['POST'])
def export_mes_csv():
    try:
        conn = get_db_connection()
        if not conn: return jsonify({'status': 'error'})
        
        # Читаем таблицу в DataFrame
        try:
            df = pd.read_sql_query("SELECT * FROM mes_service_otchet", conn)
        except Exception:
            df = pd.DataFrame()
        
        conn.close()
        
        # Сохраняем (utf-8-sig для Excel)
        df.to_csv('mes_service_otchet.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV обновлен при входе супервизора")
        return jsonify({'status': 'success'})
    except Exception as e:
        print(f"❌ Ошибка экспорта CSV: {e}")
        return jsonify({'status': 'error', 'message': str(e)})
        
# --- API: СОЗДАТЬ ЗАДАЧУ С ТЕРМИНОМ (ДЛЯ TEXDIR) ---
@app.route('/api/create_termin_task', methods=['POST'])
def create_termin_task():
    data = request.json
    id_terem = data.get('id_terem')
    
    # 1. Ищем техника, который закреплен за аппаратом
    # (Используем APARATS_DB, который объявлен в начале файла)
    target_aparat = next((item for item in APARATS_DB if item["id"] == int(id_terem)), None)
    
    if not target_aparat:
        return jsonify({'status': 'error', 'message': 'Апарат не знайдено в базі!'})
        
    assigned_tech = target_aparat['tech'] # 'ruslan', 'igor' и т.д.

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            INSERT INTO zavdanya_termin (id_terem, adres, zavdanya, termin, texnik, date_time_open, status)
            VALUES (%s, %s, %s, %s, %s, NOW(), 'open')
        """
        cur.execute(query, (
            id_terem,
            data.get('adres'),
            data.get('zavdanya'),   # Текст задачи
            data.get('termin'),     # Дней (цифра)
            assigned_tech           # Техник (найденный автоматически)
        ))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'status': 'success', 'message': f'Завдання створено!\nВиконавець: {assigned_tech}'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, port=port, host='0.0.0.0')
```

---

## 📝 **Полная последовательность действий:**

### 1. **Создайте `requirements.txt`** (если еще не создали):
```
Flask==3.0.0
psycopg2-binary==2.9.9
python-dotenv==1.0.0
pandas==2.1.4
gunicorn==21.2.0