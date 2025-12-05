import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd

# Функция для создания таблицы техников
def create_technicians_table():
    technicians = [
        {'id': 1, 'id_terem': '', 'adres': '', 'texik': 'ruslan', 'time_first': ''},
        {'id': 2, 'id_terem': '', 'adres': '', 'texik': 'dmutro', 'time_first': ''},
        {'id': 3, 'id_terem': '', 'adres': '', 'texik': 'igor', 'time_first': ''}
    ]
    df = pd.DataFrame(technicians)
    df.to_csv('vremya_vuxoda_res.csv', index=False, encoding='utf-8-sig')
    return df

# Функция для добавления записи в таблицу техников
def update_technician_record(df, technician_name, address, time_first, terem_id=''):
    # Находим индекс техника
    idx = df[df['texik'] == technician_name].index
    if len(idx) > 0:
        df.at[idx[0], 'adres'] = address
        df.at[idx[0], 'time_first'] = time_first
        df.at[idx[0], 'id_terem'] = terem_id
    return df

# Функция для поиска техника по адресу (заглушка - нужно адаптировать под вашу логику)
def find_technician_by_address(address):
    # Здесь должна быть ваша логика определения техника по адресу
    # Пока возвращаем первого техника как пример
    # В реальности нужно сопоставить адрес с закрепленными адресами техников
    address_lower = address.lower()
    
    if 'винники' in address_lower:
        return 'ruslan'
    elif 'львів' in address_lower:
        return 'dmutro'
    elif 'київ' in address_lower:
        return 'igor'
    else:
        # Если не нашли, распределяем поочередно
        return 'ruslan'  # временно возвращаем первого

# --- ДЛИТЕЛЬНАЯ ОПЕРАЦИЯ: ЭКСПОРТ CSV ---
def run_csv_export():
    """Фоновая функция для выполнения тяжелой операции."""
    print("⏳ Начинаем фоновый экспорт CSV...")
    try:
        conn = get_db_connection() # Убедитесь, что эта функция корректна
        
        # ВАША ЛОГИКА ЭКСПОРТА (пример из предыдущего шага)
        df = pd.read_sql_query("SELECT * FROM mes_service_otchet", conn)
        
        # Здесь должна быть логика сохранения df в файл (не меняйте ее)
        # df.to_csv('путь/к/отчету.csv', index=False, encoding='utf-8-sig') 
        
        print(f"✅ CSV обновлен при входе супервизора. Строк: {len(df)}")
        conn.close()
    except Exception as e:
        print(f"❌ Ошибка в фоновом экспорте CSV: {e}", file=sys.stderr)
        
    print("✅ Фоновый экспорт CSV завершен.")

def main():
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 15)
    
    try:
        print("🔐 Авторизация...")
        driver.get("https://soliton.net.ua/water/baza/")
        time.sleep(3)
        
        # Ввод логина и пароля
        driver.find_element(By.NAME, "auth_login").send_keys("Service_zenya")
        driver.find_element(By.NAME, "auth_pass").send_keys("zenya")
        driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
        
        # Ожидание авторизации
        auth_marker_xpath = "//a[@href='/water/baza/?fid=2&subsection=stat']"
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, auth_marker_xpath)))
            print("✅ Авторизация успешна.")
        except TimeoutException:
            print("❌ Ошибка: Не удалось авторизоваться.")
            return
        
        time.sleep(3)
        
        # --- Суммирование столбцов ---
        print("\n💰 Расчет доходов...")
        
        # Ищем таблицу
        table_xpath = "//table[.//th[contains(text(), 'Виторг')]]"
        try:
            table = wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))
        except TimeoutException:
            print("❌ Не найдена таблица с доходами.")
            return
        
        # Находим индексы столбцов
        headers = table.find_elements(By.XPATH, ".//tr[1]/th")
        cash_col_index = -1
        cashless_col_index = -1
        
        for i, header in enumerate(headers):
            header_text = header.text
            if 'Виторг,' in header_text and 'BN' not in header_text:
                cash_col_index = i
            elif 'Виторг BN,' in header_text:
                cashless_col_index = i
        
        if cash_col_index == -1 or cashless_col_index == -1:
            print("❌ Не найдены нужные столбцы.")
            return
        
        # Суммируем значения
        cash_total = 0
        cashless_total = 0
        
        rows = table.find_elements(By.XPATH, ".//tr[position()>1]")
        for row in rows:
            tds = row.find_elements(By.TAG_NAME, "td")
            if len(tds) > max(cash_col_index, cashless_col_index):
                # Денежный доход
                try:
                    cash_text = tds[cash_col_index].text.strip()
                    if cash_text:
                        cash_total += float(cash_text.replace(',', '.'))
                except ValueError:
                    pass
                
                # Безналичный доход
                try:
                    cashless_text = tds[cashless_col_index].text.strip()
                    if cashless_text:
                        cashless_total += float(cashless_text.replace(',', '.'))
                except ValueError:
                    pass
        
        # Вывод результатов
        print(f"📊 Доход за добу нал.: {cash_total:.2f} грн")
        print(f"📊 Доход за добу б.нал.: {cashless_total:.2f} грн")
        
        # Расчет процентного соотношения
        total_income = cash_total + cashless_total
        if total_income > 0:
            cash_percent = (cash_total / total_income) * 100
            cashless_percent = (cashless_total / total_income) * 100
            print(f"📈 Соотношение: Наличные {cash_percent:.1f}% | Безналичные {cashless_percent:.1f}%")
        
        # Встраиваем результаты в страницу с помощью JavaScript
        js_code = f"""
        // Создаем контейнер для результатов
        var resultDiv = document.createElement('div');
        resultDiv.id = 'income_results';
        resultDiv.style.cssText = 'position: fixed; top: 10px; right: 10px; background: white; border: 2px solid #333; padding: 10px; z-index: 10000;';
        
        resultDiv.innerHTML = `
            <h3 style="margin: 0 0 10px 0;">Доходы за день</h3>
            <p style="margin: 5px 0;">Наличные: <strong>{cash_total:.2f} грн</strong></p>
            <p style="margin: 5px 0;">Безналичные: <strong>{cashless_total:.2f} грн</strong></p>
            <p style="margin: 5px 0;">Соотношение: <strong>{cash_percent:.1f}% / {cashless_percent:.1f}%</strong></p>
        `;
        
        document.body.appendChild(resultDiv);
        """
        
        driver.execute_script(js_code)
        print("✅ Результаты встроены в страницу")
        
        time.sleep(3)
        
        # --- Переход на страницу Датчики ---
        print("\n📊 Переход на страницу 'Датчики'...")
        sensors_link = driver.find_element(By.XPATH, "//a[contains(@href, 'section=sensors&fid=2')]")
        sensors_link.click()
        time.sleep(3)
        
        # --- Переход на страницу Система ---
        print("🔧 Переход на страницу 'Система'...")
        system_link = driver.find_element(By.XPATH, "//a[contains(@href, 'sensors_stat=system')]")
        system_link.click()
        time.sleep(2)
        
        # --- Выбор сервисного режима ---
        print("⚙️ Выбор 'Сервисный режим'...")
        select_element = driver.find_element(By.NAME, "system")
        select = Select(select_element)
        select.select_by_value("Service")
        time.sleep(0.5)
        
        # --- Нажатие кнопки "Вивести" ---
        print("📋 Получение данных...")
        submit_button = driver.find_element(By.XPATH, "//input[@type='submit' and @value='Вивести']")
        submit_button.click()
        time.sleep(3)
        
        # --- Проверка наличия таблицы ---
        print("🔍 Проверка данных сервисного режима...")
        df_technicians = create_technicians_table()
        
        try:
            # Ищем таблицу с данными
            service_table = driver.find_element(By.XPATH, "//table[.//td[contains(text(), 'Service')]]")
            rows = service_table.find_elements(By.XPATH, ".//tr[position()>1]")
            
            if rows:
                print(f"✅ Найдено {len(rows)} записей сервисного режима")
                
                for row in rows:
                    tds = row.find_elements(By.TAG_NAME, "td")
                    if len(tds) >= 3:
                        time_str = tds[0].text.strip()  # Время
                        service_status = tds[1].text.strip()  # Service ON/OFF
                        address = tds[2].text.strip()  # Адрес
                        
                        # Проверяем, что это именно Service
                        if 'Service' in service_status:
                            # Определяем техника по адресу
                            technician = find_technician_by_address(address)
                            
                            # Обновляем запись техника
                            df_technicians = update_technician_record(
                                df_technicians, 
                                technician, 
                                address, 
                                time_str
                            )
                            
                            print(f"   - {technician}: {address} в {time_str}")
                
                # Сохраняем обновленную таблицу
                df_technicians.to_csv('vremya_vuxoda_res.csv', index=False, encoding='utf-8-sig')
                print("✅ Файл vremya_vuxoda_res.csv обновлен")
                
                # Встраиваем статусы техников в страницу
                status_js = """
                // Создаем контейнер для статусов техников
                var statusDiv = document.createElement('div');
                statusDiv.id = 'technician_status';
                statusDiv.style.cssText = 'position: fixed; top: 200px; right: 10px; background: white; border: 2px solid #333; padding: 10px; z-index: 10000;';
                
                // Получаем статусы из CSV
                var statusHTML = '<h3 style="margin: 0 0 10px 0;">Статус техников</h3>';
                """
                
                # Добавляем статусы каждого техника
                for _, row in df_technicians.iterrows():
                    tech_name = row['texik']
                    status = "Работает" if row['time_first'] else "Не было"
                    color = "green" if status == "Работает" else "gray"
                    
                    status_js += f'statusHTML += \'<p style="margin: 5px 0;">{tech_name}: <span style="color: {color}"><strong>{status}</strong></span></p>\';'
                
                status_js += """
                statusDiv.innerHTML = statusHTML;
                document.body.appendChild(statusDiv);
                """
                
                driver.execute_script(status_js)
                print("✅ Статусы техников встроены в страницу")
                
            else:
                print("ℹ️ Записей сервисного режима за сегодня нет")
                
        except NoSuchElementException:
            print("ℹ️ Таблица сервисного режима не найдена (возможно, записей нет)")
        
        print("\n🎉 Программа успешно выполнена!")
        print("📁 Создан файл: vremya_vuxoda_res.csv")
        
        # Оставляем браузер открытым на 30 секунд для просмотра результатов
        print("\n⏳ Браузер останется открытым на 30 секунд...")
        time.sleep(30)
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🔄 Закрытие браузера...")
        driver.quit()

if __name__ == "__main__":
    main()