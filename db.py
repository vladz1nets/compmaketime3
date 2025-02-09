import sqlite3
import json
import os

DB_PATH = "shops.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS shops (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    shop_name TEXT,
                    results TEXT
                 )''')
    conn.commit()
    conn.close()

def add_shop(user_id, shop_name, schedule_file):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Перевірка чи вже існує такий цех для користувача
    c.execute("SELECT * FROM shops WHERE user_id=? AND shop_name=?", (user_id, shop_name))
    if c.fetchone() is not None:
        conn.close()
        return False, "Цей цех вже існує."
    # Зберігаємо абсолютний шлях до файлу
    results_list = [os.path.abspath(schedule_file)]
    results_json = json.dumps(results_list)
    c.execute("INSERT INTO shops (user_id, shop_name, results) VALUES (?, ?, ?)", (user_id, shop_name, results_json))
    conn.commit()
    conn.close()
    return True, "Цех створено."

def update_shop(user_id, shop_name, schedule_file):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT results FROM shops WHERE user_id=? AND shop_name=?", (user_id, shop_name))
    row = c.fetchone()
    if row is None:
        conn.close()
        return False, "Цей цех не знайдено."
    results_json = row[0]
    results_list = json.loads(results_json) if results_json else []
    # Зберігаємо абсолютний шлях до файлу
    results_list.append(os.path.abspath(schedule_file))
    new_results_json = json.dumps(results_list)
    c.execute("UPDATE shops SET results=? WHERE user_id=? AND shop_name=?", (new_results_json, user_id, shop_name))
    conn.commit()
    conn.close()
    return True, "Цех оновлено."

def get_user_shops(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT shop_name, results FROM shops WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows

# Ініціалізація бази при імпорті модуля
if __name__ == "__main__":
    init_db()

