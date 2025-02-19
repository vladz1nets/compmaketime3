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
    c.execute("SELECT * FROM shops WHERE user_id=? AND shop_name=?", (user_id, shop_name))
    if c.fetchone() is not None:
        conn.close()
        return False, "Цех вже існує."
    results_list = [os.path.abspath(schedule_file)]
    results_json = json.dumps(results_list)
    c.execute("INSERT INTO shops (user_id, shop_name, results) VALUES (?, ?, ?)",
              (user_id, shop_name, results_json))
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
        return False, "Цех не знайдено."
    results_json = row[0]
    results_list = json.loads(results_json) if results_json else []
    results_list.append(os.path.abspath(schedule_file))
    new_results_json = json.dumps(results_list)
    c.execute("UPDATE shops SET results=? WHERE user_id=? AND shop_name=?",
              (new_results_json, user_id, shop_name))
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

def delete_shop(user_id, shop_name):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # Отримуємо результати, щоб видалити пов’язані файли
        c.execute("SELECT results FROM shops WHERE user_id=? AND shop_name=?", (user_id, shop_name))
        row = c.fetchone()
        if row is None:
            conn.close()
            return False, "Цех не знайдено."
        results_json = row[0]
        results_list = json.loads(results_json) if results_json else []
        # Видаляємо файли результатів
        for result_file in results_list:
            if os.path.exists(result_file):
                os.remove(result_file)
        # Видаляємо запис з бази даних
        c.execute("DELETE FROM shops WHERE user_id=? AND shop_name=?", (user_id, shop_name))
        conn.commit()
        conn.close()
        return True, "Цех видалено."
    except Exception as e:
        return False, str(e)

if __name__ == "__main__":
    init_db()


