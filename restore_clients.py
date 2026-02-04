import os
import re
from datetime import datetime
import psycopg2

REPORT_PATH = "clients_report.txt"

PREFIXES = (
    "🆔 ID:", "📅 Дата:", "🧑‍🤝‍🧑 Пол:", "📊 Возраст:", "🛒 Посещения:",
    "👍 Нравится:", "👎 Не нравится:", "💡 Предложения:"
)

def parse_clients(text: str):
    lines = [ln.rstrip("\n") for ln in text.splitlines()]
    clients = []
    cur = None
    cur_field = None

    def flush():
        nonlocal cur
        if cur and cur.get("user_id"):
            # чистим переносы/пробелы
            for k in ("appreciate", "dislike", "improve"):
                if cur.get(k) is not None:
                    cur[k] = re.sub(r"\s+\n", "\n", cur[k]).strip()
            clients.append(cur)
        cur = None

    def start_new(name, username):
        nonlocal cur, cur_field
        flush()
        cur = {
            "user_id": None,
            "username": None if (username is None or username.lower() == "none") else username,
            "full_name": name.strip(),
            "timestamp": None,
            "gender": None,
            "age_group": None,
            "visit_freq": None,
            "appreciate": None,
            "dislike": None,
            "improve": None,
        }
        cur_field = None

    for ln in lines:
        # пропускаем "шапки" чата
        if ln.startswith('"ДЫМ"') or ln.startswith("📋 Подробный отчёт"):
            continue

        # разделитель
        if ln.strip().startswith("===="):
            cur_field = None
            continue

        # старт клиента: 👤 Имя (@username)
        m = re.match(r"^👤\s*(.+?)\s*\(@(.*?)\)\s*$", ln)
        if m:
            start_new(m.group(1), m.group(2))
            continue

        if cur is None:
            continue  # до первого клиента

        # поля
        if ln.startswith("🆔 ID:"):
            cur["user_id"] = int(ln.split(":", 1)[1].strip())
            cur_field = None
        elif ln.startswith("📅 Дата:"):
            val = ln.split(":", 1)[1].strip()
            try:
                cur["timestamp"] = datetime.fromisoformat(val)
            except Exception:
                cur["timestamp"] = None
            cur_field = None
        elif ln.startswith("🧑‍🤝‍🧑 Пол:"):
            cur["gender"] = ln.split(":", 1)[1].strip()
            cur_field = None
        elif ln.startswith("📊 Возраст:"):
            cur["age_group"] = ln.split(":", 1)[1].strip()
            cur_field = None
        elif ln.startswith("🛒 Посещения:"):
            cur["visit_freq"] = ln.split(":", 1)[1].strip()
            cur_field = None
        elif ln.startswith("👍 Нравится:"):
            cur["appreciate"] = ln.split(":", 1)[1].strip()
            cur_field = "appreciate"
        elif ln.startswith("👎 Не нравится:"):
            cur["dislike"] = ln.split(":", 1)[1].strip()
            cur_field = "dislike"
        elif ln.startswith("💡 Предложения:"):
            cur["improve"] = ln.split(":", 1)[1].strip()
            cur_field = "improve"
        else:
            # продолжение многострочного текста (у тебя есть такие случаи)
            if cur_field in ("appreciate", "dislike", "improve") and ln.strip():
                cur[cur_field] = (cur[cur_field] or "") + "\n" + ln.strip()

    flush()
    return clients

def main():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in environment")

    with open(REPORT_PATH, "r", encoding="utf-8") as f:
        report = f.read()

    clients = parse_clients(report)
    print(f"Parsed clients: {len(clients)}")

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                # важно: не ломаем существующие записи — заполняем только пустые поля
                for c in clients:
                    cur.execute("""
                        INSERT INTO clients (
                            user_id, username, full_name,
                            appreciate, dislike, improve,
                            gender, age_group, visit_freq,
                            is_admin, timestamp
                        ) VALUES (
                            %(user_id)s, %(username)s, %(full_name)s,
                            %(appreciate)s, %(dislike)s, %(improve)s,
                            %(gender)s, %(age_group)s, %(visit_freq)s,
                            FALSE, %(timestamp)s
                        )
                        ON CONFLICT (user_id) DO UPDATE SET
                            username = COALESCE(EXCLUDED.username, clients.username),
                            full_name = COALESCE(EXCLUDED.full_name, clients.full_name),

                            appreciate = CASE
                                WHEN clients.appreciate IS NULL OR clients.appreciate = '' THEN EXCLUDED.appreciate
                                ELSE clients.appreciate END,

                            dislike = CASE
                                WHEN clients.dislike IS NULL OR clients.dislike = '' THEN EXCLUDED.dislike
                                ELSE clients.dislike END,

                            improve = CASE
                                WHEN clients.improve IS NULL OR clients.improve = '' THEN EXCLUDED.improve
                                ELSE clients.improve END,

                            gender = CASE
                                WHEN clients.gender IS NULL OR clients.gender = '' THEN EXCLUDED.gender
                                ELSE clients.gender END,

                            age_group = CASE
                                WHEN clients.age_group IS NULL OR clients.age_group = '' THEN EXCLUDED.age_group
                                ELSE clients.age_group END,

                            visit_freq = CASE
                                WHEN clients.visit_freq IS NULL OR clients.visit_freq = '' THEN EXCLUDED.visit_freq
                                ELSE clients.visit_freq END,

                            timestamp = COALESCE(EXCLUDED.timestamp, clients.timestamp);
                    """, c)

        print("OK: clients restored (safe upsert).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
