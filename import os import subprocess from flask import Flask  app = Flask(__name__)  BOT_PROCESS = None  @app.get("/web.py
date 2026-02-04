import os
import subprocess
from flask import Flask

app = Flask(__name__)

BOT_PROCESS = None

@app.get("/health")
def health():
    # 200 OK — достаточно для UptimeRobot
    return "ok", 200

def start_bot():
    global BOT_PROCESS
    if BOT_PROCESS is None or BOT_PROCESS.poll() is not None:
        # запускаем твоего бота как отдельный процесс
        BOT_PROCESS = subprocess.Popen(["python", "bot.py"])

if __name__ == "__main__":
    start_bot()
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
