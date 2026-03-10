"""
run.py — вечный лончер. Запускай: python run.py
При падении main.py — автоматически перезапускает.
"""
import subprocess, sys, time, os

DELAY  = int(os.getenv("RESTART_DELAY", "5"))
script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.py")
count  = 0

print("[run.py] JARVIS запущен. Ctrl+C для остановки.\n")

while True:
    try:
        code = subprocess.run([sys.executable, script]).returncode
        count += 1
        print(f"[run.py] Процесс завершился (код {code}). Перезапуск #{count} через {DELAY}с...")
        time.sleep(DELAY)
    except KeyboardInterrupt:
        print("\n[run.py] Остановлен. До свидания, Сэр.")
        sys.exit(0)
    except Exception as e:
        print(f"[run.py] Ошибка: {e}. Перезапуск через {DELAY}с...")
        time.sleep(DELAY)
