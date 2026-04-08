FROM python:3.12-slim

WORKDIR /app

# Системные зависимости
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libmagic1 \
    antiword \
    unrar-free \
    p7zip-full \
    && rm -rf /var/lib/apt/lists/*

# Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скачиваем spaCy модель для русского
RUN python -m spacy download ru_core_news_sm

# Playwright
RUN playwright install chromium --with-deps

# Копируем код
COPY . .

# Создаём директории
RUN mkdir -p data logs backups uploads/temp

EXPOSE 8000

CMD ["python", "main.py"]
