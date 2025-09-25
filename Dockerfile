# Dockerfile for Render deploy (aiogram 2.x, webhook mode)
FROM python:3.11-slim

WORKDIR /app

# Install pip first (latest avoids resolution issues)
RUN pip install --no-cache-dir --upgrade pip

# Install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy bot source
COPY . /app

# Create data dir for sqlite & backups
RUN mkdir -p /app/data

# Environment defaults (can be overridden in Render dashboard)
ENV DB_PATH=/app/data/database.sqlite3
ENV JOB_DB_PATH=/app/data/jobs.sqlite
ENV PORT=10000

# Run bot
CMD ["python", "bot.py"]