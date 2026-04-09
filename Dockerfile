FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV RUN_MODE=kafka
ENV LOG_LEVEL=INFO

CMD ["python", "main.py"]