FROM python:3.13-slim

WORKDIR /app

# залежності окремим шаром — кешується, поки requirements не змінились
COPY common/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/library/main.py ./main.py

ENV SERVICE_NAME=library-service SERVICE_PORT=8000 PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
