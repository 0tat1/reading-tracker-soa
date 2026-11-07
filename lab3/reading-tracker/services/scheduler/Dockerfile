FROM python:3.13-slim

WORKDIR /app
COPY common/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/scheduler/main.py ./main.py

ENV SERVICE_NAME=scheduler PYTHONUNBUFFERED=1
CMD ["python", "main.py"]
