FROM python:3.10-slim

WORKDIR /app
COPY ../requirements.txt /app

RUN pip install --default-timeout=100 --upgrade pip
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt

CMD ["sleep", "infinity"]