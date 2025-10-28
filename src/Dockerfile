FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
COPY ./src /app/src

RUN pip install --default-timeout=100 --upgrade pip
RUN pip install --default-timeout=100 -r requirements.txt


ENTRYPOINT ["python", "-m", "src.main"]