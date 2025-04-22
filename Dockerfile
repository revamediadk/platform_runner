FROM python:3.11-slim
ARG CACHEBUST=1

WORKDIR /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1


COPY . .

RUN pip install -r requirements.txt

CMD ["python", "main.py"]
