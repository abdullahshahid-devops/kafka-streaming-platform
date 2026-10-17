FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python
COPY producer/producer.py .
USER 10001
CMD ["python", "-u", "producer.py"]
