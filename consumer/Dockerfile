FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python
COPY consumer/consumer.py .
USER 10001
CMD ["python", "-u", "consumer.py"]
