FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir fastapi uvicorn httpx python-multipart

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "python node.py 8001 & python node.py 8002 & python node.py 8003 & sleep 1 && python coordinator.py"]
