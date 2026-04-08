FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY data ./data
COPY main.py ./main.py

ENV PYTHONPATH=/app

# Default: show help / keep container alive by running a module (change as needed)
CMD ["python", "-c", "print('Container built. Run: python -m src.model.train_model (or other modules)')"]
