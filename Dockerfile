FROM python:3.11-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


FROM python:3.11-slim

# Non-root user for security
RUN adduser --disabled-password --gecos "" botuser

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Copy source (only what .dockerignore allows)
COPY . .

ENV PYTHONPATH=/app/src

# Logs directory owned by botuser
RUN mkdir -p logs && chown -R botuser:botuser /app

USER botuser

# Health: verify the .alive file was written recently (< 5 min)
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import time, os; exit(0 if os.path.exists('logs/.alive') and time.time() - os.path.getmtime('logs/.alive') < 300 else 1)"

CMD ["python", "-u", "main.py"]
