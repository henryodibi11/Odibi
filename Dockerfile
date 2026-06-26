FROM python:3.11-slim

# Install Java (required for Spark/PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# Install dependencies first (cached layer — only rebuilds when pyproject.toml changes)
COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev,spark,polars,thermodynamics,pandas]"

# Copy project
COPY . .
RUN pip install --no-cache-dir -e ".[dev,spark,polars,thermodynamics,pandas]"

CMD ["bash"]
