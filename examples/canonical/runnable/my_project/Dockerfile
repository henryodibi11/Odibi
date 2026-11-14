FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (e.g., for pyodbc)
# RUN apt-get update && apt-get install -y unixodbc-dev

# Install Odibi
RUN pip install odibi[all]

# Copy project files
COPY . /app

# Default command
CMD ["odibi", "run", "odibi.yaml"]
