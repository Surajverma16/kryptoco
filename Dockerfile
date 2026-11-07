FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y build-essential curl && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your project files (excluding .env and old DB)
COPY core/ ./core/
COPY main.py .
COPY app.py .

# Create writable data directory for SQLite WAL mode
RUN mkdir -p /app/data && chmod -R 777 /app/data

EXPOSE 7860

# Run Streamlit on Hugging Face's default port (7860), which auto-starts main.py
CMD ["streamlit", "run", "app.py", "--server.port=7860", "--server.address=0.0.0.0"]