FROM python:3.11-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Set environment variables
ENV FLASK_ENV=production
ENV BOOTSTRAP_SERVERS=kafka:9092

# Expose port 5000
EXPOSE 5000

# Start Gunicorn server binding to 0.0.0.0 so container port is exposed
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "kafka_util:app"]
