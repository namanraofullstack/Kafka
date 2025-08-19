FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Copy kafget script into PATH
COPY kafget /usr/local/bin/kafget
RUN chmod +x /usr/local/bin/kafget

# Expose port for Django
EXPOSE 8000

# Default command for Django API
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "simpleapi.wsgi:application"]
