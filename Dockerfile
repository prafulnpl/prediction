# Use official Python image
FROM python:3.9.16

# Set the working directory
WORKDIR /src

# Install system dependencies (PostgreSQL client + Tor + curl for health checks)
RUN apt-get update && apt-get install -y \
    postgresql-client \
    tor \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy only requirement.txt first to leverage Docker cache
COPY requirement.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirement.txt

# Copy application code and startup scripts
COPY . .
COPY start-tor.sh /start-tor.sh
RUN chmod +x /start-tor.sh

# Expose Flask app port
EXPOSE 5000

# Start Tor and application
CMD ["/start-tor.sh", "python", "main.py"]