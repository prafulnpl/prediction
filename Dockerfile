FROM python:3.9.16

# Set the working directory
WORKDIR /src

# Install system dependencies (including Tor)
RUN apt-get update && apt-get install -y \
    tor \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy and install Python dependencies
COPY requirement.txt .
RUN pip install -r requirement.txt

# Copy the application code
COPY . .

# Expose Tor's default SOCKS proxy port
EXPOSE 9050

# Start Tor in the background before running the app
CMD service tor start && python main.py
