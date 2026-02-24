# Use a small Python base image (slim = smaller size)
FROM python:3.10-slim

# Install OS-level packages needed by some Python libraries (like pandas, lxml, etc.)
# build-essential = compiler tools
# libxml2-dev, libxslt1-dev = common dependencies for XML/HTML parsing libs
RUN apt-get update \
 && apt-get install -y build-essential libxml2-dev libxslt1-dev \
 && rm -rf /var/lib/apt/lists/*

# Set working directory inside the container
WORKDIR /app

# Copy requirements file first (better caching)
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files into the container
COPY . .

# When the container starts, run your Python script
CMD ["python", "handlers/main.py"]