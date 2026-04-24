# Start from an official Python image — this gives us a clean Linux environment with Python 3.11
# We don't need to install Python ourselves; it's already in this base image
FROM python:3.11-slim

# Set the working directory inside the container
# All subsequent commands run from here, and our code lives here
WORKDIR /app

# Copy requirements.txt first and install dependencies
# We do this before copying the rest of the code because Docker caches each step —
# if requirements.txt hasn't changed, Docker reuses the cached layer (much faster rebuilds)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Now copy all project files into the container
COPY . .

# Create the data and reports directories inside the container
RUN mkdir -p data reports

# Tell Docker that port 8000 will be used by this container (FastAPI)
EXPOSE 8000

# Default command: start the FastAPI server
# 0.0.0.0 means "listen on all network interfaces" — required inside a container
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
