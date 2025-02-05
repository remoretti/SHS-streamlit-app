# Use an official Python runtime image.
FROM python:3.10-slim

# Install build dependencies.
# Run apt-get update first and then install gcc and build-essential.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        build-essential \
        libffi-dev \
        libssl-dev \
        && rm -rf /var/lib/apt/lists/*

# Ensure output is logged immediately.
ENV PYTHONUNBUFFERED=1

# Set working directory.
WORKDIR /app

# Copy requirements file and install dependencies.
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of the application code.
COPY . /app/

# Expose the port (default for Streamlit is 8501).
EXPOSE 8501

# Run the Streamlit app.
CMD ["streamlit", "run", "streamlit_app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
