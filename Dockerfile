# Use the official Dataflow Python base image for the correct architecture
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Set working directory
WORKDIR /dataflow/template

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the files needed for the Dataflow template
COPY main.py ./main.py

# Set the template entry point to main.py in root directory
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/dataflow/template/main.py"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories with proper permissions for template launcher
RUN mkdir -p /var/log/dataflow/template_launcher && \
    chmod 777 /var/log/dataflow && \
    chmod 777 /var/log/dataflow/template_launcher

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash dataflow
RUN chown -R dataflow:dataflow /dataflow/template

# Keep running as root for template launcher compatibility
# USER dataflow

# Default command (will be overridden by Dataflow)
CMD ["python", "-m", "src.dataflow.dataflow_template_test"] 