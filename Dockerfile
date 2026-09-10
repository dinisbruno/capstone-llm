FROM public.ecr.aws/dataminded/spark-k8s-glue:v4.0.1-hadoop-3.4.2-v4

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

# 1. Install the Python dependencies first (own layer -> cached across code changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy and install the project itself (deps already installed above)
COPY pyproject.toml README.md ./
COPY src/ src/
RUN pip install --no-cache-dir --no-deps .
