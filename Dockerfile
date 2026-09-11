FROM public.ecr.aws/dataminded/spark-k8s-glue:v4.0.1-hadoop-3.4.2-v4

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

# Add the project code and its dependencies to the image.
# `pip install .` reads pyproject.toml, so it installs capstonellm + its deps in one step.
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip3 install --no-cache-dir .
