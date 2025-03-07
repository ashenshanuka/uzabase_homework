# Use the official Miniconda3 image (Debian based)
FROM continuumio/miniconda3:latest

# Create a new conda environment with Python 3.11 and install required packages.
# The packages list has been interpreted as individual packages:
# pyspark, pytorch, numpy, pandas, scipy, scikit-learn, polars, orjson,
# awswrangler (for pyarrow.awswrangler), transformers, accelerate, duckdb, neo4j,
# s3fs, umap-learn, smart-open, onnxruntime, spacy, seqeval, gensim, numba,
# sqlalchemy, and pytest.
# Create a new conda environment with Python 3.11 and install required packages.
RUN conda create -n myenv python=3.11 -y && \
    conda install -n myenv -c conda-forge \
        pyspark \
        pytorch \
        numpy \
        pandas \
        scipy \
        scikit-learn \
        polars \
        orjson \
        awswrangler \
        transformers \
        accelerate \
        duckdb \
        s3fs \
        umap-learn \
        smart-open \
        onnxruntime \
        spacy \
        seqeval \
        gensim \
        numba \
        sqlalchemy \
        pytest -y && \
    pip install neo4j && \
    conda clean -afy

# Update the PATH so that the conda environment is used.
ENV PATH /opt/conda/envs/myenv/bin:$PATH
# Ensure that PySpark uses the correct Python interpreter.
ENV PYSPARK_PYTHON /opt/conda/envs/myenv/bin/python
ENV PYSPARK_DRIVER_PYTHON /opt/conda/envs/myenv/bin/python

# Set the working directory to /app
WORKDIR /app

# Copy all project files into the container.
COPY . .

# Create logs directory and generate pip_list.txt with the installed packages.
RUN mkdir -p logs && pip list > logs/pip_list.txt


# Run the pipeline using the provided bash script.
CMD ["bash", "script/run.sh"]
