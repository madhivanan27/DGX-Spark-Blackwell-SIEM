FROM nvcr.io/nvidia/morpheus/morpheus:25.06-runtime
WORKDIR /app
COPY . /app
CMD ["python", "parallel_runner.py"]
