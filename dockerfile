FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.5

WORKDIR /app

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["gunicorn", "main_api:app", "-k", "uvicorn.workers.UvicornWorker"]
