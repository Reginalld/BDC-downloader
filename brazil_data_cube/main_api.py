from fastapi import FastAPI
from brazil_data_cube.api.models import DownloadRequest
from brazil_data_cube.api.downloader import iniciar_download

app = FastAPI(
    title="STAC Downloader API",
    description="API para acionar downloads de imagens via Brazil Data Cube",
    version="1.0"
)

@app.post("/download")
def download(request: DownloadRequest):
    return iniciar_download(request)
