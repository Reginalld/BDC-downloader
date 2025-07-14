from fastapi import FastAPI
from brazil_data_cube.api.models import DownloadRequest
from brazil_data_cube.api.downloader import iniciar_download, estado_execucao
from pathlib import Path
from datetime import datetime
from brazil_data_cube.utils.task_manager import start_download_task, get_task_status


app = FastAPI(
    title="STAC Downloader API",
    description="API para acionar downloads de imagens via Brazil Data Cube",
    version="1.0"
)

@app.post("/download")
def download(request: DownloadRequest):
    task_id = start_download_task(iniciar_download, request)
    return {"mensagem": "Download agendado", "task_id": task_id}

@app.get("/status")
async def status():
    return {"status": estado_execucao.get_status()}

@app.get("/status/{task_id}")
def status(task_id: str):
    return get_task_status(task_id)


@app.get("/logs")
async def logs(satelite: str, start_date: str):

    ano_mes = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
    log_path = Path("log") / satelite / ano_mes / "execucao.log"

    if satelite == "minio":
        log_path = Path("log") / "upload_minio.txt"

    if not log_path.exists():
        return {"mensagens": f"Arquivo de log não encontrado: {log_path}"}
    
    with open(log_path, "r") as f:
        conteudo = f.read()

    return {"log": conteudo}