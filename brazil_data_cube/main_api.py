import asyncio
import uuid
from datetime import datetime
from pathlib import Path

import aiofiles
from fastapi import FastAPI, HTTPException

from brazil_data_cube.api.downloader import start_download
from brazil_data_cube.api.models import DownloadRequest
from brazil_data_cube.utils.task_manager import (get_task_status,
                                                 start_download_task)

app = FastAPI(
    title="STAC Downloader API",
    description="API para acionar downloads de imagens via Brazil Data Cube",
    version="1.0"
)


@app.post("/download")
async def download(request: DownloadRequest):
    exec_id = str(uuid.uuid4())[:8]
    task_id = await asyncio.to_thread(
            start_download_task, start_download,
            request, exec_id=exec_id
            )
    return {"mensagem": "Download agendado", "task_id": task_id}


@app.get("/status/{task_id}")
async def status(task_id: str):
    return get_task_status(task_id)


@app.get("/logs/{task_id}")
async def logs(task_id: str):
    task = get_task_status(task_id)

    if task["status"] == "não encontrado" and task_id != "minio":
        raise HTTPException(status_code=404, detail="Tarefa não encontrada.")

    if task_id == "minio":
        log_path = Path("log") / "upload_minio.txt"
    else:
        try:
            satellite = task["satellite"]
            start_date = task["start_date"]
            year_month = datetime.strptime(start_date, "%Y-%m-%d") \
                .strftime("%Y-%m")

            log_path = Path("log") / satellite / year_month / f"{task_id}.log"
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail="Metadados incompletos para este task_id."
                )
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Data em formato inválido nos metadados."
                )

    if not log_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Arquivo de log não encontrado: {log_path}"
            )

    try:
        async with aiofiles.open(log_path, encoding="utf-8") as f:
            conteudo = await f.read()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao ler o log: {str(e)}"
            )

    return {"log": conteudo}
