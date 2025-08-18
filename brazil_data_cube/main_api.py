import asyncio
import uuid
from datetime import datetime
from pathlib import Path

import aiofiles
from fastapi import FastAPI, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from contextlib import asynccontextmanager

from brazil_data_cube.api.downloader import start_download
from brazil_data_cube.api.database import get_db, engine
from brazil_data_cube.api.models.models_db import Base, User
from brazil_data_cube.api.models.models_download import DownloadRequest
from brazil_data_cube.api.security.security import (authenticate_user, create_access_token,
                                                    get_current_user,get_password_hash)
from brazil_data_cube.utils.task_manager import (get_task_status,
                                                 start_download_task)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

app = FastAPI(
    lifespan=lifespan,
    title="STAC Downloader API",
    description="API para acionar downloads de imagens via Brazil Data Cube",
    version="1.0"
)

@app.post("/create_user")
async def create_user(username: str, password: str, db: AsyncSession = Depends(get_db)):
    stmt = await db.execute(select(User).where(User.username == username))
    existing_user = stmt.scalar_one_or_none()
    if existing_user:
        raise HTTPException(status_code=400, detail="Usuário já existe")

    new_user = User(username=username, hashed_password=get_password_hash(password))
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)
    return {"msg": f"Usuário {new_user.username} criado com sucesso"}

@app.post("/token")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
):
    user = await authenticate_user(form_data.username, form_data.password, db)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuário ou senha inválidos",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/download")
async def download(request: DownloadRequest, user: dict = Depends(get_current_user)):
    exec_id = str(uuid.uuid4())[:8]
    task_id = await asyncio.to_thread(
            start_download_task, start_download,
            request, exec_id=exec_id
            )
    return {"mensagem": "Download agendado", "task_id": task_id}


@app.get("/status/{task_id}")
async def status(task_id: str, user: dict = Depends(get_current_user)):
    return get_task_status(task_id)


@app.get("/logs/{task_id}")
async def logs(task_id: str, user: dict = Depends(get_current_user)):
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
