# brazil_data_cube/task_manager.py

import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

executor = ThreadPoolExecutor(max_workers=1)

task_status: Dict[str, Dict] = {}
task_status_lock = threading.Lock()

def start_download_task(iniciar_download_fn, request, exec_id: str):

    task_id = exec_id

    with task_status_lock:
        task_status[task_id] = {
            "status": "esperando",
            "satelite": request.satelite,
            "start_date": request.start_date
            }

    def task():
        try:
            with task_status_lock:
                task_status[task_id]["status"] = "baixando"

            result = iniciar_download_fn(request, exec_id)

            with task_status_lock:
                task_status[task_id]["status"] = "concluído"
                task_status[task_id]["resultado"] = result

        except Exception as e:
            with task_status_lock:
                task_status[task_id]["status"] = "erro"
                task_status[task_id]["erro"] = str(e)

    future = executor.submit(task)

    return task_id

def get_task_status(task_id: str) -> Dict:
    with task_status_lock:
        return task_status.get(task_id, {"status": "não encontrado"})
