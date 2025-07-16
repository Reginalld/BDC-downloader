import logging
from pydantic import ValidationError
from brazil_data_cube.downloader.image_downloader import ImageDownloader
from brazil_data_cube.utils.logger import ResultManager
from brazil_data_cube.api.models import DownloadRequest
from brazil_data_cube.config import IMAGES_DIR
from brazil_data_cube.api.state import ExecutionState
import uuid

execution_state = ExecutionState()

def start_download(request: DownloadRequest, exec_id: str):
    try:
        # Configura logger dinâmico por satélite/ano-mês
        execution_state.set_running()
        logger = ResultManager.setup_logger(request.satellite, request.start_date, exec_id)

        logger.info("Início da execução do download")

        downloader = ImageDownloader(logger,output_dir=IMAGES_DIR)

        downloader.execute_download(
            satellite=request.satellite,
            lat=request.lat,
            lon=request.lon,
            tile_id=request.tile_id,
            radius_km=request.radius_km,
            start_date=request.start_date,
            end_date=request.end_date,
            tile_grid_path=request.tile_grid_path,
            max_cloud_cover=request.max_cloud_cover
        )

        return {"status": "Download iniciado com sucesso"}
    except ValidationError:
        raise
    except Exception as e:
        logger.exception("Erro ao iniciar download")
        return {"status": "erro", "mensagem": str(e)}
    
    finally:
        execution_state.set_idle()
