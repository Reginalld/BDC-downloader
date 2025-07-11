import logging
from pydantic import ValidationError
from brazil_data_cube.downloader.image_downloader import ImagemDownloader
from brazil_data_cube.utils.logger import ResultManager
from brazil_data_cube.api.models import DownloadRequest
from brazil_data_cube.config import IMAGES_DIR
from brazil_data_cube.api.state import EstadoExecucao

logger = logging.getLogger(__name__)

estado_execucao = EstadoExecucao()

def iniciar_download(request: DownloadRequest):
    try:
        # Configura logger dinâmico por satélite/ano-mês
        estado_execucao.set_running()
        ResultManager.setup_logger(request.satelite, request.start_date)

        downloader = ImagemDownloader(output_dir=IMAGES_DIR)

        downloader.executar_download(
            satelite=request.satelite,
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
        estado_execucao.set_idle()
