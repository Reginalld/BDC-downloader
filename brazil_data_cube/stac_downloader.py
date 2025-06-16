# Importa o Typer para criar uma interface de linha de comando
import typer

from brazil_data_cube.downloader.image_downloader import ImagemDownloader
from brazil_data_cube.utils.logger import ResultManager
from brazil_data_cube.config import IMAGES_DIR, SHAPEFILE_PATH, MAX_CLOUD_COVER_DEFAULT

import logging

# Configuração do logger 
ResultManager.setup_logger()
logger = logging.getLogger(__name__)

# Instância do Typer para CLI
app = typer.Typer()

# Define o comando principal da aplicação
@app.command()
def main(
    satelite: str = typer.Argument(..., help="Escolha um satélite (ex: S2_L2A-1)"),
    lat: float = typer.Option(None, help="Latitude da área de interesse"),
    lon: float = typer.Option(None, help="Longitude da área de interesse"),
    tile_id: str = typer.Option(None, help="ID do tile Sentinel-2 (ex: '21JYN')"),
    radius_km: float = typer.Option(10.0, help="Raio da área de interesse em km"),
    start_date: str = typer.Argument(..., help="Data de início (YYYY-MM-DD)"),
    end_date: str = typer.Argument(..., help="Data final (YYYY-MM-DD)"),
    output_dir: str = typer.Option(IMAGES_DIR, help="Diretório de saída para salvar as imagens"),
    tile_grid_path: str = typer.Option(SHAPEFILE_PATH),
    max_cloud_cover: float = typer.Option(MAX_CLOUD_COVER_DEFAULT, help="Máximo de nuvens")
):
    """
    Main reponsável pela orquestração inicial do código, apenas coletando informações do usuário.
    """
    imagem_downloader = ImagemDownloader(output_dir)
    imagem_downloader.executar_download(
        satelite, lat, lon, tile_id, radius_km,
        start_date, end_date, tile_grid_path, max_cloud_cover
    )

if __name__ == "__main__":
    app()