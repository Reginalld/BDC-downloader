# brazil_data_cube/config.py
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # Carrega as variáveis do .env

# Diretório raiz do projeto
ROOT_DIR = Path(__file__).parent.parent.absolute()
DATA_DIR = ROOT_DIR / ""

# Caminhos úteis
IMAGES_DIR = DATA_DIR / "imagens"
SHAPEFILE_PATH_SENTINEL = DATA_DIR / "shapefile_ids" / "grade_sentinel_brasil.shp"  # noqa: E501
SHAPEFILE_PATH_LANDSAT = DATA_DIR / "shapefile_ids" / "WRS2_descending.shp"
SHAPEFILE_PATH_BDC_MD = DATA_DIR / "shapefile_ids" / "BDC_MD_V2.shp"
LOG_DIR = DATA_DIR / "log"
CSV_DIR = DATA_DIR / "temp"
LOG_CSV_PATH = DATA_DIR / "log/falhas_download.csv"
LOG_FILE = "log/brazil_data_cube_log.txt"

TILES_PATH_SENTINEL = DATA_DIR / "shapefile_ids" / "sentinel_UFids.json"
TILES_PATH_LANDSAT = DATA_DIR / "shapefile_ids" / "landsat_UFids.json"
TILES_PATH_BDC_MD_V2 = DATA_DIR / "shapefile_ids" / "BDC_MD_V2.json"

# Variáveis sensíveis
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
DATABASE_URL = os.getenv("DATABASE_URL")

# Satélites suportados
SAT_SUPPORTED = ['S2', 'L8', 'CBERS4-MUX-2M-1', 'S1']

# Configurações padrão
DEFAULT_RADIUS_KM = 10.0
MAX_CLOUD_COVER_DEFAULT = 20.0
REDUCTION_FACTOR = 0.2
COMMON_CRS = "EPSG:32721"
MIN_GEOMETRY_COVER_DEFAULT = 82.0
