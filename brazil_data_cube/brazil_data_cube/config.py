# brazil_data_cube/config.py

from pathlib import Path

# Diretório raiz do projeto
ROOT_DIR = Path(__file__).parent.parent.absolute()
DATA_DIR = ROOT_DIR / ""

# Caminhos úteis
IMAGES_DIR = DATA_DIR / "imagens"
SHAPEFILE_PATH = DATA_DIR / "shapefile_ids" / "grade_sentinel_brasil.shp"
SHAPEFILE_PATH_LANDSAT = DATA_DIR / "shapefile_ids" / "WRS2_descending.shp"
LOG_DIR = DATA_DIR / "log"
CSV_DIR = DATA_DIR / "temp"
LOG_CSV_PATH = DATA_DIR / "log/falhas_download.csv"
LOG_FILE = "log/brazil_data_cube_log.txt"

TILES_PATH_SENTINEL = DATA_DIR / "shapefile_ids" / "sentinel_UFids.json"
TILES_PATH_LANDSAT = DATA_DIR / "shapefile_ids" / "landsat_UFids.json"

# sqa_c8322b9bfa56cc3a7de40b2b59c55ba531b4e740 - sonarqube token

# Satélites suportados
SAT_SUPPORTED = ['S2_L2A-1', 'landsat-2']

# Configurações padrão
DEFAULT_RADIUS_KM = 10.0
MAX_CLOUD_COVER_DEFAULT = 20.0
REDUCTION_FACTOR = 0.2
COMMON_CRS = "EPSG:32721"
