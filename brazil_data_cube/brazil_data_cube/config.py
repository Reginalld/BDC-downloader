# brazil_data_cube/config.py

import os
from pathlib import Path
from datetime import datetime

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
LOG_FILE = f"log/brazil_data_cube_log.txt"

# Tiles do Paraná
TILES_PARANA = [
    "21JYM","21JYN",
    # "21JZM",
    # "21JZN",
    # "21KYP",
    # "21KYQ",
    # "21KZP",
    # "21KZQ",
    # "21KZR",
    # "22JBR",
    # "22JBS",
    # "22JBT",
    # "22JCR",
    # "22JCS",
    # "22JCT",
    # "22JDR",
    # "22JDS",
    # "22JDT",
    # "22JER",
    # "22JES",
    # "22JET",
    # "22JFR",
    # "22JFS",
    # "22JFT",
    # "22JGS",
    # "22JGT",
    # "22KBA",
    # "22KBU",
    # "22KBV",
    # "22KCA",
    # "22KCU",
    # "22KCV",
    # "22KDA",
    # "22KDU",
    # "22KDV",
    # "22KEU",
    # "22KEV",
    # "22KFU",
    # "22KFV"
]

LANDSAT_TILES_PARANA = [
    "220077",
    "220078",
    # "221076",
    # "221077",
    # "221078",
    # "221079",
    # "222076",
    # "222077",
    # "222078",
    # "223076",
    # "223077",
    # "223078",
    # "224076",
    # "224077",
    # "224078",
]

# Satélites suportados
SAT_SUPPORTED = ['S2_L2A-1','landsat-2']

# Configurações padrão
DEFAULT_RADIUS_KM = 10.0
MAX_CLOUD_COVER_DEFAULT = 20.0
REDUCTION_FACTOR = 0.2
COMMON_CRS = "EPSG:32721"