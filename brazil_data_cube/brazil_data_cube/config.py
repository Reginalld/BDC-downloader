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
LOG_DIR = DATA_DIR / "log"
CSV_DIR = DATA_DIR / "temp"
LOG_CSV_PATH = DATA_DIR / "log/falhas_download.csv"
LOG_FILE = f"log/brazil_data_cube_log.txt"

# Tiles do Paraná
TILES_PARANA = [
    "21JYM", "21JYN", "21KYP", "22JBS", "22JBT","22KBU", "22KBV", "22JCS",
    "22JCT", "22KCU", "22KCV", "22JDS", "22JDT", "22KDU", "22KDV", "22JES",
    "22JET", "22KEU", "22KEV", "22JFS", "22JFT", "22KFU", "22JGS", "22JGT"
]

TILES_SANTACATARINA = [
    "22JBR","22JCQ","22JCR","22JDQ","22JDR","22JEN","22JEP","22JEQ","22JER",
    "22JES","22JFN","22JFP","22JFQ","22JFR","22JFS","22JGP","22JGQ","22JGR","22JGS"
]

TILES_RIOGRANDE_SUL = [
    "21JVG","21JVH","21JWF","21JWG","21JWH","21JWJ","21JXF","21JXG","21JXH","21JXJ","21JXK","21HYE","21JYF","21JYG","21JYH","21JYJ","21JYK",
    "22HBH","22HBJ","22HBK","22JBL","22JBM","22JBN","22JBP","22JBQ","22HCJ","22HCK","22JCL","22JCM","22JCN","22JCP","22JCQ","22HDK","22JDL",
    "22JDM","22JDN","22JDP","22JDQ","22JEL","22JEM","22JEN","22JEP","22JFN"
]

# Satélites suportados
SAT_SUPPORTED = ['S2_L2A-1', 'S2-16D-2','landsat-2']

# Configurações padrão
DEFAULT_RADIUS_KM = 10.0
MAX_CLOUD_COVER_DEFAULT = 20.0
REDUCTION_FACTOR = 0.2
COMMON_CRS = "EPSG:32721"