# brazil_data_cube/logger.py

import logging
import csv
from datetime import datetime
import os
from ..config import LOG_CSV_PATH, TILES_PARANA, LOG_FILE
import pandas as pd 
from pathlib import Path
from typing import List, Dict, Any


logger = logging.getLogger(__name__)

class ResultManager:
    def __init__(self):
        pass

    @staticmethod
    def log_error_csv(tile: str, satelite: str, erro_msg: str) -> None:
        """
        Registra erros no CSV de falhas.
        
        Args:
            tile (str): Tile ou região afetada
            satelite (str): Nome do satélite
            erro_msg (str): Mensagem de erro detalhada
        """
        os.makedirs(os.path.dirname(LOG_CSV_PATH), exist_ok=True)
        file_exists = os.path.isfile(LOG_CSV_PATH)

        try:
            with open(LOG_CSV_PATH, mode="a", newline="", encoding='utf-8') as csvfile:
                fieldnames = ["Data", "Tile_id", "Satelite", "Erro"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='excel')

                if not file_exists:
                    writer.writeheader()

                writer.writerow({
                    "Data": datetime.now().isoformat(timespec='seconds'),
                    "Tile_id": tile,
                    "Satelite": satelite,
                    "Erro": erro_msg
                })
            logger.info(f"Erro registrado no CSV: {tile} - {satelite}")
        except Exception as e:
            logger.critical(f"Falha ao gravar no CSV de erros: {e}")
            
    def gerenciar_resultados(self, tile_mosaic_files: List[str], results_time_estimated: List[Dict[str, float]]) -> None:
        """
        Gera relatórios de tempo 
        
        Args:
            tile_mosaic_files (List[str]): Lista de caminhos das imagens processadas
            results_time_estimated (List[Dict]): Lista com duração de downloads
            output_dir (str): Pasta onde salvar resultados
            satelite (str): Nome do satélite usado
            start_date (str): Data inicial do download
            end_date (str): Data final do download
        """
        if not tile_mosaic_files:
            logger.error("Nenhum tile foi baixado.")
            return

        executed_at = datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        time_stamp_str = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")

        df = self._criar_dataframe(results_time_estimated, executed_at)
        media = df["Duracao_minutos"].mean().round(2)
        estimativa_total = (media * len(tile_mosaic_files)).__round__(2)

        df = self._adicionar_resumo(df, executed_at, media, estimativa_total)

        csv_path = f"log/tempo_downloads_{time_stamp_str}.csv"
        df.to_csv(csv_path, index=False)

        self._imprimir_resumo(media, estimativa_total, csv_path)


    def _criar_dataframe(self, results_time_estimated, executed_at):
        return pd.DataFrame([
            {
                "Tile_id": entry["Tile_id"],
                "Duracao_minutos": round(entry["duration_sec"] / 60, 2),
                "executed_at": executed_at
            }
            for entry in results_time_estimated
        ])

    def _adicionar_resumo(self, df, executed_at, media, estimativa_total):
        summary_df = pd.DataFrame([
            {"Tile_id": "MÉDIA", "Duracao_minutos": media, "executed_at": executed_at},
            {"Tile_id": "ESTIMATIVA_TOTAL", "Duracao_minutos": estimativa_total, "executed_at": executed_at}
        ])
        return pd.concat([df, summary_df], ignore_index=True)

    def _imprimir_resumo(self, media, estimativa_total, csv_path):
        print(f"Média por quadrante: {media:.2f} minutos")
        print(f"Estimativa total ({len(TILES_PARANA)} quadrantes): {estimativa_total:.2f} minutos")
        print(f"CSV salvo em: {csv_path}")

    @staticmethod
    def setup_logger(log_file=LOG_FILE):
        """Configura o sistema de logging para console e arquivo."""
        log_path = Path(log_file)
        log_path.parent.mkdir(exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler()
            ]
        )