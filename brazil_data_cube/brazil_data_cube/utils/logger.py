# brazil_data_cube/logger.py

import logging
import csv
from datetime import datetime
import os
from ..config import LOG_CSV_PATH, TILES_PARANA
import pandas as pd 
from pathlib import Path
from typing import List, Dict, Any



class ResultManager:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def log_error_csv(self,tile: str, satellite: str, error_msg: str, start_date: str, base_log_dir: str = "log") -> None:
        """
        Registra erros no CSV de falhas, organizados por satélite e ano/mês.

        Args:
            tile (str): Tile ou região afetada.
            satelite (str): Nome do satélite.
            erro_msg (str): Mensagem de erro detalhada.
            start_date (str): Data de início da execução (formato YYYY-MM-DD).
            base_log_dir (str): Caminho base onde salvar os logs.
        """
        year_month = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
        log_dir = Path(base_log_dir) / satellite / year_month
        log_dir.mkdir(parents=True, exist_ok=True)

        csv_path = log_dir / "erros.csv"
        file_exists = csv_path.is_file()

        try:
            with open(csv_path, mode="a", newline="", encoding='utf-8') as csvfile:
                fieldnames = ["Data", "Tile_id", "Satelite", "Erro"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='excel')

                if not file_exists:
                    writer.writeheader()

                writer.writerow({
                    "Data": datetime.now().isoformat(timespec='seconds'),
                    "Tile_id": tile,
                    "Satelite": satellite,
                    "Erro": error_msg
                })
            self.logger.info(f"Erro registrado no CSV: {tile} - {satellite}")
        except Exception as e:
            self.logger.critical(f"Falha ao gravar no CSV de erros: {e}")
            
    def manage_results(self, tile_mosaic_files: List[str], results_time_estimated: List[Dict[str, float]], satellite: str, start_date: str) -> None:
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
            self.logger.error("Nenhum tile foi baixado.")
            return

        executed_at = datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        time_stamp_str = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")

        df = self.create_dataframe(results_time_estimated, executed_at)
        avarage = df["Duracao_minutos"].mean().round(2)
        total_estimate = (avarage * len(tile_mosaic_files)).__round__(2)

        df = self.add_sumarry(df, executed_at, avarage, total_estimate)

        year_month = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
        base_log_dir= 'log'
        log_dir = Path(base_log_dir) / satellite / year_month
        log_dir.mkdir(parents=True, exist_ok=True)

        csv_path = log_dir / f"tempo_downloads_{time_stamp_str}.csv"
        df.to_csv(csv_path, index=False)

        self.print_sumary(avarage, total_estimate, csv_path)


    def create_dataframe(self, results_time_estimated, executed_at):
        return pd.DataFrame([
            {
                "Tile_id": entry["Tile_id"],
                "Duracao_minutos": round(entry["duration_sec"] / 60, 2),
                "executed_at": executed_at
            }
            for entry in results_time_estimated
        ])

    def add_sumarry(self, df, executed_at, avarage, total_estimate):
        summary_df = pd.DataFrame([
            {"Tile_id": "MÉDIA", "Duracao_minutos": avarage, "executed_at": executed_at},
            {"Tile_id": "total_estimate", "Duracao_minutos": total_estimate, "executed_at": executed_at}
        ])
        return pd.concat([df, summary_df], ignore_index=True)

    def print_sumary(self, avarage, total_estimate, csv_path):
        print(f"Média por quadrante: {avarage:.2f} minutos")
        print(f"Estimativa total ({len(TILES_PARANA)} quadrantes): {total_estimate:.2f} minutos")
        print(f"CSV salvo em: {csv_path}")


    @staticmethod
    def setup_logger(satellite: str, data: str, exec_id: str, base_log_dir: str = "log") -> logging.Logger:
        year_month = datetime.strptime(data, "%Y-%m-%d").strftime("%Y-%m")
        log_dir = Path(base_log_dir) / satellite / year_month
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / f"{exec_id}.log"
        logger_name = f"{satellite}_{exec_id}"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

            logger.propagate = False

        logger.info(f"Logger iniciado para execução {exec_id}")
        return logger