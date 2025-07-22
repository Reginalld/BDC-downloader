# brazil_data_cube/logger.py

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd


YEAR_MONTH_VARIABLE = "%Y-%m"
DATE_VARIABLE = "%Y-%m-%d"


class ResultManager:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def log_error_csv(
        self,
        tile: str,
        satellite: str,
        error_msg: str,
        start_date: str,
        base_log_dir: str = "log"
    ) -> None:
        """
        Registra erros no CSV de falhas, organizados por satélite e ano/mês.
        """
        year_month = datetime.strptime(start_date, DATE_VARIABLE).strftime(
            YEAR_MONTH_VARIABLE
        )
        log_dir = Path(base_log_dir) / satellite / year_month
        log_dir.mkdir(parents=True, exist_ok=True)

        csv_path = log_dir / "erros.csv"
        file_exists = csv_path.is_file()

        try:
            with open(
                    csv_path, mode="a", newline="", encoding="utf-8"
                    ) as csvfile:
                fieldnames = ["Data", "Tile_id", "Satelite", "Erro"]
                writer = csv.DictWriter(
                    csvfile,
                    fieldnames=fieldnames,
                    dialect="excel"
                )

                if not file_exists:
                    writer.writeheader()

                writer.writerow({
                    "Data": datetime.now().isoformat(timespec="seconds"),
                    "Tile_id": tile,
                    "Satelite": satellite,
                    "Erro": error_msg
                })
            self.logger.info(f"Erro registrado no CSV: {tile} - {satellite}")
        except Exception as e:
            self.logger.critical(f"Falha ao gravar no CSV de erros: {e}")

    def manage_results(
        self,
        tile_mosaic_files: List[str],
        results_time_estimated: List[Dict[str, float]],
        satellite: str,
        start_date: str
    ) -> None:
        """
        Gera relatórios de tempo de download e estimativas.
        """
        if not tile_mosaic_files:
            self.logger.error("Nenhum tile foi baixado.")
            return

        executed_at = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
        time_stamp_str = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")

        df = self.create_dataframe(results_time_estimated, executed_at)
        avarage = df["Duracao_minutos"].mean().round(2)
        total_estimate = round(avarage * len(tile_mosaic_files), 2)

        df = self.add_sumarry(df, executed_at, avarage, total_estimate)

        year_month = datetime.strptime(start_date, DATE_VARIABLE).strftime(
            YEAR_MONTH_VARIABLE
        )
        base_log_dir = "log"
        log_dir = Path(base_log_dir) / satellite / year_month
        log_dir.mkdir(parents=True, exist_ok=True)

        csv_path = log_dir / f"tempo_downloads_{time_stamp_str}.csv"
        df.to_csv(csv_path, index=False)

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
            {
                "Tile_id": "MÉDIA",
                "Duracao_minutos": avarage,
                "executed_at": executed_at
            },
            {
                "Tile_id": "total_estimate",
                "Duracao_minutos": total_estimate,
                "executed_at": executed_at
            }
        ])
        return pd.concat([df, summary_df], ignore_index=True)

    @staticmethod
    def setup_logger(
        satellite: str,
        data: str,
        exec_id: str,
        base_log_dir: str = "log"
    ) -> logging.Logger:
        year_month = datetime.strptime(data, DATE_VARIABLE).strftime(
            YEAR_MONTH_VARIABLE
        )
        log_dir = Path(base_log_dir) / satellite / year_month
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / f"{exec_id}.log"
        logger_name = f"{satellite}_{exec_id}"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
            )
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

            logger.propagate = False

        logger.info(f"Logger iniciado para execução {exec_id}")
        return logger
