import os
import pytest
import logging
from unittest.mock import MagicMock, patch
from datetime import datetime
import geopandas as gpd
from shapely.geometry import Polygon

from brazil_data_cube.processors.tile_processor import TileProcessor


@pytest.fixture
def fake_logger():
    return logging.getLogger("test")


@pytest.fixture
def fake_uploader():
    uploader = MagicMock()
    uploader.upload_file = MagicMock()
    uploader.object_exists = MagicMock(return_value=True)
    return uploader


@pytest.fixture
def processor(fake_logger, fake_uploader, tmp_path):
    fetcher = MagicMock()
    downloader = MagicMock()
    return TileProcessor(
        logger=fake_logger,
        remover_loger=fake_logger,
        fetcher=fetcher,
        downloader=downloader,
        output_dir=str(tmp_path),
        tile_grid_path=str(tmp_path / "grid.shp"),  # fake path
        max_cloud_cover=10.0,
        minio_uploader=fake_uploader,
        min_geometry_cover=0.5,
    )


def test_build_prefix_with_date(processor):
    prefix = processor.build_prefix(
        "S2", "SENTINEL", "21LVC", "2025-09-20T00:00:00Z", "L2A"
    )
    assert prefix == "S2_SENTINEL_21LVC_20250920_L2A"


def test_build_prefix_without_date(processor):
    prefix = processor.build_prefix("S2", "SENTINEL", "21LVC", None, "L2A")
    assert prefix.endswith("_00000000_L2A")


def test_upload_and_cleanup_removes_file(processor, tmp_path, fake_uploader):
    # cria arquivo falso
    fake_file = tmp_path / "file.tif"
    fake_file.write_text("dummy data")

    files = {"B4": str(fake_file)}

    processor.upload_and_cleanup(files, "prefix")

    fake_uploader.upload_file.assert_called_once()
    fake_uploader.object_exists.assert_called_once()
    assert not fake_file.exists()  # deve ter sido removido


def test_upload_and_cleanup_file_not_found(processor, fake_uploader):
    fake_file = "/tmp/file_that_does_not_exist.tif"
    files = {"B4": fake_file}

    # não deve lançar exceção
    processor.upload_and_cleanup(files, "prefix")

    fake_uploader.upload_file.assert_called_once()
    fake_uploader.object_exists.assert_called_once()


def test_select_tile_grid_sentinel(processor):
    df = gpd.GeoDataFrame({"NAME": ["21LVC"]}, geometry=[Polygon()])
    grid, minio_prefix, mission, sat, level = processor.select_tile_grid(df, "21LVC", "S2")
    assert not grid.empty
    assert mission == "SENTINEL2"
    assert sat == "S2A"


def test_select_tile_grid_cbers(processor):
    df = gpd.GeoDataFrame({"tile": ["001/002"]}, geometry=[Polygon()])
    grid, minio_prefix, mission, sat, level = processor.select_tile_grid(df, "001_002", "CBERS")
    assert not grid.empty
    assert mission == "CBERS"
    assert sat == "CB4"


def test_select_tile_grid_landsat(processor):
    df = gpd.GeoDataFrame({"PATH": [1], "ROW": [2]}, geometry=[Polygon()])
    grid, minio_prefix, mission, sat, level = processor.select_tile_grid(df, "001002", "L8")
    assert not grid.empty
    assert mission == "LANDSAT"
    assert sat == "L8"


def test_process_tile_list_no_grid(processor):
    with patch.object(processor, "load_grid_robustly", return_value=None):
        processor.process_tile_list(["21LVC"], "S2", "2025-09-01", "2025-09-10")
        # como não carregou, não deve chamar fetcher
        processor.fetcher.fetch_image.assert_not_called()


def test_process_tile_list_with_valid_tile(processor, tmp_path):
    # mock grid com tile válido
    df = gpd.GeoDataFrame({"NAME": ["21LVC"]}, geometry=[Polygon([(0,0),(1,0),(1,1),(0,1)])], crs="EPSG:4326")

    with patch.object(processor, "load_grid_robustly", return_value=df), \
         patch("brazil_data_cube.processors.tile_processor.DownloadBands") as mock_download:
        mock_download.return_value.download_bands.return_value = {"B4": str(tmp_path / "file.tif")}

        processor.fetcher.fetch_image.return_value = MagicMock(
            properties={"created": "2025-09-20T00:00:00Z"},
            assets={"B4": MagicMock()}
        )

        processor.process_tile_list(["21LVC"], "S2", "2025-09-01", "2025-09-10")

        processor.fetcher.fetch_image.assert_called_once()
        mock_download.return_value.download_bands.assert_called_once()
