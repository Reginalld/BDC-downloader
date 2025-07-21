# tests/test_fetcher.py
from unittest.mock import MagicMock

import pytest
from brazil_data_cube.downloader.fetcher import SatelliteImageFetcher


def test_fetch_image_valid(mocker):
    # Mock do item retornado pelo STAC
    mock_item = MagicMock()
    mock_item.assets = {
        "B04": {"href": "https://example.com/B04.tif"},
        "B03": {"href": "https://example.com/B03.tif"},
        "B02": {"href": "https://example.com/B02.tif"}
    }
    mock_item.properties = {"eo:cloud_cover": 15, "tileId": "tile_fake"}

    # Mock da conexão STAC
    mock_connection = MagicMock()
    mock_connection.search.return_value.items.return_value = [mock_item]

    # Mock do GeometryUtils e seu método is_good_geometry
    mock_geometry_utils = mocker.patch("brazil_data_cube.utils.geometry_utils.GeometryUtils")

    instance = mock_geometry_utils.return_value
    instance.is_good_geometry.return_value = True  # Finge que passou o filtro de geometria

    fetcher = SatelliteImageFetcher(mock_connection)

    result = fetcher.fetch_image(
        satellite="S2_L2A-1",
        bounding_box=[-51.0, -25.0, -50.0, -24.0],
        start_date="2024-01-01",
        end_date="2024-02-01",
        max_cloud_cover=20.0,
        tile_grid_path="shapefile_ids/grade_sentinel_brasil.shp",
        tile="tile_fake"
    )

    assert result is not None
    assert "B04" in result
