# tests/test_bbox_handler.py
import geopandas as gpd
import pytest
from brazil_data_cube.utils.bounding_box_handler import BoundingBoxHandler
from shapely.geometry import box


def test_calcular_bbox_reduzido(mocker):
    mock_gdf = gpd.GeoDataFrame({
        "NAME": ["21JYM"],
        "geometry": [box(-55.0, -25.0, -53.0, -23.0)]
    })

    mocker.patch("geopandas.read_file", return_value=mock_gdf)

    bbox_handler = BoundingBoxHandler(reduction_factor=0.2)
    result = bbox_handler.calcular_bbox_reduzido(mock_gdf)
    
    assert len(result) == 4
    assert all(isinstance(coord, float) for coord in result)