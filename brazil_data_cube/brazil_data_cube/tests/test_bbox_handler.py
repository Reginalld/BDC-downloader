import logging
import math
import pytest
import geopandas as gpd
from shapely.geometry import Polygon
from brazil_data_cube.utils.bounding_box_handler import BoundingBoxHandler


@pytest.fixture
def logger():
    return logging.getLogger("test_logger")


@pytest.fixture
def handler(logger):
    return BoundingBoxHandler(logger)


def test_calculate_reduced_bbox(handler):
    # cria um polígono simples
    poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[poly], crs="EPSG:4326")

    bbox = handler.calculate_reduced_bbox(gdf)
    assert len(bbox) == 4
    assert bbox[0] < bbox[2]
    assert bbox[1] < bbox[3]


def test_calculate_reduced_bbox_tile(handler):
    bbox = handler.calculate_reduced_bbox_tile(0, 0, 10, 10)
    assert len(bbox) == 4
    # Verifica se realmente foi reduzido
    assert bbox[0] > 0
    assert bbox[2] < 10


def test_to_2d_polygon_with_z(handler):
    # polígono 3D
    poly3d = Polygon([(0, 0, 5), (1, 0, 5), (1, 1, 5), (0, 1, 5)])
    poly2d = handler.to_2d(poly3d)
    assert isinstance(poly2d, Polygon)
    assert not poly2d.has_z


def test_calculate_tile_bbox(handler):
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame(
        {"NAME": ["test"]}, geometry=[poly], crs="EPSG:4326"
        )

    bbox, lat, lon, radius = handler.calculate_tile_bbox(gdf, "S2")
    assert isinstance(bbox, list)
    assert len(bbox) == 4
    assert math.isclose(lat, 0.5)
    assert math.isclose(lon, 0.5)
    assert radius > 0


def test_get_tile_data_sentinel2(handler):
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame(
        {"NAME": ["21JYM"]}, geometry=[poly], crs="EPSG:4326"
        )

    tile = handler.get_tile_data(gdf, "21JYM", "S2")
    assert not tile.empty


def test_get_tile_data_landsat(handler):
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame(
        {"PATH": [1], "ROW": [2]}, geometry=[poly], crs="EPSG:4326"
        )

    tile = handler.get_tile_data(gdf, "001002", "LANDSAT")
    assert not tile.empty


def test_get_tile_data_cbers(handler):
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame({"tile": ["X/Y"]}, geometry=[poly], crs="EPSG:4326")

    tile = handler.get_tile_data(gdf, "X_Y", "CBERS")
    assert not tile.empty


def test_calculate_bbox_from_coords(handler):
    bbox = handler.calculate_bbox_from_coords(-15.0, -47.0, 10.0)
    assert len(bbox) == 4
    assert bbox[0] < bbox[2]
    assert bbox[1] < bbox[3]


def test_obter_bounding_box_with_coords(handler):
    bbox, lat, lon, radius = handler.obter_bounding_box(
        tile_id=None,
        lat=-10.0,
        lon=-50.0,
        radius_km=5.0,
        tile_grid_path="dummy",
        satellite="S2",
    )
    assert isinstance(bbox, list)
    assert math.isclose(lat, -10.0)
    assert math.isclose(lon, -50.0)


def test_obter_bounding_box_without_inputs(handler):
    with pytest.raises(ValueError):
        handler.obter_bounding_box(
            tile_id=None,
            lat=None,
            lon=None,
            radius_km=5.0,
            tile_grid_path="dummy",
            satellite="S2",
        )


def test_load_tile_grid_file_not_found(handler):
    with pytest.raises(FileNotFoundError):
        handler.load_tile_grid("arquivo_inexistente.shp")
