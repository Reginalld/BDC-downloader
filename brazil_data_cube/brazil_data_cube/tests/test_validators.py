import pytest
from pydantic import ValidationError
from brazil_data_cube.api.downloader import DownloadRequest

VALID_COMMON = {
    "satelite": "S2_L2A-1",
    "start_date": "2023-01-01",
    "end_date": "2023-01-10",
    "radius_km": 10.0
}

def test_valid_with_tile_id():
    req = DownloadRequest(**{
        **VALID_COMMON,
        "tile_id": "21JYN"
    })
    assert req.tile_id == "21JYN"

def test_valid_with_lat_lon():
    req = DownloadRequest(**{
        **VALID_COMMON,
        "lat": -25.0,
        "lon": -49.0
    })
    assert req.lat == -25.0 and req.lon == -49.0


def test_invalid_satellite():
    with pytest.raises(ValidationError) as exc:
        DownloadRequest(**{
            **VALID_COMMON,
            "satelite": "nope",
            "tile_id": "21JYN"
        })
    assert "Satélite 'nope' não suportado" in str(exc.value)


def test_invalid_date_format():
    with pytest.raises(ValidationError) as exc:
        DownloadRequest(**{
            **VALID_COMMON,
            "start_date": "01-01-2023",
            "tile_id": "21JYN"
        })
    assert "A data deve estar no formato YYYY-MM-DD" in str(exc.value)


def test_start_date_after_end_date():
    with pytest.raises(ValidationError) as exc:
        DownloadRequest(**{
            **VALID_COMMON,
            "start_date": "2023-02-01",
            "end_date": "2023-01-01",
            "tile_id": "21JYN"
        })
    assert "A data de início deve ser anterior ou igual à data de término" in str(exc.value)


def test_tile_id_and_lat_lon_together():
    with pytest.raises(ValidationError) as exc:
        DownloadRequest(**{
            **VALID_COMMON,
            "tile_id": "21JYN",
            "lat": -10.0,
            "lon": -55.0
        })
    assert "Informe apenas 'tile_id' ou par de coordenadas" in str(exc.value)


def test_only_lat():
    with pytest.raises(ValidationError) as exc:
        DownloadRequest(**{
            **VALID_COMMON,
            "lat": -10.0
        })
    assert "Latitude e longitude devem ser fornecidas juntas." in str(exc.value)
