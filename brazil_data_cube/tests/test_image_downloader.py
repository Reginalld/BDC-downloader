# brazil_data_cube/tests/test_image_downloader.py

import os
from unittest.mock import MagicMock, patch

import pytest
import requests

from brazil_data_cube.downloader.image_downloader import ImageDownloader


@pytest.fixture
def mock_logger():
    """Cria um logger fake para testes."""
    class DummyLogger:
        def info(self, *args, **kwargs): pass
        def warning(self, *args, **kwargs): pass
        def error(self, *args, **kwargs): pass
    return DummyLogger()


@pytest.fixture
def downloader(tmp_path, mock_logger):
    """Instancia ImageDownloader com diretório temporário."""
    return ImageDownloader(logger=mock_logger, output_dir=str(tmp_path))


def test_create_output(downloader, tmp_path):
    """Verifica se o diretório de saída é criado."""
    path = tmp_path / "nested"
    ImageDownloader(downloader.logger, str(path))
    assert path.exists()
    assert path.is_dir()


@patch("brazil_data_cube.downloader.image_downloader.requests.get")
def test_download_success(mock_get, downloader, tmp_path):
    """Simula download bem-sucedido."""
    fake_resp = MagicMock()
    fake_resp.iter_content = lambda chunk_size: [b"123", b"456"]
    fake_resp.headers = {"content-length": "6"}
    fake_resp.raise_for_status = lambda: None
    mock_get.return_value = fake_resp

    filepath = downloader.download(
        asset=MagicMock(href="http://fake.com/file.tif"),
        filename="file.tif"
    )

    assert filepath is not None
    assert os.path.exists(filepath)
    with open(filepath, "rb") as f:
        assert f.read() == b"123456"


@patch("brazil_data_cube.downloader.image_downloader.requests.get")
def test_download_failure(mock_get, downloader):
    """Simula erro no download."""
    mock_get.side_effect = requests.RequestException("Falhou")

    filepath = downloader.download(
        asset=MagicMock(href="http://fake.com/file.tif"),
        filename="file.tif"
    )

    assert filepath is None


def test_prepare_output_dir(downloader):
    """Verifica se o diretório é atualizado corretamente."""
    downloader.prepare_output_dir("LANDSAT", "2025-09-01")
    assert "LANDSAT" in downloader.output_dir
    assert "2025-09" in downloader.output_dir
    assert os.path.exists(downloader.output_dir)


def test_upload_and_cleanup_removes_file(downloader, tmp_path):
    """Testa upload e remoção do arquivo local."""
    fake_file = tmp_path / "test.tif"
    fake_file.write_text("dummy")

    uploader = MagicMock()
    uploader.object_exists.return_value = True

    downloader.upload_and_cleanup(
        uploader=uploader,
        filepath=str(fake_file),
        bucket_prefix="prefix"
    )

    assert not fake_file.exists()


def test_upload_and_cleanup_file_not_exists(downloader, tmp_path):
    """Não falha se o arquivo já tiver sido removido."""
    fake_file = tmp_path / "test.tif"

    uploader = MagicMock()
    uploader.object_exists.return_value = True

    # captura logs de remover_log
    with patch.object(downloader.remover_log, "info") as mock_info, \
         patch.object(downloader.remover_log, "warning") as mock_warning:

        downloader.upload_and_cleanup(
            uploader=uploader,
            filepath=str(fake_file),
            bucket_prefix="prefix"
        )

    # verifica se chamou object_exists
    uploader.object_exists.assert_called_once()
    # verifica se registrou warning que arquivo não existe
    mock_warning.assert_called_with(f"Arquivo {fake_file} não encontrado.")

    mock_info
