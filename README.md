# Brazil Data Cube - STAC Downloader

Este projeto realiza o download e gerenciamento de imagens de satélite via STAC (SpatioTemporal Asset Catalog), com suporte a Sentinel-2 e Landsat, utilizando FastAPI, MinIO e processamento geoespacial com Python.


## Requisitos

- Python 3.10+


## Configuração do ambiente

### 1. Crie o ambiente virtual

```bash
python -m venv venv
source venv/bin/activate      # Linux/macOS
# ou
venv\Scripts\activate         # Windows
```

### 2. Instale as dependências

```bash
pip install -r requirements.txt
```

### 3. Configure as variáveis de ambiente

Copie o arquivo `.env.example` e renomeie para `.env`:

```bash
cp .env.example .env  
```

Edite o `.env` e preencha os valores com suas credenciais e configurações.


## Executando a API

Com o ambiente virtual ativado, variáveis configuradas e no diretório raiz do projeto:

```bash
uvicorn main_api:app
```

Ou usando gunicorn como servidor

```bash
gunicorn main_api:app -k uvicorn.workers.UvicornWorker
```

A API estará disponível em:

```
http://localhost:8000
```

Documentação Swagger (automática):

```
http://localhost:8000/docs
```

---

## Estrutura esperada de variáveis no `.env`

# MinIO
MINIO_ENDPOINT=localhost:9000

MINIO_ACCESS_KEY=seu_usuario

MINIO_SECRET_KEY=sua_senha

MINIO_BUCKET=imagens-brutas

MINIO_SECURE=false


## Observações

- O projeto usa `python-dotenv` para carregar variáveis de ambiente automaticamente.

# Quality Gate Status

[![Quality Gate Status](http://localhost:9000/api/project_badges/measure?project=brazil-data-cube&metric=alert_status&token=sqb_7691835362507c5f87225b315fd58e99690384ab)](http://localhost:9000/dashboard?id=brazil-data-cube)

