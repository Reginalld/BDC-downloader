# Brazil Data Cube - STAC Downloader

Este projeto realiza o download e gerenciamento de imagens de satélite via STAC (SpatioTemporal Asset Catalog), com suporte a Sentinel-2, Landsat, CBERS e Sentinel-1, utilizando FastAPI, MinIO e processamento geoespacial com Python.


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

MINIO_ACCESS_KEY=sua_acces_key

MINIO_SECRET_KEY=sua_secret_key

MINIO_BUCKET=seu_bucket

MINIO_SECURE=false

DATABASE_URL="postgresql+asyncpg://bdcuser:bdcpass@localhost:5431/bdcdb"


## Observações

- O projeto usa `python-dotenv` para carregar variáveis de ambiente automaticamente.


