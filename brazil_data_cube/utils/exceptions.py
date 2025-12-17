class OrphanFileError(Exception):
    """Erro lançado quando o banco falha APÓS o upload do arquivo."""
    def __init__(self, minio_path, original_error):
        self.minio_path = minio_path
        self.original_error = original_error
        super().__init__(f"Arquivo órfão gerado em: {minio_path}")
