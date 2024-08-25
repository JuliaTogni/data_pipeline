from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def create_bucket_if_not_exists(bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

def list_files(bucket_name):
    try:
        objects = minio_client.list_objects(bucket_name)
        return [obj.object_name for obj in objects]
    except Exception as e:
        print(f"Erro ao listar arquivos no bucket {bucket_name}: {e}")
        return []

def upload_file(bucket_name, file_path):
    file_name = os.path.basename(file_path)
    minio_client.fput_object(bucket_name, file_name, file_path)

def download_file(bucket_name, file_name, local_file_path):
    minio_client.fget_object(bucket_name, file_name, local_file_path)