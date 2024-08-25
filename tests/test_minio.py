from minio import Minio
from minio.error import S3Error

def test_minio():
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket_name = "ponderada-bucket"
    object_name = "test.txt"
    file_content = "Este é um arquivo teste"

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado com sucesso!")
        else:
            print(f"Bucket '{bucket_name}' já existe...")

        with open(object_name, "w") as file:
            file.write(file_content)

        # Upload
        client.fput_object(bucket_name, object_name, object_name)
        print(f"Arquivo '{object_name}' enviado com sucesso!")

        # Download
        client.fget_object(bucket_name, object_name, f"downloaded_{object_name}")
        print(f"Arquivo '{object_name}' baixado com sucesso do bucket '{bucket_name}'.")

    except S3Error as err:
        print(f"Erro ao enviar o arquivo: {err}")

if __name__ == "__main__":
    test_minio()

