from data_pipeline.minio_client import upload_file, download_file, list_files
from data_pipeline.clickhouse_client import get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import requests
import pandas as pd

def fetch_harry_potter_data(endpoint):
    # buscar dados da API de Harry Potter
    base_url = "https://potterapi-fedeperin.vercel.app"
    url = f"{base_url}/{endpoint}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar dados da API: {response.status_code}")
        return None

def start_verification_for_storage():
    expected_filename = "harry_potter_data.parquet"

    # Listar arquivos no bucket
    existing_files = list_files("raw-data")

    if expected_filename in existing_files:
        # Arquivo já existe, então baixar e usar o dado existente
        download_file("raw-data", expected_filename, f"downloaded_{expected_filename}")
        df_parquet = pd.read_parquet(f"downloaded_{expected_filename}")
        print("Arquivos já existentes")
    else:
        # Arquivo não existe, então faz a requisição à API, processa e salva o dado
        endpoint = "en/books"
        response_data = fetch_harry_potter_data(endpoint)

        if response_data is not None:
            # Processar e salvar dados
            datas_api = process_data(response_data)
            upload_file("raw-data", datas_api)

            # Baixar o arquivo Parquet do MinIO
            download_file("raw-data", datas_api, f"downloaded_{datas_api}")
            df_parquet = pd.read_parquet(f"downloaded_{datas_api}")

            # Preparar e inserir dados no ClickHouse
            df_prepared = prepare_dataframe_for_insert(df_parquet)
            client = get_client()  # Obter o cliente ClickHouse
            insert_dataframe(client, 'working_data', df_prepared)
            print("Arquivo salvo no storage :)")
        else:
            print("Nenhum dado foi processado devido a um erro na API.")

# Para executar a verificação e armazenamento
if __name__ == "__main__":
    start_verification_for_storage()
