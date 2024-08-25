from flask import Flask, request, jsonify
from data_pipeline.minio_client import upload_file, download_file
from data_pipeline.clickhouse_client import get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
from data_pipeline.potter_api_client import fetch_harry_potter_data
import pandas as pd

app = Flask(__name__)

@app.route('/data', methods=['POST'])
def receive_data():
    data = request.get_json()
    if not data or 'endpoint' not in data:
        return jsonify({"error": "Formato de dados inválido"}), 400

    try:
        endpoint = data['endpoint']  
        response_data = fetch_harry_potter_data(endpoint)
        
        if response_data is None:
            return jsonify({"error": "Erro ao buscar dados da API"}), 500
    
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Tipo de dados inválido: {str(e)}"}), 400

    # Processar e salvar dados
    filename = process_data(response_data) 
    upload_file("raw-data", filename)

    # Ler arquivo Parquet de volta do Minio
    download_file("raw-data", filename, f"downloaded_{filename}")
    df_parquet = pd.read_parquet(f"downloaded_{filename}")

    # Preparar e inserir dados no ClickHouse
    df_prepared = prepare_dataframe_for_insert(df_parquet)
    client = get_client()
    insert_dataframe(client, "working_data", df_prepared)

    return jsonify({"message": "Dados da API de Harry Potter processados e armazenados com sucesso"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
