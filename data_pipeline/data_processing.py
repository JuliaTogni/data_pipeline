import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime


def process_data(data):
    # Verifique se os dados recebidos são um dicionário de valores escalares
    if isinstance(data, dict):
        # Coloque os dados dentro de uma lista para criar o DataFrame corretamente
        df = pd.DataFrame([data])
    else:
        # Caso os dados já estejam em um formato adequado (como uma lista de dicionários)
        df = pd.DataFrame(data)
    
    filename = f"raw_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)
    return filename


def prepare_dataframe_for_insert(df):
    df['data_ingestao'] = datetime.now()
    df['dado_linha'] = df.apply(lambda row: row.to_json(), axis=1)
    df['tag'] = 'example_tag'
    return df[['data_ingestao', 'dado_linha', 'tag']]
