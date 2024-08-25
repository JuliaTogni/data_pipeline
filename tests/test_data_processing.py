import pandas as pd
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert

def test_process_data():
    data = [{"title": "Harry Potter and the Sorcerer's Stone"}]
    filename = process_data(data)
    assert filename.endswith(".parquet")

def test_prepare_dataframe_for_insert():
    data = [{"title": "Harry Potter and the Sorcerer's Stone"}]
    df = pd.DataFrame(data)
    prepared_df = prepare_dataframe_for_insert(df)
    assert "data_ingestao" in prepared_df.columns
    assert "dado_linha" in prepared_df.columns
