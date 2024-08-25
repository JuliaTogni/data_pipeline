from data_pipeline.potter_api_client import fetch_harry_potter_data

def test_fetch_books():
    data = fetch_harry_potter_data("books")
    assert data is not None
    assert isinstance(data, list)
    assert "title" in data[0]

def test_fetch_spells():
    data = fetch_harry_potter_data("spells")
    assert data is not None
    assert isinstance(data, list)
    assert "spell" in data[0]  # Supondo que 'spell' seja uma chave no retorno
