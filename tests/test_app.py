import pytest
from app import app

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_receive_data_books(client):
    # Testar recebimento de dados de "books"
    response = client.post('/data', json={"endpoint": "books"})
    assert response.status_code == 200
    assert response.json == {"message": "Dados da API de Harry Potter processados e armazenados com sucesso"}

def test_receive_data_spells(client):
    # Testar recebimento de dados de "spells"
    response = client.post('/data', json={"endpoint": "spells"})
    assert response.status_code == 200
    assert response.json == {"message": "Dados da API de Harry Potter processados e armazenados com sucesso"}

def test_invalid_data(client):
    # Testar envio de dados inválidos
    response = client.post('/data', json={"invalid_field": "value"})
    assert response.status_code == 400
    assert response.json == {"error": "Formato de dados inválido"}
