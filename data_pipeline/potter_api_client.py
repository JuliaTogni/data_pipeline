import requests

BASE_URL = "https://potterapi-fedeperin.vercel.app"
LANGUAGE = "pt"

def fetch_harry_potter_data(endpoint):
    url = f"{BASE_URL}/{LANGUAGE}/{endpoint}"
    print(f"Requesting URL: {url}")
    response = requests.get(url)
    
    if response.status_code == 200:
        print(f"Status Code: {response.status_code}")
        return response.json()
    else:
        print(f"Erro ao buscar dados da API: {response.status_code}")
        return None
