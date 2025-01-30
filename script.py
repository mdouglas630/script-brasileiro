import requests
import pandas as pd

API_KEY = ""

def obter_jogos_brasileirao(ano):
    url = f"https://v3.football.api-sports.io/fixtures?league=71&season={ano}"
    headers = {"x-apisports-key": API_KEY}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Erro ao acessar API-Football! Código: {response.status_code}")
        return
    
    jogos = response.json().get("response", [])
    
    lista_jogos = []
    for jogo in jogos:
        data = jogo["fixture"]["date"][:10]
        time_casa = jogo["teams"]["home"]["name"]
        time_visitante = jogo["teams"]["away"]["name"]
        placar_casa = jogo["goals"]["home"]
        placar_visitante = jogo["goals"]["away"]
        estadio = jogo["fixture"]["venue"]["name"]

        lista_jogos.append({
            "Data": data,
            "Time Casa": time_casa,
            "Time Visitante": time_visitante,
            "Placar Casa": placar_casa,
            "Placar Visitante": placar_visitante,
            "Estádio": estadio
        })
    
    df = pd.DataFrame(lista_jogos)
    df.to_csv(f"jogos_brasileirao_{ano}.csv", index=False, encoding="utf-8-sig")
    print(f"✅ Arquivo 'jogos_brasileirao_{ano}.csv' criado com sucesso!")

# Teste com 2023
obter_jogos_brasileirao(2023)
