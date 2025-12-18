import os
import json
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pandas as pd

# Crée le dossier s'il n'existe pas
os.makedirs("data/processed", exist_ok=True)

# Chargement des URLs depuis .env
load_dotenv()
BASE_URL = os.getenv("BASE_URL")
BASE_URL_AGENDA = os.getenv("BASE_URL_AGENDA")

headers = {
    "User-Agent": "Mozilla/5.0"
}

# ============================
# Scraping de la page agenda
# ============================
agenda_url = BASE_URL + BASE_URL_AGENDA
res = requests.get(agenda_url, headers=headers)
soup = BeautifulSoup(res.text, 'html.parser')
items = soup.select('div.ListSit-item > a.Card')

resultats = []

for item in items:
    titre = item.select_one('.Card-title').get_text(strip=True)
    date_text = item.select_one('.Card-label').get_text(strip=True)
    lien = item.get('href')
    if lien.startswith("/"):
        lien = BASE_URL + lien

    # ---------------------------
    # Scraping de la page événement pour SitIntro-container
    # ---------------------------
    res_link = requests.get(lien, headers=headers)
    soup_link = BeautifulSoup(res_link.text, 'html.parser')
    show_event = soup_link.select_one('div.SitIntro-container')


    infos_list = []

    # Récupère les informations supplémentaires sous forme de liste
    if show_event is not None:
        list_info = show_event.select('ul > li')
        infos_list = [li.get_text(strip=True) for li in list_info]
      
        
    calendar = {
        "month": None, 
        "days": []
        }

    # ---------------------------
    # Scraping de la page calendrier
    # ---------------------------
    calendar_container = soup_link.select_one('.SitCalendar-container')
    calendar_item = calendar_container.select_one('.SitCalendar-item')

    # Récupération du mois
    calendar["month"] = calendar_item.select_one('.SitCalendar-month').text.strip()

    # Récupération de tous les jours
    for day_el in calendar_item.select('.SitCalendar-day'):
        spans = day_el.find_all('span')
        calendar["days"].append({
            "day": spans[0].text.strip(),
            "time": spans[1].text.strip()
        })


    resultats.append({
        "titre": titre,
        "date_text": date_text,
        "lien": lien,
        "infos": infos_list,
        "calendar": calendar
    })
    
    
# ---------------------------
# Sauvegarde
# ---------------------------

df_resultats = pd.DataFrame(resultats)
df_resultats.to_parquet("data/processed/agenda_enriched.parquet", index=False)

# Affichage rapide au format JSON
for r in resultats:
    print(json.dumps(r, ensure_ascii=False, indent=2))
