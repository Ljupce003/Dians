import json

import requests
from bs4 import BeautifulSoup
import re
import Filter_II


def fetch_issuer_names(url):
    """Fetch issuer names from the given URL and filter those without numbers in their codes."""
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    dropdown = soup.find('select', class_='form-control')
    if not dropdown:
        print("No dropdown found.")
        return []
    # Extract and filter issuer names
    return [
        option.text.strip()
        for option in dropdown.find_all('option')
        if option.get('value') and not re.search(r'\d', option.get('value'))
    ]


def Call_Filter_1():
    url = 'https://www.mse.mk/mk/stats/symbolhistory/KMB'
    # Претпоставуваме дека fetch_issuer_names(url) враќа листа на имиња
    issuer_names = fetch_issuer_names(url)

    # Додавање на ID за секое име во листата
    issuer_data = [{"id": idx + 1, "Name": name} for idx, name in enumerate(issuer_names)]

    # Запишување на листата со ID и имиња во JSON фајл
    with open("../Baza/issuer_names.json", "w") as file:
        json.dump(issuer_data, file, indent=4)
    print("issuer_names data appended")
    Filter_II.Call_Filter_II()
