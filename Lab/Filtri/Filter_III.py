import threading

import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www.mse.mk/mk/stats/symbolhistory/MPT"


# def fetch_data_for_period(firm_code, start_date, end_date):
#     session = requests.Session()
#     payload = {
#         "FromDate": start_date,
#         "ToDate": end_date,
#         "Code": firm_code
#     }
#     response = session.post(url, data=payload)
#     if response.status_code == 200:
#         soup = BeautifulSoup(response.text, 'html.parser')
#         table = soup.find('table')
#         if table:
#             rows = []
#             headers = [th.text.strip() for th in table.find_all('th')]
#             for tr in table.find_all('tr')[1:]:
#                 cells = [td.text.strip() for td in tr.find_all('td')]
#                 if cells:
#                     rows.append(cells)
#             data = pd.DataFrame(rows, columns=headers)
#             data.insert(0, "Issuer", firm_code)  # Add issuer name as a new first column
#             return data
#     return None

def fetch_data_for_period(firm_code, start_date, end_date):
    session = requests.Session()
    payload = {
        "FromDate": start_date,
        "ToDate": end_date,
        "Code": firm_code
    }
    response = session.post(url, data=payload)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
        if table:
            rows = []
            headers = ["Date", "Price", "Max", "Min", "Volume", "BEST"]
            for tr in table.find_all('tr')[1:]:
                # cells = [td.text.strip() for td in tr.find_all('td')]
                i = 0
                cells = []
                dozvoleni = [0, 1, 2, 3, 6, 7]
                # cells.append(firm_code)
                for td in tr.find_all('td'):

                    if i == 0 or i == 1 or i == 2 or i == 3 or i == 6 or i == 7:
                        cells.append(td.text.strip())
                    i += 1
                # print(cells)
                # print(cells.__len__())
                # date - 0
                # price - 1
                # max - 2
                # min - 3
                # volume - 6
                # BEST - 7

                BEST_val = float(str(cells.__getitem__(5)).replace(".", "".replace(",", "")))
                if cells and not BEST_val == 0:
                    rows.append(cells)
                    # print(cells)
                    # print(len(cells))
            try:
                data = pd.DataFrame(rows, columns=headers)
                data.insert(0, "Code", firm_code)  # Add issuer name as a new first column
            except Exception as e:
                print(e)
                print(headers)
                print(rows.__getitem__(0))

            return data
    return None


from datetime import datetime, timedelta


# Главната функција за поделба на интервали и повикување на getDataitem
def fetch_data_for_large_date_range(sif, start_date, end_date):
    all_data = []

    max_days = 365
    current_start = datetime.strptime(start_date, "%d.%m.%Y")

    while current_start <= datetime.strptime(end_date, "%d.%m.%Y"):  # Променето: <= за да го вклучиме и последниот ден
        # Одредување на крајната дата за тековниот интервал (максимум 365 дена)
        next_end = current_start + timedelta(days=max_days - 1)

        # Ако next_end надмине end_date, го поставуваме на end_date
        if next_end > datetime.strptime(end_date, "%d.%m.%Y"):
            next_end = datetime.strptime(end_date, "%d.%m.%Y")

        # Повик на getDataitem за тековниот под-интервал
        data = fetch_data_for_period(sif, current_start.strftime("%d.%m.%Y"), next_end.strftime("%d.%m.%Y"))
        if data is not None:
            all_data.append(data)
        #getDataitem(current_start, next_end)

        # Поместување на стартната дата за следниот интервал
        current_start = next_end + timedelta(days=1)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return None


lock = threading.Lock()


def Call_save_data_from_to(firm_code, start_date, end_date):
    all_issuers_data = []

    # Fetch data for the specified large date range
    data = fetch_data_for_large_date_range(firm_code, start_date, end_date)
    if data is not None:
        all_issuers_data.append(data)
        print(f"Data fetched for issuer: {firm_code}")
    else:
        print(f"No data for issuer: {firm_code}")

    # Combine all issuer data into a single DataFrame and save as CSV
    if all_issuers_data:
        # Комбинирај ги сите нови податоци
        combined_data = pd.concat(all_issuers_data, ignore_index=True)

        combined_data.drop_duplicates(subset=["Code", "Date"], inplace=True)
        # Додај ги новите податоци на постоечкиот CSV
        with lock:
            combined_data.to_csv('../Baza/mega-data.csv', mode='a', header=False, index=False)

        print("Data appended to 'mega-data.csv'")
    else:
        print("No data to append.")
#Call_save_data_from_to("KMB","08.11.2014","08.11.2024")
