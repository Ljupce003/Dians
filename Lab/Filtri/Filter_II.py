import os
import pandas as pd
import Filter_III
from datetime import datetime

import json
import threading
from functools import partial
from dateutil.relativedelta import relativedelta

csv_file_path = "../Baza/mega-data.csv"  # замени со вистинскиот пат до CSV документот
json_file_path = "../Baza/issuer_names.json"  # замени со вистинскиот пат до JSON документот
output_json = "../Baza/last_dates.json"
last_dates_json_path = "../Baza/last_dates.json"  # замени со вистинската патека до JSON документот

semaphore = threading.Semaphore(20)


def load_or_create_csv(csv_file):
    # Ако папката не постои, креирај ја
    folder = os.path.dirname(csv_file)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)
        print(f"Created folder: {folder}")

    # Проверка дали датотеката постои
    if not os.path.isfile(csv_file):
        # Ако не постои, создади ја со потребните наслови
        headers = ["Code", "Date", "Price", "Max", "Min", "Volume", "BEST"]
        pd.DataFrame(columns=headers).to_csv(csv_file, index=False)
        print(f"Created CSV file with headers: {csv_file}")

    # Учитај го CSV документот
    csv_data = pd.read_csv(csv_file, header=0, names=["Code", "Date", "Price", "Max", "Min", "Volume", "BEST"])
    return csv_data


def get_last_dates_for_firms(csv_file, json_file, output_json):
    csv_data = load_or_create_csv(csv_file)
    csv_data = csv_data[csv_data["Date"].str.match(r"\d{2}\.\d{2}\.\d{4}")]
    csv_data["Date"] = pd.to_datetime(csv_data["Date"], format="%d.%m.%Y")
    csv_data = csv_data.sort_values(by=["Code", "Date"])

    with open(json_file, "r", encoding="utf-8") as file:
        json_data = json.load(file)

    last_dates = []

    # За секоја шифра во JSON документот, најди ја последната дата од CSV документот
    for firm in json_data:
        code = firm["Name"]
        firm_data = csv_data[csv_data["Code"] == code]

        # Ако има податоци за таа шифра, земи ја последната дата
        if not firm_data.empty:
            last_date = firm_data["Date"].max().strftime("%d.%m.%Y")
            last_dates.append({"Code": code, "last_date": last_date})
        else:
            # Ако нема податоци, стави последната дата да е од пред 10 години
            today = datetime.today()
            date_10_years_ago = today - relativedelta(years=10)
            last_dates.append({"Code": code, "last_date": date_10_years_ago.strftime("%d.%m.%Y")})

    # Запиши ги резултатите во нов JSON документ
    with open(output_json, "w", encoding="utf-8") as file:
        json.dump(last_dates, file, ensure_ascii=False, indent=4)

    print(f"Резултатите се запишани во {output_json}")
    return last_dates


def outdated_firms(last_dates_json, t_list):
    # Учитај ги податоците од JSON документот
    with open(last_dates_json, "r", encoding="utf-8") as file:
        last_dates_data = json.load(file)

    # Дефинирај ја денешната дата во формат "дд.мм.гггг"
    today = datetime.today().strftime("%d.%m.%Y")

    def thread_task(code, last_date, today):
        with semaphore:
            Filter_III.Call_save_data_from_to(code, last_date, today)

    # Провери која шифра има последна дата различна од денешната и испечати ги тие информации
    for entry in last_dates_data:
        code = entry["Code"]
        last_date = entry["last_date"]

        if last_date and last_date != today:
            print(f"Шифра: {code}, Последна дата: {last_date}, Денешна дата: {today}")

            thread = threading.Thread(target=partial(thread_task, code, last_date, today))
            thread.start()
            t_list.append(thread)
            # Filter_III.Call_save_data_from_to(code, last_date, today)


def Call_Filter_II():
    thread_list: list[threading.Thread] = []

    get_last_dates_for_firms(csv_file_path, json_file_path, output_json)
    outdated_firms(last_dates_json_path, thread_list)

    for thread_ in thread_list:
        thread_.join()

    get_last_dates_for_firms(csv_file_path, json_file_path, output_json)

#get_last_dates_for_firms(csv_file_path, json_file_path,output_json)
#outdated_firms(last_dates_json_path)
#get_last_dates_for_firms(csv_file_path, json_file_path,output_json)
