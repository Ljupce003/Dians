import time
from datetime import *

from selenium.common import TimeoutException
from selenium.webdriver.support.ui import Select
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import threading
from time import time
from dateutil.relativedelta import relativedelta

pd.options.mode.chained_assignment = None

max_threads = 10
semaphore = threading.Semaphore(max_threads)


class FilterThread(threading.Thread):

    def __init__(self, firm_code, local_db, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        self.local_database = local_db
        self.firm_code = firm_code
        self.th_local_df = None
        super().__init__(group, target, name, args, kwargs, daemon=daemon)

    def run(self):
        with semaphore:
            try:
                last_c = Second_Filter(self.firm_code, local_database)
                self.th_local_df = ThirdFilter(last_c, self.firm_code, local_database)
            except Exception as e:
                print("Thread problem for the code:", self.firm_code)
                print(e)
            finally:
                semaphore.release()


def safe_find_elements(driver, selector, multiple=False, max_retries=5):
    for attempt in range(max_retries):
        try:
            if multiple:
                return driver.find_elements(By.CSS_SELECTOR, selector)
            else:
                return driver.find_element(By.CSS_SELECTOR, selector)
        except TimeoutException:
            if attempt < max_retries - 1:
                print(f"Retry {attempt + 1}/{max_retries} on TimeoutException")
            else:
                raise


def CollectForDates(firm_code, e_date):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-images")

    browser = webdriver.Chrome(options=options)
    browser.get('https://www.mse.mk/mk/stats/symbolhistory/ALK')

    new_entries = []

    start_d = datetime.now()
    end_d = datetime.strptime(e_date, "%d.%m.%Y")
    unchanged_d = datetime.strptime(e_date, "%d.%m.%Y")

    blank_rows_c = 0
    years = start_d.year - end_d.year
    for i in range(0, years + 1):
        starting_date = start_d.strftime("%d.%m.%Y")
        rec_end_date = max(unchanged_d, end_d)
        ending_date = rec_end_date.strftime("%d.%m.%Y")

        # Ova gi ciste i gi vnesuva od(Datum) i do(Datum)
        WebDriverWait(browser, 3600).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".datepicker ")))
        # date_pickers = browser.find_elements(By.CSS_SELECTOR, ".datepicker ")
        date_pickers = safe_find_elements(browser, ".datepicker", True)
        date_pickers[1].clear()
        date_pickers[0].clear()
        date_pickers[1].send_keys(starting_date)
        date_pickers[0].send_keys(ending_date)
        WebDriverWait(browser, 3600).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#Code")))
        # code_select = Select(browser.find_element(By.CSS_SELECTOR, '#Code'))
        code_select = Select(safe_find_elements(browser, "#Code"))
        # Tuka se selektira sifrata
        code_select.select_by_visible_text(firm_code)

        WebDriverWait(browser, 3600).until(
            EC.invisibility_of_element_located((By.ID, 'myModal'))
        )
        # Ova e dugmeto za prikazi
        WebDriverWait(browser, 3600).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".container-end input["
                                                                                            "value='Прикажи']")))
        dugme = browser.find_element(By.CSS_SELECTOR, ".container-end input[value='Прикажи']")
        # dugme = safe_find_elements(browser, ".container-end input")
        dugme.click()

        # Ova ceka da se loadira cela tabela
        WebDriverWait(browser, 3600).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#resultsTable tbody tr")))

        rows = browser.find_elements(By.CSS_SELECTOR, "#resultsTable tbody tr")

        for row in rows:
            cl = row.find_elements(By.CSS_SELECTOR, "td")
            # date - 0
            # price - 1
            # max - 2
            # min - 3
            # volume - 6
            # BEST - 7
            data = cl[0].get_attribute("innerHTML")
            price = ReplaceDots(cl[1].get_attribute("innerHTML"))
            max_value = ReplaceDots(cl[2].get_attribute("innerHTML"))
            min_value = ReplaceDots(cl[3].get_attribute("innerHTML"))
            volume = cl[6].get_attribute("innerHTML")
            best = cl[7].get_attribute("innerHTML").strip()
            try:
                best_n = int(best)
                if best_n == 0:
                    continue
            except Exception:
                blank_rows_c += 1
                # print("Wrong number for BEST")

            best = ReplaceDots(cl[7].get_attribute("innerHTML").strip())

            parsed_row = {
                "Code": firm_code,
                "Date": data,
                "Price": price,
                "Max": max_value,
                "Min": min_value,
                "Volume": volume,
                "BEST": best}
            new_entries.append(parsed_row)

        start_d = end_d
        end_d = end_d - timedelta(days=365)

    print("Blank rows from collecting are:", blank_rows_c)
    return pd.DataFrame(new_entries)


url = "https://www.mse.mk/mk/stats/symbolhistory/ALK"


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
            headers = ["Code", "Date", "Price", "Max", "Min", "Volume", "BEST"]
            for tr in table.find_all('tr')[1:]:
                # cells = [td.text.strip() for td in tr.find_all('td')]
                i = 0
                cells = []
                dozvoleni = [0, 1, 2, 3, 6, 7]
                cells.append(firm_code)
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
                BEST_val = float(cells.__getitem__(5))
                if cells and not BEST_val == 0:
                    rows.append(cells)
                    # print(cells)
                    # print(len(cells))
            data = pd.DataFrame(rows, columns=headers)
            data.insert(0, "Issuer", firm_code)  # Add issuer name as a new first column
            return data
    return None


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


# def CollectWithSoup(start_date, end_date, firm_code):
#     url = "https://www.mse.mk/mk/stats/symbolhistory/ALK"
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
#             headers = ["Code", "Date", "Price", "Max", "Min", "Volume", "BEST"]
#             for tr in table.find_all('tr')[1:]:
#                 cells = [td.text.strip() for td in tr.find_all('td')]
#                 print(cells)
#                 if cells:
#                     rows.append(cells)
#             data = pd.DataFrame(rows, columns=headers)
#             print(data)
#             return None
#             data.insert(0, "Issuer", firm_code)  # Add issuer name as a new first column
#             return data
#     return None


def CollectorDecade(firm_code):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-images")

    browser = webdriver.Chrome(options=options)
    browser.get('https://www.mse.mk/mk/stats/symbolhistory/ALK')

    # voa e za begin/end date
    t_bound = datetime.now()
    l_bound = t_bound - timedelta(days=365)

    firm_table = []

    # Multithreaded za sekoja goidna na edna sifra
    # togas sledniot for neka bide poveke threads

    blank_rows_count = 0
    for i in range(0, 10):
        t_bound_str = t_bound.strftime("%d.%m.%Y")
        l_bound_str = l_bound.strftime("%d.%m.%Y")

        # Ova gi ciste i gi vnesuva od(Datum) i do(Datum)
        WebDriverWait(browser, 3600).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".datepicker")))
        date_pickers = browser.find_elements(By.CSS_SELECTOR, ".datepicker ")
        date_pickers[1].clear()
        date_pickers[0].clear()
        date_pickers[1].send_keys(t_bound_str)
        date_pickers[0].send_keys(l_bound_str)

        # Tuka se selektira sifrata
        WebDriverWait(browser, 3600).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#Code")))
        code_select = Select(browser.find_element(By.CSS_SELECTOR, '#Code'))
        code_select.select_by_visible_text(firm_code)

        WebDriverWait(browser, 3600).until(
            EC.invisibility_of_element_located((By.ID, 'myModal'))
        )

        WebDriverWait(browser, 3600).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".container-end input")))
        dugme = browser.find_element(By.CSS_SELECTOR, ".container-end input")
        dugme.click()

        # Ova ceka da se loadira cela tabela
        WebDriverWait(browser, 3600).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#resultsTable tbody tr")))
        rows = browser.find_elements(By.CSS_SELECTOR, "#resultsTable tbody tr")

        for row in rows:
            cl = row.find_elements(By.CSS_SELECTOR, "td")
            # date - 0
            # price - 1
            # max - 2
            # min - 3
            # volume - 6
            # BEST - 7
            data = cl[0].get_attribute("innerHTML")
            price = ReplaceDots(cl[1].get_attribute("innerHTML"))
            max_value = ReplaceDots(cl[2].get_attribute("innerHTML"))
            min_value = ReplaceDots(cl[3].get_attribute("innerHTML"))
            volume = cl[6].get_attribute("innerHTML")
            best = cl[7].get_attribute("innerHTML").strip()
            try:
                best_n = int(best)
                if best_n == 0:
                    continue
            except Exception:
                blank_rows_count += 1
                # print("Wrong number for BEST")

            best = ReplaceDots(cl[7].get_attribute("innerHTML").strip())

            parsed_row = {
                "Code": firm_code,
                "Date": data,
                "Price": price,
                "Max": max_value,
                "Min": min_value,
                "Volume": volume,
                "BEST": best}

            firm_table.append(parsed_row)

        t_bound = l_bound
        l_bound = t_bound - timedelta(days=365)

    print("Blank rows from collecting are:", blank_rows_count)
    return pd.DataFrame(firm_table)


def ReplaceDots(price_string):
    price_string = price_string.replace(".", "'")
    price_string = price_string.replace(",", ".")
    price_string = price_string.replace("'", ",")
    return price_string


def contains_number(string):
    return any(char.isdigit() for char in string)


# TODO This collects,filters and returns all the codes/firms from the website
def First_Filter(url):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-images")

    browser = webdriver.Chrome(options=options)
    browser.get(url)

    firms = browser.find_elements(By.CSS_SELECTOR, ".form-control option")
    firm_codes = []

    for firm in firms:
        code = firm.text
        if not contains_number(code):
            firm_codes.append(firm.text)

    return firm_codes


# TODO This checks and returns the last date of the code,else if no date or code is found then it is returned None
def Second_Filter(s_code, local_dataframe):
    try:
        code_df = local_dataframe[local_dataframe.Code == s_code]
        code_df["Date"] = code_df["Date"].astype(str)
        code_df.sort_values("Date", axis=0, ascending=False)
    except Exception:
        return None
    try:
        data = str(code_df.Date[0])
        if data.__eq__(""):
            return None
        else:
            return data
    except Exception:
        return None


# TODO This gets all entries for a code between today and last date available,and if last date is none then all data
#  from past 10 years
def ThirdFilter(last_date, t_code, local_dataframe):
    if last_date is not None:
        new_entries = CollectForDates(t_code, last_date)
    else:
        # new_entries = CollectorDecade(t_code)
        date_10y = datetime.now() - relativedelta(years=10)
        bsdf = fetch_data_for_large_date_range(t_code, date_10y.strftime("%d.%m.%Y"), datetime.now().strftime("%d.%m.%Y"
                                                                                                              ))
        # print(bsdf)
        return

    # print(local_dataframe.values.__len__(), "local database")
    # print(new_entries.values.__len__(), "new entries")

    with lock:
        local_dataframe = pd.concat([local_dataframe, new_entries], ignore_index=False)
        local_dataframe = local_dataframe.drop_duplicates(subset=["Code", "Date"])

    return local_dataframe


lock = threading.Lock()

t_start = time()

URL = "https://www.mse.mk/mk/stats/symbolhistory/ALK"

# local_database = None
try:
    local_database = pd.read_csv("local_database.csv", index_col=0)
except FileNotFoundError:
    local_database = pd.DataFrame(columns=["Code", "Date", "Price", "Max", "Min", "Volume", "BEST"])
    local_database.to_csv("local_database.csv")

codes = First_Filter(URL)

threads = []
print("First Finished in:", time() - t_start)
print("Codes size is ", len(codes))

# t_count = 0
for kod in codes:
    print(kod)
    thread = FilterThread(kod, local_database)
    thread.start()
    threads.append(thread)
    # t_count += 1
    # if t_count == 20:
    #     break

for t in threads:
    t.join()

for t in threads:
    local_database = pd.concat([local_database, t.th_local_df], ignore_index=False)

local_database.drop_duplicates(subset=["Code", "Date"], inplace=True)

e_time = time()

print(e_time - t_start)
print(local_database)
local_database.to_csv("local_database.csv")
