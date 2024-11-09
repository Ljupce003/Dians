import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.by import By

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

browser = webdriver.Chrome(options=options)

browser.get("https://www.mse.mk/mk/stats/symbolhistory/ALK")

codes_df = pd.read_csv("codes.csv")

codes = []
for i in range(0, len(codes_df["code"])):
    code = codes_df.values.__getitem__(i)[1]

