import numpy as np
from time import sleep
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
import pandas as pd

# ========================
# 1️⃣ Cấu hình Chrome
# ========================
chrome_options = Options()
chrome_options.add_argument("--headless=new")      # chạy ẩn, bỏ dòng này nếu muốn thấy trình duyệt
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Tự động tải ChromeDriver phù hợp với bản Chrome hiện có
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# ========================
# 2️⃣ Mở trang web
# ========================
url = "https://www.flashscore.com/football/england/premier-league/standings/#/OEEq9Yvp/standings/overall/"
driver.get(url)
sleep(random.randint(5, 10))

# GET header 
headers = driver.find_elements(By.CSS_SELECTOR, "div.ui-table__headerCell") 
titles = [h.get_attribute("title").strip() for h in headers if h.get_attribute("title")] 
print(titles)

#GET DATA
data={}
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Chờ phần tử rank xuất hiện
WebDriverWait(driver, 15).until(
    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.table__cell--rank"))
)

# Lấy dữ liệu

ranks = driver.find_elements(By.CSS_SELECTOR, "div.table__cell--rank")
data['Rank'] = [rank.text.strip() for rank in ranks if rank.text.strip()]
print(data['Rank'])


teams = driver.find_elements(By.CSS_SELECTOR, "a.tableCellParticipant__name")
data['Team'] = [t.text.strip() for t in teams if t.text.strip()]
print(data["Team"])

# Matches Played (MP)
data['MP'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--value:nth-of-type(1)")]

# Wins
data['WINS'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--value:nth-of-type(2)")]

# Draws
data['DRAWS'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--value:nth-of-type(3)")]

# Losses
data['LOSSES'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--value:nth-of-type(4)")]

# Goals For : Goals Against (VD: "86:41")
data['GOALS'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--score")]

# Goal Difference
data['GD'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--goalsForAgainstDiff")]

# Points
data['POINTS'] = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "span.table__cell--points")]

data['Mùa giải'] = driver.find_element(By.CSS_SELECTOR, "div.heading__info").text.strip()

# Lấy tất cả thẻ <a> chứa link standings
categories = driver.find_elements(By.CSS_SELECTOR, 'a[href*="standings/"]')

# Trích xuất phần cuối của href (overall, home, away)
data['match_category'] = [cat.get_attribute('href').split('/')[-2] for cat in categories]

print(data['match_category'])