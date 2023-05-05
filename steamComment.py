import re

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
from bs4 import BeautifulSoup
import pymysql
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}], timeout=60, max_retries=3, retry_on_timeout=True)


path = Service('chromedriver.exe')

browser = webdriver.Chrome(service=path)

browser.get('https://steamcommunity.com/app/1174180/reviews/?p=1&browsefilter=toprated&filterLanguage=all')

t1 = time.time()
while time.time() - t1 < 700:
    browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    try:
        wait = WebDriverWait(browser, 10)  # 等待时间最长为10秒钟
        wait.until(EC.element_to_be_clickable((By.ID, 'GetMoreContentBtn')))
        btn = browser.find_element_by_id('GetMoreContentBtn')
        btn.click()
    except:
        continue

html = browser.page_source
soup = BeautifulSoup(html, 'html.parser')
reviews = soup.find_all('div', {'class': 'apphub_Card'})

totalCount =0
for review in reviews:
    # 获取页面内容
    fellValue = review.find('div', {'class': 'found_helpful'})
    nick = review.find('div', {'class': 'apphub_CardContentAuthorName'})
    title = review.find('div', {'class': 'title'}).text
    hour = review.find('div', {'class': 'hours'})
    link = nick.find('a').attrs['href']
    comment = review.find('div', {'class': 'apphub_CardTextContent'}).text
    assetCount = review.find('div', {'class': 'apphub_CardContentMoreLink ellipsis'})

    # 字段截取
    assetNumbers = re.findall(r'\d+', assetCount.text)
    assetValue = int(assetNumbers[0])

    numbers = re.findall(r'\d{1,3}(?:,\d{3})*', fellValue.text)
    value = int(numbers[0].replace(',', ''))
    pattern = r"\d+\.?\d*"  # 匹配整数或浮点数
    result = re.findall(pattern, hour.text)
    index = comment.find("日")
    commentContent = comment[index+1:].strip()


    doc = {
        'userName': nick.text,
        'feelValue': value,
        'assetCount': assetValue,
        'likeType': title,
        'totalTime': float(result[0]),
        'comment': commentContent,
        'point': value+assetValue*0.5+float(result[0])*2
    }

    es.create(index='rdr',id=totalCount,body=doc)
    totalCount=totalCount+1


browser.close()