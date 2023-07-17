import os
import re
import sqlite3
import requests
from lxml import etree
from datetime import date
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor
from yaml import safe_load
from tqdm import tqdm
from openpyxl import Workbook
import chardet
import threading


# 从配置文件读取设置
with open("config.yaml", "r") as config_file:
    config = safe_load(config_file.read())

THREADS = config['threads']
DB_LOCK = threading.Lock()
TARGET_FILE = config['target_file']
DB_FILE = config['db_file']
OUTPUT_DIR = config['output_dir']
TIMEOUT = config['timeout']
SAVE_INTERVAL = 200

ua = UserAgent()
error_domains = []


def create_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS keywords (
                    id INTEGER PRIMARY KEY,
                    keyword TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    method TEXT NOT NULL
                 )""")
    conn.commit()
    conn.close()


def save_to_database(keywords, domain, method):
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        for keyword in keywords:
            cursor.execute("INSERT INTO keywords (keyword, domain, method) VALUES (?, ?, ?)", (keyword, domain, method))
        conn.commit()
        conn.close()


def get_keywords(domain, method):
    headers = {'User-Agent': ua.random}
    url = f'https://{domain}'

    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        content = response.content
        detected_encoding = chardet.detect(content)['encoding'] or 'utf-8'
        content = content.decode(detected_encoding, errors='ignore')

        # 删除 XML 编码声明
        content = re.sub(r'^<\?xml[^>]*\?>', '', content).strip()

        # 转换为字节
        content = content.encode('utf-8')

        parser = etree.HTMLParser()
        tree = etree.fromstring(content, parser)

        if tree is None:
            return []

        if method == 'A':
            nav_links = tree.xpath("//a[contains(@class, 'nav')]")
        elif method == 'B':
            nav_links = tree.xpath("//nav//a")
        elif method == 'C':
            nav_links = tree.xpath("//div[contains(@class, 'nav')]//a")

        keywords = [nav_link.text for nav_link in nav_links if nav_link.text is not None]
        return keywords
    except requests.exceptions.RequestException:
        return []




def process_domain(domain):
    domain = domain.strip()
    methods = ['A', 'B', 'C']
    all_keywords = []

    for method in methods:
        keywords = get_keywords(domain, method)
        save_to_database(keywords, domain, method)
        all_keywords.extend([(keyword, method) for keyword in keywords])

    return all_keywords, domain


def main(output_file):
    create_database()

    with open(TARGET_FILE, 'r') as f:
        domains = [line.strip() for line in f.readlines()]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.append(["Keyword", "Domain", "Method"])

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        results = list(tqdm(executor.map(process_domain, domains), total=len(domains)))

    count = 0
    for keywords, domain in results:
        for keyword, method in keywords:
            ws.append([keyword, domain, method])
            count += 1
            if count >= SAVE_INTERVAL:
                wb.save(output_file)
                count = 0

    # 保存剩余的结果
    if count > 0:
        wb.save(output_file)

    # 打印已完成
    print("任务已完成，结果保存在：", output_file)

    # 保存错误的域名
    if error_domains:
        with open(config['error_log_file'], 'w') as error_log_file:
            for domain, error_msg in error_domains:
                error_log_file.write(f'{domain}: {error_msg}\n')

        print("错误的或无法抓取的域名已保存在：", config['error_log_file'])

if __name__ == "__main__":
    output_file = os.path.join(OUTPUT_DIR, f'{date.today().strftime("%Y%m%d")}_keywords.xlsx')
    main(output_file)
