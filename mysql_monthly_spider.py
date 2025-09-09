import os
import platform
import re
import requests
from bs4 import BeautifulSoup
import pymysql
from datetime import date
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

# ----------------- 配置 -----------------
DB_HOST = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"
DB_PORT = 4000
DB_USER = "AYVUJoLj15SN2UA.root"
DB_PASS = "TtlRpMMyEf5Sl7rc"
DB_NAME = "dbmonthly"
BASE_URL = "http://mysql.taobao.org/monthly/"

date_re = re.compile(r"/monthly/(\d{4})/(\d{2})/(\d{2})/")

# 日志配置
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 线程锁
db_lock = Lock()

# ----------------- 系统证书 -----------------
def get_default_ca_path():
    system = platform.system()
    if system == "Darwin":
        return "/etc/ssl/cert.pem"
    elif system == "Linux":
        if os.path.exists("/etc/ssl/certs/ca-certificates.crt"):
            return "/etc/ssl/certs/ca-certificates.crt"
        elif os.path.exists("/etc/pki/tls/certs/ca-bundle.crt"):
            return "/etc/pki/tls/certs/ca-bundle.crt"
    elif system == "Windows":
        logging.warning("Windows 系统请手动下载 CA 证书")
        return None
    return None

# ----------------- 标签解析 -----------------
def parse_tag(title):
    lower_title = title.lower()
    if "polardb" in lower_title:
        return "PolarDB"
    elif any(k in lower_title for k in ["mysql", "maria", "innodb", "tokudb"]):
        return "MySQL"
    elif any(k in lower_title for k in ["postgres", "pgsql", "gpdb"]):
        return "PostgreSQL"
    elif "alisql" in lower_title:
        return "AliSQL"
    elif "mongodb" in lower_title:
        return "MongoDB"
    elif "redis" in lower_title:
        return "Redis"
    elif "mssql" in lower_title or "sql server" in lower_title:
        return "SQL Server"
    return "common"

# ----------------- 数据库操作 -----------------
def get_mysql_conn(ca_path):
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        ssl={"ca": ca_path} if ca_path else None
    )

def ensure_table_exists(ca_path):
    """启动时确保表存在"""
    conn = get_mysql_conn(ca_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS t_articals (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            title VARCHAR(500),
            url VARCHAR(500) UNIQUE,
            author VARCHAR(200),
            create_date DATE,
            tag VARCHAR(100)
        ) CHARACTER SET utf8mb4;
    """)
    conn.commit()
    cursor.close()
    conn.close()

def get_existing_months(conn):
    """返回已抓取的月份集合，格式 'YYYY/MM'"""
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM t_articals")
    rows = cursor.fetchall()
    cursor.close()

    months = set()
    for row in rows:
        url = row[0]
        m = re.search(r"/monthly/(\d{4}/\d{2})/", url)
        if m:
            months.add(m.group(1))
    return months

def get_existing_urls(conn):
    """返回已抓取文章 URL 集合"""
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM t_articals")
    rows = cursor.fetchall()
    cursor.close()
    return set(row[0] for row in rows)

def save_article_to_mysql(article, conn):
    with db_lock:
        cursor = conn.cursor()
        insert_sql = """
            INSERT IGNORE INTO t_articals (title, url, author, create_date, tag)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (
            article["title"], article["url"], article["author"], article["create_date"], article["tag"]
        ))
        conn.commit()
        cursor.close()

# ----------------- 抓文章详情（自动重试） -----------------
def get_article_info(article_url, ca_path, retries=3, delay=2):
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(article_url, timeout=10)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            block_div = soup.find("div", attrs={"class": "block"})
            if not block_div:
                raise ValueError("no block div found")

            h2 = block_div.find("h2")
            title = h2.get_text(strip=True) if h2 else "No Title"

            p = block_div.find("p")
            author = p.get_text(strip=True)[len("Author:"):].strip() if p else None

            date_segs = date_re.findall(article_url)
            create_date = None
            if date_segs:
                y, m, d = map(int, date_segs[0])
                create_date = date(y, m, d)

            tag = parse_tag(title)

            logging.info(f"抓取文章成功: {title}")
            return {
                "title": title,
                "url": article_url,
                "author": author,
                "create_date": create_date,
                "tag": tag
            }

        except Exception as e:
            logging.warning(f"抓取失败 {article_url}，尝试 {attempt}/{retries} 次: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.error(f"抓取失败 {article_url}，已达到最大重试次数")
                return None

# ----------------- 抓二级文章列表（多线程） -----------------
def get_articles_from_month(month_url, ca_path, max_workers=5):
    try:
        resp = requests.get(month_url)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        li_list = soup.select("ul.posts li")
        article_urls = []
        for li in li_list:
            a = li.find("a", class_="main")
            if not a:
                continue
            article_url = a["href"]
            if not article_url.startswith("http"):
                article_url = "http://mysql.taobao.org" + article_url
            article_urls.append(article_url)

        conn = get_mysql_conn(ca_path)
        existing_urls = get_existing_urls(conn)
        new_article_urls = [url for url in article_urls if url not in existing_urls]

        if not new_article_urls:
            logging.info(f"该月份全部文章已抓取: {month_url}")
            conn.close()
            return

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(get_article_info, url, ca_path): url for url in new_article_urls}
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"抓取 {month_url}"):
                info = future.result()
                if info:
                    save_article_to_mysql(info, conn)
                    logging.info(f"✅ 已写入数据库: {info['title']}")

        conn.close()
    except Exception as e:
        logging.error(f"抓取月份页面失败 {month_url}: {e}")

# ----------------- 抓一级目录 -----------------
def get_monthly_links(ca_path):
    resp = requests.get(BASE_URL)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    conn = get_mysql_conn(ca_path)
    existing_months = get_existing_months(conn)
    conn.close()

    links = []
    for a in soup.select("div.content.typo ul li a"):
        href = a.get("href")
        if href and href.startswith("/monthly/"):
            month_str = "/".join(href.strip("/").split("/")[-2:])
            full_url = "http://mysql.taobao.org" + href
            if month_str not in existing_months:
                links.append(full_url)

    logging.info(f"找到 {len(links)} 个未抓取的月份目录")
    return links

# ----------------- 主程序 -----------------
if __name__ == "__main__":
    ca_path = get_default_ca_path()
    if not ca_path:
        logging.error("未找到 CA 证书路径，请手动下载")
        exit(1)

    # ✅ 启动时确保表存在
    ensure_table_exists(ca_path)

    monthly_links = get_monthly_links(ca_path)
    for month_url in monthly_links:
        logging.info(f"开始抓取月份页面: {month_url}")
        get_articles_from_month(month_url, ca_path, max_workers=5)

    logging.info("✅ 全部文章抓取完成")
