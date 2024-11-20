import os
import json
import sqlite3
import requests
import subprocess

from datetime import datetime
from bs4 import BeautifulSoup


# Define constants for file names
SCRAPER_FILE = "extract.json"
TRANSFORMER_INPUT_FILE = SCRAPER_FILE
TRANSFORMER_OUTPUT_FILE = "transform.json"
SQLITE_DB_FILE = "sqlite.db"
SQLITE_JSON_OUTPUT_FILE = "sqlite.json"
COMMIT_MESSAGE_TEMPLATE = "Update sqlite.json: Iteration {iteration} on {date}"


# Scraper Class
class Scraper:
    def __init__(self):
        self.url = "https://tablet-pc.arukereso.hu/samsung/galaxy-tab-a9-x210-128gb-p1025165686/#"
        self.headers = {"User-Agent": "Mozilla/5.0"}
        self.item_data = []

    def fetch_data(self):
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def parse_data(self, soup):
        items = soup.find_all("div", class_="optoffer device-desktop")
        for item in items:
            link = item.find("a", class_="jumplink-overlay initial")["href"] if item.find("a", class_="jumplink-overlay initial") else "N/A"
            data = {
                    "title": item.find("h4", {"data-akjl": "Product name||ProductName"}).get_text(strip=True) if item.find("h4", {"data-akjl": "Product name||ProductName"}) else "N/A",
                    "price": item.find("span", itemprop="price").get_text(strip=True) if item.find("span", itemprop="price") else "N/A",
                    "currency": item.find("meta", itemprop="priceCurrency")["content"] if item.find("meta", itemprop="priceCurrency") else "N/A",
                    "availability": item.find("link", itemprop="availability")["href"] if item.find("link", itemprop="availability") else "N/A",
                    "rating": len(item.find_all("span", class_="star icon-star")) + 0.5 * len(item.find_all("span", class_="star icon-star-half-alt")) if item.find_all("span", class_="star icon-star") else "N/A",
                    "reviews_count": item.find("span", class_="reviews-count").get_text(strip=True).strip("()") if item.find("span", class_="reviews-count") else "N/A",
                    "store_name": item.find("div", {"data-akjl": "Store name||StoreName"}).get_text(strip=True) if item.find("div", {"data-akjl": "Store name||StoreName"}) else "N/A",
                    "link": link,
                    "timestamp": datetime.now().isoformat(),
                    }
            self.item_data.append(data)

    def run(self):
        soup = self.fetch_data()
        if soup:
            self.parse_data(soup)


# DataWriter Class
class DataWriter:
    def __init__(self, data):
        self.data = data

    def to_json(self, filename):
        with open(filename, "w", encoding="utf-8") as json_file:
            json.dump(self.data, json_file, indent=4, ensure_ascii=False)


# Transformer Class
class Transformer:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.data = []

    def load_data(self):
        with open(self.input_file, "r", encoding="utf-8") as file:
            self.data = json.load(file)

    def transform_data(self):
        for record in self.data:
            if "price" in record:
                record["price"] = int(record["price"].replace("Ft", "").replace(" ", "").strip())
            if "reviews_count" in record:
                reviews_text = record["reviews_count"].replace("vélemény", "").replace(" ", "").strip()
                record["reviews_count"] = int(reviews_text) if reviews_text.isdigit() else 0
            if "store_logo" in record:
                del record["store_logo"]

    def save_data(self):
        with open(self.output_file, "w", encoding="utf-8") as file:
            json.dump(self.data, file, indent=4, ensure_ascii=False)

    def run(self):
        self.load_data()
        self.transform_data()
        self.save_data()


# SQLiteWriter Class
class SQLiteWriter:
    def __init__(self, input_file, db_file, output_file):
        self.input_file = input_file
        self.db_file = db_file
        self.output_file = output_file

    def run(self):
        with open(self.input_file, "r", encoding="utf-8") as file:
            data = json.load(file)

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            price INTEGER,
            currency TEXT,
            availability TEXT,
            rating REAL,
            reviews_count INTEGER,
            store_name TEXT,
            link TEXT,
            timestamp TEXT
        )
        """)

        for record in data:
            cursor.execute("""
            INSERT INTO records (title, price, currency, availability, rating, reviews_count, store_name, link, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record["title"], record["price"], record["currency"], record["availability"],
                record["rating"], record["reviews_count"], record["store_name"], record["link"], record["timestamp"]
            ))

        conn.commit()
        cursor.execute("SELECT * FROM records")
        all_data = cursor.fetchall()

        with open(self.output_file, "w", encoding="utf-8") as file:
            json.dump([dict(zip([column[0] for column in cursor.description], row)) for row in all_data], file, indent=4)

        conn.close()


# Git Automation Functions
def get_iteration_count(file_path):
    try:
        result = subprocess.run(["git", "log", "--pretty=format:%s", file_path], capture_output=True, text=True)
        messages = result.stdout.splitlines()
        iteration_numbers = [int(line.split("Iteration")[1].split("on")[0].strip()) for line in messages if "Iteration" in line]
        return max(iteration_numbers, default=0) + 1
    except Exception:
        return 1


def commit_and_push(file_path, iteration):
    repo_path = os.getenv("GAD_REPO_PATH")
    github_repo_url = os.getenv("GAD_REPO_URL")
    pat = os.getenv("GAD_GET_PAT")

    if not repo_path or not github_repo_url or not pat:
        raise ValueError("Environment variables not set.")

    date_str = datetime.now().strftime("%c")
    commit_message = COMMIT_MESSAGE_TEMPLATE.format(iteration=iteration, date=date_str)

    git_env = os.environ.copy()
    git_env["GIT_DIR"] = os.path.join(repo_path, ".git")
    git_env["GIT_WORK_TREE"] = repo_path

    subprocess.run(["git", "add", file_path], env=git_env, cwd=repo_path, check=True)
    subprocess.run(["git", "commit", "-m", commit_message], env=git_env, cwd=repo_path, check=True)

    repo_with_auth = github_repo_url.replace("https://", f"https://{pat}@")
    subprocess.run(["git", "push", repo_with_auth], env=git_env, cwd=repo_path, check=True)


# Main Process
if __name__ == "__main__":
    scraper = Scraper()
    scraper.run()

    writer = DataWriter(scraper.item_data)
    writer.to_json(SCRAPER_FILE)

    transformer = Transformer(input_file=TRANSFORMER_INPUT_FILE, output_file=TRANSFORMER_OUTPUT_FILE)
    transformer.run()

    sqlite_writer = SQLiteWriter(input_file=TRANSFORMER_OUTPUT_FILE, db_file=SQLITE_DB_FILE, output_file=SQLITE_JSON_OUTPUT_FILE)
    sqlite_writer.run()

    iteration_count = get_iteration_count(SQLITE_JSON_OUTPUT_FILE)
    commit_and_push(SQLITE_JSON_OUTPUT_FILE, iteration_count)
