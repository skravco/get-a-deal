
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import json
import sqlite3

# Constants for file names
SCRAPER_FILE = "extract.json"
TRANSFORMER_INPUT_FILE = "extract.json"
TRANSFORMER_OUTPUT_FILE = "transform.json"
SQLITE_DB_FILE = "sqlite.db"
SQLITE_JSON_OUTPUT_FILE = "sqlite.json"


class Scraper:
    """Scraper class to retrieve data from a specific product page."""

    def __init__(self):
        self.url = "https://tablet-pc.arukereso.hu/samsung/galaxy-tab-a9-x210-128gb-p1025165686/#"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
        }
        self.item_data = []

    def fetch_data(self):
        """Fetch the page content."""
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.RequestException as e:
            print(f"Failed to retrieve the page: {e}")
            return None

    def parse_data(self, soup):
        """Parse the HTML to extract required item data."""
        items = soup.find_all("div", class_="optoffer device-desktop")
        for item in items:
            link = (
                item.find("a", class_="jumplink-overlay initial")["href"]
                if item.find("a", class_="jumplink-overlay initial")
                else "N/A"
            )

            data = {
                "title": item.find("h4", {"data-akjl": "Product name||ProductName"}).get_text(strip=True)
                if item.find("h4", {"data-akjl": "Product name||ProductName"}) else "N/A",
                "price": item.find("span", itemprop="price").get_text(strip=True)
                if item.find("span", itemprop="price") else "N/A",
                "currency": item.find("meta", itemprop="priceCurrency")["content"]
                if item.find("meta", itemprop="priceCurrency") else "N/A",
                "availability": item.find("link", itemprop="availability")["href"]
                if item.find("link", itemprop="availability") else "N/A",
                "rating": len(item.find_all("span", class_="star icon-star")) +
                          0.5 * len(item.find_all("span", class_="star icon-star-half-alt"))
                if item.find_all("span", class_="star icon-star") else "N/A",
                "reviews_count": item.find("span", class_="reviews-count").get_text(strip=True).strip("()")
                if item.find("span", class_="reviews-count") else "N/A",
                "store_logo": item.find("img", class_="img-responsive logo-host")["src"]
                if item.find("img", class_="img-responsive logo-host") else "N/A",
                "store_name": item.find("div", {"data-akjl": "Store name||StoreName"}).get_text(strip=True)
                if item.find("div", {"data-akjl": "Store name||StoreName"}) else "N/A",
                "link": link,
                "timestamp": datetime.now().isoformat(),
            }
            self.item_data.append(data)

    def run(self):
        """Execute the scraping process."""
        soup = self.fetch_data()
        if soup:
            self.parse_data(soup)


class DataWriter:
    """DataWriter class to save the scraped data into JSON format."""

    def __init__(self, data):
        self.data = data

    def to_json(self, filename):
        """Write data to JSON file."""
        if not self.data:
            print("No data to write to JSON.")
            return

        with open(filename, "w", encoding="utf-8") as json_file:
            json.dump(self.data, json_file, indent=4, ensure_ascii=False)
        print(f"Data written to {filename}")


class Transformer:
    """Transformer class to process and transform data from a JSON file."""

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.data = []

    def load_data(self):
        """Load data from the input JSON file."""
        try:
            with open(self.input_file, "r", encoding="utf-8") as json_file:
                self.data = json.load(json_file)
            print(f"Data loaded from {self.input_file}")
        except FileNotFoundError:
            print(f"File {self.input_file} not found.")
        except json.JSONDecodeError:
            print(f"Error decoding JSON from {self.input_file}.")

    def transform_data(self):
        """Transform the loaded data."""
        if not self.data:
            print("No data to transform.")
            return

        for record in self.data:
            if "price" in record:
                record["price"] = int(record["price"].replace("Ft", "").replace(" ", "").strip())
            if "reviews_count" in record:
                reviews_text = record["reviews_count"].replace("vélemény", "").replace(" ", "").strip()
                if reviews_text.isdigit():
                    record["reviews_count"] = int(reviews_text)
                else:
                    record["reviews_count"] = 0
            if "store_logo" in record:
                del record["store_logo"]

    def save_data(self):
        """Save transformed data to the output JSON file."""
        if not self.data:
            print("No data to save.")
            return

        with open(self.output_file, "w", encoding="utf-8") as json_file:
            json.dump(self.data, json_file, indent=4, ensure_ascii=False)
        print(f"Transformed data saved to {self.output_file}")

    def run(self):
        """Execute the transformation process."""
        self.load_data()
        self.transform_data()
        self.save_data()


class SQLiteWriter:
    """SQLiteWriter class to handle SQLite database operations."""

    def __init__(self, input_file, db_file, output_file):
        self.input_file = input_file
        self.db_file = db_file
        self.output_file = output_file

    def write_to_db(self):
        """Write JSON data into the SQLite database."""
        try:
            with open(self.input_file, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)

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
                    record.get("title", "N/A"),
                    record.get("price", 0),
                    record.get("currency", "N/A"),
                    record.get("availability", "N/A"),
                    record.get("rating", 0),
                    record.get("reviews_count", 0),
                    record.get("store_name", "N/A"),
                    record.get("link", "N/A"),
                    record.get("timestamp", datetime.now().isoformat()),
                ))

            conn.commit()
            conn.close()
            print("Data written to SQLite database.")

        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error processing input file: {e}")

    def export_to_json(self):
        """Export SQLite database contents to JSON."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM records")
        rows = cursor.fetchall()

        column_names = [description[0] for description in cursor.description]
        data = [dict(zip(column_names, row)) for row in rows]

        with open(self.output_file, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)

        print(f"SQLite database exported to {self.output_file}.")
        conn.close()

    def run(self):
        """Execute the SQLite write and export process."""
        self.write_to_db()
        self.export_to_json()


# Main process
if __name__ == "__main__":
    scraper = Scraper()
    scraper.run()

    writer = DataWriter(scraper.item_data)
    writer.to_json(SCRAPER_FILE)

    transformer = Transformer(input_file=TRANSFORMER_INPUT_FILE, output_file=TRANSFORMER_OUTPUT_FILE)
    transformer.run()

    sqlite_writer = SQLiteWriter(
        input_file=TRANSFORMER_OUTPUT_FILE,
        db_file=SQLITE_DB_FILE,
        output_file=SQLITE_JSON_OUTPUT_FILE
    )
    sqlite_writer.run()
