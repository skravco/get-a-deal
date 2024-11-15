from datetime import datetime
from bs4 import BeautifulSoup
import requests
import json

class Scraper:
    """Scraper class to retrieve data from a specific product page."""

    def __init__(self, counter_file="counter.json"):
        self.url = "https://tablet-pc.arukereso.hu/samsung/galaxy-tab-a9-x210-128gb-p1025165686/#"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
        }
        self.item_data = []
        self.counter_file = counter_file
        self.counter = self.load_counter()

    def load_counter(self):
        """Load the counter value from a file."""
        try:
            with open(self.counter_file, "r", encoding="utf-8") as f:
                return json.load(f).get("counter", 1)
        except (FileNotFoundError, json.JSONDecodeError):
            return 1  # Default to 1 if the file doesn't exist

    def save_counter(self):
        """Save the current counter value to a file."""
        with open(self.counter_file, "w", encoding="utf-8") as f:
            json.dump({"counter": self.counter}, f)

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
                "id": self.counter,  # Add the auto-increment ID
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
            self.counter += 1  # Increment the counter for the next record

    def run(self):
        """Execute the scraping process."""
        soup = self.fetch_data()
        if soup:
            self.parse_data(soup)
        self.save_counter()  # Save the updated counter

# Main process
if __name__ == "__main__":
    scraper = Scraper()
    scraper.run()

    # Save to out.json
    with open("out.json", "w", encoding="utf-8") as f:
        json.dump(scraper.item_data, f, indent=4, ensure_ascii=False)
    print(f"Data written to out.json")
