import os
import json
import sqlite3
import subprocess
from datetime import datetime


class Loader:
    """Loader class to load transformed data into SQLite and commit updates to GitHub."""

    def __init__(self, input_file, db_file, dataset_file):
        self.input_file = input_file
        self.db_file = db_file
        self.dataset_file = dataset_file
        self.data = []

        # Get the GitHub Personal Access Token from environment variable
        self.git_token = os.getenv("GET_A_DEAL_PAT")
        if not self.git_token:
            raise ValueError("GitHub Personal Access Token (GET_A_DEAL_PAT) is not set in your environment.")

    def load_data(self):
        try:
            with open(self.input_file, "r", encoding="utf-8") as json_file:
                self.data = json.load(json_file)
            print(f"Data loaded from {self.input_file}")
        except FileNotFoundError:
            print(f"File {self.input_file} not found.")
        except json.JSONDecodeError:
            print(f"Error decoding JSON from {self.input_file}.")

    def write_to_db(self):
        if not self.data:
            print("No data to write to the database.")
            return

        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
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
            for record in self.data:
                cursor.execute("""
                    INSERT INTO products (title, price, currency, availability, rating, reviews_count, store_name, link, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record["title"],
                    record["price"],
                    record["currency"],
                    record["availability"],
                    record["rating"],
                    record["reviews_count"],
                    record["store_name"],
                    record["link"],
                    record["timestamp"]
                ))
            conn.commit()
            conn.close()
            print(f"Data successfully written to {self.db_file}")
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")

    def write_to_file(self):
        if not self.data:
            print("No data to write to the dataset file.")
            return

        try:
            if os.path.exists(self.dataset_file):
                with open(self.dataset_file, "r+", encoding="utf-8") as json_file:
                    existing_data = json.load(json_file)
                    existing_data.extend(self.data)
                    json_file.seek(0)
                    json.dump(existing_data, json_file, indent=4, ensure_ascii=False)
            else:
                with open(self.dataset_file, "w", encoding="utf-8") as json_file:
                    json.dump(self.data, json_file, indent=4, ensure_ascii=False)
            print(f"Data successfully written to {self.dataset_file}")
        except Exception as e:
            print(f"Error writing to dataset file: {e}")

    def commit_to_github(self):
        if not self.git_token:
            print("GitHub Personal Access Token is missing!")
            return
    
        try:
            record_count = len(self.data)
            timestamp = datetime.now().strftime("%c")
            repo_url = f"https://{self.git_token}@github.com/skravco/get-a-deal.git"
    
            # Stage the dataset file and the counter file
            subprocess.run(["git", "add", self.dataset_file], check=True)
    
            # Assuming you also track the counter file (e.g., counter.json)
            counter_file = "counter.json"  # Specify the counter file if it's different
            subprocess.run(["git", "add", counter_file], check=True)
    
            # Commit the changes with a message including the record count and timestamp
            subprocess.run(["git", "commit", "-m", f"Processed {record_count} records - fetch data for {timestamp}"], check=True)
            
            # Push changes to GitHub
            subprocess.run(["git", "push", repo_url], check=True)
    
            print(f"Changes committed to GitHub with timestamp {timestamp} and record count {record_count}")
        except subprocess.CalledProcessError as e:
            print(f"Git error: {e}")
        except FileNotFoundError:
            print("Git is not installed or not found in the PATH.")

    def run(self):
        self.load_data()
        self.write_to_db()
        self.write_to_file()
        self.commit_to_github()
