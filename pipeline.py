import json
import sqlite3
import subprocess
from datetime import datetime
from extractor import Scraper  # Correct class name for the scraper
from transformer import Transformer
from loader import Loader

class Pipeline:
    """Pipeline class to run the entire process from scraping, transforming, to loading."""

    def __init__(self, extractor, transformer, loader):
        """
        :param extractor: Instance of the Extractor (Scraper) class.
        :param transformer: Instance of the Transformer class.
        :param loader: Instance of the Loader class.
        """
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        """Run the entire pipeline."""
        print("Starting pipeline...")

        # Step 1: Run the Extractor (Scraping Data)
        print("Running extractor (scraper)...")
        try:
            self.extractor.run()
        except Exception as e:
            print(f"Error during extraction: {e}")
            return  # Stop pipeline on failure

        # Step 2: Run the Transformer (Transforming Data)
        print("Running transformer...")
        try:
            self.transformer.run()
        except Exception as e:
            print(f"Error during transformation: {e}")
            return  # Stop pipeline on failure

        # Step 3: Run the Loader (Loading Data into DB and Pushing to GitHub)
        print("Running loader...")
        try:
            self.loader.run()
        except Exception as e:
            print(f"Error during loading: {e}")
            return  # Stop pipeline on failure

        print("Pipeline completed successfully.")

# Main process to run the pipeline
if __name__ == "__main__":
    # Initialize the Extractor, Transformer, and Loader classes
    extractor = Scraper()  # Pass output file explicitly
    transformer = Transformer(input_file="out.json", output_file="post.json")
    loader = Loader(input_file="post.json", db_file="sqlite.db", dataset_file="saved.json")

    # Initialize the pipeline with the instances
    pipeline = Pipeline(extractor=extractor, transformer=transformer, loader=loader)

    # Run the pipeline
    pipeline.run()
