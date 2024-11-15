import json


class Transformer:
    """Transformer class to process and transform data from a JSON file."""

    def __init__(self, input_file, output_file):
        """
        :param input_file: Path to the input JSON file.
        :param output_file: Path to the output JSON file.
        """
        self.input_file = input_file
        self.output_file = output_file
        self.data = []
        self.transformed_ids = set()  # Track IDs of already transformed records

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

    def load_existing_data(self):
        """Load existing transformed data to avoid duplicates."""
        try:
            with open(self.output_file, "r", encoding="utf-8") as json_file:
                existing_data = json.load(json_file)
                self.transformed_ids = {record["id"] for record in existing_data}  # Track existing IDs
            print(f"Loaded existing transformed data from {self.output_file}")
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"No existing transformed data in {self.output_file}. Starting fresh.")
            self.transformed_ids = set()  # Start fresh if no existing file

    def transform_data(self):
        """Transform the loaded data."""
        if not self.data:
            print("No data to transform.")
            return

        transformed_data = []
        for record in self.data:
            if record.get("id") in self.transformed_ids:
                # Skip records that are already transformed
                continue

            # Ensure transformations are applied consistently
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

            transformed_data.append(record)

        # Append new transformed records to avoid overwriting existing ones
        if self.transformed_ids:
            with open(self.output_file, "r", encoding="utf-8") as json_file:
                existing_data = json.load(json_file)
                transformed_data = existing_data + transformed_data

        self.data = transformed_data  # Update the transformed dataset

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
        self.load_existing_data()  # Load existing data to avoid duplicates
        self.load_data()
        self.transform_data()
        self.save_data()


# Main process
if __name__ == "__main__":
    transformer = Transformer(input_file="out.json", output_file="post.json")
    transformer.run()
