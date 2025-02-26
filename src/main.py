import csv
import json
import os

# Extractor
class Extractor:
    def extract(self):
        raise NotImplementedError("Subclasses must implement this method")
    
# CSV Extractor
class CSVExtractor:
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        data = []
        with open(self.file_path, mode='r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                data.append(row)
        return data

# JSON Extractor
class JSONExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        with open(self.file_path, mode='r') as json_file:
            data = json.load(json_file)
        return data

# # Transformer
# class Transformer:
#     def transform(self, data):
#         for row in data:
#             for key, value in row.items():
#                 if isinstance(value, str):
#                     row[key] = value.strip()  # Trim spaces
#         return data

# # Loader
# class Loader:
#     def __init__(self, output_path):
#         self.output_path = output_path

#     def load(self, data):
#         if not data:
#             print("No data to save.")
#             return
        
#         os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
#         fieldnames = data[0].keys()  # Assume all rows have the same structure

#         with open(self.output_path, mode='w', newline='', encoding='utf-8') as csv_file:
#             writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
#             writer.writeheader()
#             writer.writerows(data)
#         print(f"Data successfully saved to {self.output_path}")

# # ETL Pipeline
# class ETLPipeline:
#     def __init__(self, extractors, transformer, loader):
#         self.extractors = extractors
#         self.transformer = transformer
#         self.loader = loader

#     def run(self):
#         extracted_data = []
#         for extractor in self.extractors:
#             extracted_data.extend(extractor.extract())

#         transformed_data = self.transformer.transform(extracted_data)
#         self.loader.load(transformed_data)

# Main execution
if __name__ == "__main__":
    # Fetch the base directory of taxi-pipeline path
    base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print(f'Base directory path {base_directory}\n')

    # Fetch the CSV directory
    csv_directory = os.path.join(base_directory, "data/csv")
    print(f'CSV folder path: {csv_directory}\n')
    csv_list = os.listdir(csv_directory)
    print(f'CSV list of files: {csv_list}\n')

    # Fetch the JSON directory
    json_directory = os.path.join(base_directory, "data/json")
    print(f'JSON folder path: {json_directory}\n')
    json_list = os.listdir(json_directory)
    print(f'JSON list of files: {json_list}\n')

    # this ETL is not done yet, still need to do transformations and load fuctions