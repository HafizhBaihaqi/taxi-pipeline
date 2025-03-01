import os
import pandas as pd
import re

# Extractor
class Extractor:
    def extract(self):
        raise NotImplementedError('Subclasses must implement this method')
    
# CSV Extractor using Pandas
class CSVExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        # Fetch the base directory of taxi-pipeline path
        base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(f'Base directory path {base_directory}\n')

        full_path = os.path.join(base_directory, self.file_path)

        # Check if the given path is a directory or a file
        ## This is to accomodate a single lookup file (e.g payment_type) that can be hard-coded
        if os.path.isdir(full_path):
            # Fetch the CSV files within the directory
            print(f'Fetching CSV files...\n')
            csv_files = os.listdir(full_path)
            print(f'CSV list of files: {csv_files}\n')

            # Reading CSV files
            print(f'Reading CSV files...\n')
            csv_extractor = [pd.read_csv(os.path.join(full_path, file)) for file in csv_files] 
            print(f'Concancenate CSV files...\n')
            df_csv = pd.concat(csv_extractor)

            return df_csv
        
        elif os.path.isfile(full_path):
            # Read the single CSV file
            print(f'Reading supporting file...\n')
            df_csv = pd.read_csv(full_path)

            return df_csv
        
        else:
            raise FileNotFoundError(f'File or directory not found: {full_path}')

# JSON Extractor using Pandas
class JSONExtractor(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        # Fetch the base directory of taxi-pipeline path
        base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(f'Base directory path {base_directory}\n')

        # Fetch the JSON directory
        json_directory = os.path.join(base_directory, self.file_path)
        print(f'JSON folder path: {json_directory}\n')

        # Fetch the JSON files within the directory
        json_files = os.listdir(json_directory)
        print(f'JSON list of files: {json_files}\n')

        # Reading JSON files
        print(f'Reading JSON files...\n')
        json_extractor = [pd.read_json(os.path.join(json_directory, file)) for file in json_files] 
        print(f'Concancenate JSON files...\n')
        df_json = pd.concat(json_extractor)

        return df_json

# Transformer
class Transformer:
    def __init__(self, csv_extractor, json_extractor, payment_extractor):
        self.csv_extractor = csv_extractor
        self.json_extractor = json_extractor
        self.payment_extractor = payment_extractor

    def transform(self):
        # Extract CSV and JSON
        df_csv = self.csv_extractor.extract()
        df_json = self.json_extractor.extract()
        
        # Extract payment type file
        df_payment_type = self.payment_extractor.extract()

        # Concatenate data
        print(f'Concancenate CSV and JSON files...\n')
        df_concatenated = pd.concat([df_csv, df_json])

        # Transforming
        print(f'Transforming...')
        df_transformed = df_concatenated.copy()
        ## 1. Normalizing columns
        df_transformed.columns = [re.sub(r'([a-z])([A-Z])', r'\1_\2', col).lower() for col in df_transformed.columns]
        print(f'Transformed column names: {df_transformed.columns}\n')

        ## 2. Add trip duration in minutes
        print(f'Creating trip duration in minutes based on time interval between pickup and drop off...\n')
        df_transformed['trip_duration_m'] = (pd.to_datetime(df_transformed['lpep_dropoff_datetime']) - 
                                             pd.to_datetime(df_transformed['lpep_pickup_datetime'])).dt.components.minutes

        ## 3. Look up to payment type
        print(f'Changing payment type to a readable format...\n')
        df_transformed = df_transformed.merge(df_payment_type, how='left', on='payment_type')
        df_transformed['payment_type'] = df_transformed['description']
        df_transformed.drop('description', axis=1, inplace=True)

        ## 4. Convert trip_distance from Mil to KM
        print(f'Convert trip distance from Miles to Kilometers...\n')
        df_transformed['trip_distance'] = df_transformed['trip_distance'] * 1.6

        return df_concatenated, df_transformed

# Loader
class Loader:
    def __init__(self, output_path, df):
        self.output_path = output_path
        self.df = df

    def stag_load(self):
        # Fetch the base directory of taxi-pipeline path
        base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # Define staging path
        stag_path = os.path.join(base_directory, self.output_path)

        # Save to CSV
        print(f'Saving staging file to {stag_path}\n')
        return  self.df.to_csv(os.path.join(stag_path, 'staging_file.csv'))

    def transformed_load(self):
        # Fetch the base directory of taxi-pipeline path
        base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Define staging path
        transformed_path = os.path.join(base_directory, self.output_path)

        # Save to CSV
        print(f'Saving transformed file to {transformed_path}\n')
        return  self.df.to_csv(os.path.join(transformed_path, 'transformed_file.csv'))

# ETL Pipeline
class ETLPipeline:
    def __init__(self, csv_extractor, json_extractor, payment_extractor, transformer):
        self.csv_extractor = csv_extractor
        self.json_extractor = json_extractor
        self.payment_extractor = payment_extractor
        self.transformer = transformer

    def run(self):
        print('Running ETL Pipeline...\n')

        # Extraction
        df_csv = self.csv_extractor.extract()
        df_json = self.json_extractor.extract()
        df_payment_type = self.payment_extractor.extract()

        # Transformation
        df_concatenated, df_transformed = self.transformer.transform()

        # Loading
        staging_loader = Loader('staging/', df_concatenated)
        transformed_loader = Loader('result/', df_transformed)

        staging_loader.stag_load()
        transformed_loader.transformed_load()
        
# Main execution
if __name__ == '__main__':
    # Define path for the extractor
    csv_extractor = CSVExtractor('data/csv')
    json_extractor = JSONExtractor('data/json')
    payment_extractor = CSVExtractor('data/payment_type.csv')

    # Define transformer
    transformer = Transformer(csv_extractor, json_extractor, payment_extractor)

    # Run ETL Pipeline
    etl_pipeline = ETLPipeline(csv_extractor, json_extractor, payment_extractor, transformer)
    etl_pipeline.run()