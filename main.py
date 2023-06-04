import json
import requests
import ast
import re

ELASTICSEARCH_URL = 'http://localhost:9200/'
ORIGINAL_FILE_PATH = 'data/sample-1M.jsonl'

EXTRACTED_10K_DATA_FILE_PATH = 'data/extracted_10K_data.json'
ARTICLES_10K_FILE_PATH = 'data/articles_10k_data.json'
PROCESSED_10K_DATA_FILE_PATH = 'data/processed_10K_data.json'

EXTRACTED_100_DATA_FILE_PATH = 'data/extracted_100_data.json'
ARTICLES_100_FILE_PATH = 'data/articles_100_data.json'
PROCESSED_100_DATA_FILE_PATH = 'data/processed_100_data.json'


# AUFGABE 1

# Format JSON File
def format_json_file(file_path, new_path):
    with open(file_path, 'r') as file:
        file_content = file.read()

    # Add new line after the closing curly braces
    file_content = file_content.replace('},', '}\n')

    # Remove the square brackets
    # Stack Overflow: https://stackoverflow.com/questions/3411771/best-way-to-replace-multiple-characters-in-a-string
    for char in ['[', ']']:
        if char in file_content:
            file_content = file_content.replace(char, '')

    # Remove spaces at the beginning of each line in a JSON file
    # Stack Overflow: https://stackoverflow.com/questions/9189172/python-string-replace.
    file_content = '\n'.join([line.lstrip() for line in file_content.splitlines()])

    with open(new_path, 'w') as file:
        file.write(file_content)


# Extract data
def extract_data(num_docs, file_path, output_path):
    data_list = []
    with open(ORIGINAL_FILE_PATH, 'r') as original_file:
        for line in original_file:
            data = json.loads(line)
            data_list.append(data)

    # Get the first 10_000 objects
    data_list = data_list[:num_docs]

    # Write the first 10_000 objects to the new JSON file
    with open(file_path, 'w') as new_file:
        json.dump(data_list, new_file)

    # Format file for indexing
    format_json_file(file_path, output_path)

    print(f'Data have been extracted and saved to the {output_path} file.')


# Create index method
def create_index(index_name, mappings):
    response = requests.request(method='PUT',
                                url=ELASTICSEARCH_URL + index_name,
                                json=mappings)
    print(response.json())


mappings = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "content": {"type": "text"},
            "title": {"type": "text"},
            "media-type": {"type": "text"},
            "source": {"type": "text"},
            "published": {"type": "date"}
        }
    }
}


# Index method
def index_docs(document_dicts, index_url):
    for idx, doc in enumerate(document_dicts):
        response = requests.request(method='POST',
                                    url=ELASTICSEARCH_URL + index_url + str(idx),
                                    json=doc)
        print(response.json())


# Pre-process method
def process_data(file_path, new_path):
    with open(file_path, 'r') as json_file:
        data = json_file.readlines()
        cleaned_data = []

        for line in data:
            cleaned_line = re.sub("\\\\u[\\d\\w]{4}|\\\\\\w", '', line)
            cleaned_line = re.sub("\\s+", ' ', cleaned_line)
            cleaned_data.append(cleaned_line)

    with open(new_path, 'w') as output_file:
        for line in cleaned_data:
            output_file.write(line + '\n')


def index_10K_articles():
    extract_data(10000, EXTRACTED_10K_DATA_FILE_PATH, ARTICLES_10K_FILE_PATH)

    create_index(index_name='articles_10K_data', mappings=mappings)

    with open(ARTICLES_10K_FILE_PATH, 'r') as json_file:
        articles_data = json_file.readlines()

    articles_data = [ast.literal_eval(line) for line in articles_data]
    articles_index_url = 'articles_10K_data/_create/_'
    index_docs(articles_data, articles_index_url)


def index_processed_10K_articles():
    process_data(ARTICLES_10K_FILE_PATH, PROCESSED_10K_DATA_FILE_PATH)

    create_index(index_name='processed_10K_data', mappings=mappings)

    with open(PROCESSED_10K_DATA_FILE_PATH, 'r') as json_file:
        processed_data = json_file.readlines()

    processed_data = [ast.literal_eval(line) for line in processed_data]
    process_index_url = 'processed_10K_data/_create/_'
    index_docs(processed_data, process_index_url)


def index_100_articles():
    extract_data(100, EXTRACTED_100_DATA_FILE_PATH, ARTICLES_100_FILE_PATH)

    create_index(index_name='articles_100_data', mappings=mappings)

    with open(ARTICLES_100_FILE_PATH, 'r') as json_file:
        articles_data = json_file.readlines()

    articles_data = [ast.literal_eval(line) for line in articles_data]
    articles_index_url = 'articles_100_data/_create/_'
    index_docs(articles_data, articles_index_url)


def index_processed_100_articles():
    process_data(ARTICLES_100_FILE_PATH, PROCESSED_100_DATA_FILE_PATH)

    create_index(index_name='processed_100_data', mappings=mappings)

    with open(PROCESSED_100_DATA_FILE_PATH, 'r') as json_file:
        processed_data = json_file.readlines()

    processed_data = [ast.literal_eval(line) for line in processed_data]
    process_index_url = 'processed_100_data/_create/_'
    index_docs(processed_data, process_index_url)


if __name__ == "__main__":

    # Aufgabe 1a: Index for the first 10_000 articles
    index_10K_articles()

    # Aufgabe 1b: Index for the first 10_000 articles with Pre-processing step
    index_processed_10K_articles()

    # Aufgabe 1c: Repeat with num_docs = 100
    index_100_articles()
    index_processed_100_articles()
