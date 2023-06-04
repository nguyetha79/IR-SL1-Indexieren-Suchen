import json
import requests
import ast

ELASTICSEARCH_URL = 'http://localhost:9200/'
ORIGINAL_FILE_PATH = 'data/sample-1M.jsonl'
EXTRACTED_DATA_FILE_PATH = 'data/extracted_data.json'
ARTICLES_DATA_FILE_PATH = 'data/articles_data.json'

extracted_data_list = []


# AUFGABE 1
# Format JSON File
def format_json_file(file_path, new_path):
    with open(file_path, 'r') as file:
        file_content = file.read()

    # Add new line after the closing curly braces
    file_content = file_content.replace('},', '}\n')

    # Remove the square brackets
    for char in ['[', ']']:
        if char in file_content:
            file_content = file_content.replace(char, '')

    # Remove spaces at the beginning of each line in a JSON file
    file_content = '\n'.join([line.lstrip() for line in file_content.splitlines()])

    with open(new_path, 'w') as file:
        file.write(file_content)


# Extract the first 10_000 data
def extract_data(data_list):

    with open(ORIGINAL_FILE_PATH, 'r') as original_file:
        for line in original_file:
            try:
                data = json.loads(line)
                data_list.append(data)
            except json.JSONDecodeError as e:
                print(f"Error parsing line: {line}. {e}")

    # Get the first 10_000 objects
    data_list = data_list[:5]

    # Write the first 10_000 objects to the new JSON file
    with open(EXTRACTED_DATA_FILE_PATH, 'w') as new_file:
        json.dump(data_list, new_file)

    # Format file for indexing
    format_json_file(EXTRACTED_DATA_FILE_PATH, ARTICLES_DATA_FILE_PATH)

    print('First 10_000 data have been extracted and saved to the articles_data.json file.')


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
def index_docs(document_dicts):
    for idx, doc in enumerate(document_dicts):
        response = requests.request(method='POST',
                                    url=ELASTICSEARCH_URL + 'articles_data/_create/_' + str(idx),
                                    json=doc)
        print(response.json())


if __name__ == "__main__":
    extract_data(extracted_data_list)

    # Aufgabe 1a: Index for the first 10_000 articles
    create_index(index_name='articles_data', mappings=mappings)

    with open(ARTICLES_DATA_FILE_PATH, 'r') as json_file:
        articles_data = json_file.readlines()

    articles_data = [ast.literal_eval(line) for line in articles_data]
    index_docs(articles_data)

    print(len(extracted_data_list))

    # Aufgabe 1b: Index for the first 10_000 articles with Pre-processing step
