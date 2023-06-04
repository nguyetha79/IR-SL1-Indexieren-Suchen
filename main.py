import json
import requests
import ast

ELASTICSEARCH_URL = 'http://localhost:9200/'
ORIGINAL_FILE_PATH = 'sample-1M.jsonl'
EXTRACTED_DATA_FILE_PATH = 'articles_data.json'

# Aufgabe 1
# Extract the first 10_000 data
# since sample-1M.jsonl is too large to push into Github, so I deleted this file


def extract_the_first_10000_data():
    first_10000_data = []

    with open(ORIGINAL_FILE_PATH, 'r') as original_file:
        for line in original_file:
            try:
                data = json.loads(line)
                first_10000_data.append(data)
            except json.JSONDecodeError as e:
                print(f"Error parsing line: {line}. {e}")

    # Get the first 10 objects
    first_10000_data = first_10000_data[:10000]

    # Write the first 10 objects to the new JSON file
    with open(EXTRACTED_DATA_FILE_PATH, 'w') as new_file:
        json.dump(first_10000_data, new_file, indent=4)

    print('First 10000 data have been extracted and saved to the new articles_data.json file.')


# Create index
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
#create_index(index_name='articles_data', mappings=mappings)

