import requests
import ast
import re

ELASTICSEARCH_URL = 'http://localhost:9200/'
FILE_PATH = 'sample-1M.jsonl'


# Convert method: list of dicts into list of strings
def convert(list_of_dicts):
    list_of_strings = []

    for dictionary in list_of_dicts:
        string_representation = str(dictionary)
        list_of_strings.append(string_representation)

    return list_of_strings


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
        requests.request(method='POST',
                         url=ELASTICSEARCH_URL + index_url + str(idx),
                         json=doc)

    print("Index done!")


# Pre-process method
def process(line):
    # cleaned_line = re.sub("\\\\u[\\d\\w]{4}|\\\\\\w|(\\\\)(?=\\\\/)", '', line)
    cleaned_line = re.sub("\\\\u[\\d\\w]{4}|\\\\\\w", '', line)
    cleaned_line = re.sub("\\s+", ' ', cleaned_line)

    return cleaned_line


# Aufgabe 1
def index_articles(num_docs):
    # Index
    create_index(index_name=f'articles_{num_docs}_data', mappings=mappings)

    with open(FILE_PATH, 'r') as json_file:
        articles_data = json_file.readlines()

    articles_data = [ast.literal_eval(line) for line in articles_data]
    articles_data = articles_data[:num_docs]

    articles_index_url = f'articles_{num_docs}_data/_create/_'
    index_docs(articles_data, articles_index_url)

    # Index with Pre-processing step
    create_index(index_name=f'processed_{num_docs}_data', mappings=mappings)

    cleaned_data = []
    articles_data = convert(articles_data)

    for line in articles_data:
        cleaned_line = process(line)
        cleaned_data.append(cleaned_line)

    cleaned_data = [ast.literal_eval(line) for line in cleaned_data]
    articles_processed_url = f'processed_{num_docs}_data/_create/_'
    index_docs(cleaned_data, articles_processed_url)


# Aufgabe 2
# Search method
def search_index(query, index_name):
    reponse = requests.request('GET', url=ELASTICSEARCH_URL + index_name + '/_search', json=query)
    print("Query reponse: ")
    print(reponse.json())


# Search queries for 2a
query1 = {
    "query": {
        "match": {
            "media-type": "blog"
        }
    }
}

query2 = {
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "content": "internet"
          }
        },
        {
          "match": {
            "media-type": "news"
          }
        }
      ]
    }
  }
}

# Boolean queries for 2b

# "internet" AND "market" AND NOT source = "TradingCharts.com"
query3 = {
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "content": "internet"
          }
        },
        {
          "match": {
            "content": "market"
          }
        }
      ],
      "must_not": {
        "match": {
          "source": "TradingCharts.com"
        }
      }
    }
  }
}

# ("Tesla" OR "Elon Musk") AND "electric vehicles"
query4 = {
  "query": {
    "bool": {
      "should": [
        {
          "match": {
            "content": "telsa"
          }
        },
        {
          "match": {
            "content": "elon musk"
          }
        }
      ],
      "must": {
        "match": {
          "content": "electric vehicles"
        }
      }
    }
  }
}


if __name__ == "__main__":

    # Aufgabe 1a+b: Index for the first 10_000 articles + Pre-processing step
    index_articles(10000)

    # Aufgabe 1c: Repeat with num_docs = 100
    index_articles(100)

    # Aufgabe 2: Search
    # Search queries
    search_index(query=query1, index_name='processed_100_data')
    search_index(query=query2, index_name='processed_100_data')
    # Boolean queries
    search_index(query=query3, index_name='processed_100_data')
    search_index(query=query4, index_name='processed_100_data')


