import requests
import ast

ELASTICSEARCH_URL = 'http://localhost:9200/'
FILE_PATH = 'sample-1M.jsonl'


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


process_mappings = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "content": {
                "type": "text",
                "analyzer": "english"
            },
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


# Reindex method
def reindex(num_docs):
    re_index = {
        "source": {
            "index": f"articles_{num_docs}_data"
        }, "dest": {
            "index": f"processed_{num_docs}_data"
        }
    }

    requests.request(method='POST',
                     url=ELASTICSEARCH_URL + '_reindex',
                     json=re_index)
    print("Reindex done!")


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
    create_index(index_name=f'processed_{num_docs}_data', mappings=process_mappings)

    reindex(num_docs)


# Aufgabe 2
# Search method 2a
def search_optional_params(query, index_name, fields):
    data = []

    reponse = requests.request('GET', url=ELASTICSEARCH_URL + index_name + '/_search', json=query)
    res = reponse.json()

    for row in res["hits"]["hits"]:
        data.append(row["_source"].items())

    results = [pair for record in data for pair in record if pair[0] in fields]
    print(results)


# Search method 2b
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


# Aufgabe 3
query3_1 = {
    "query": {
        "match": {
            "content": "Who bought National Geographic magazine?"
        }
    }
}

query3_2 = {
    "query": {
        "match": {
            "content": "What are people fleeing from in Syria?"
        }
    }
}
query3_3 = {
    "query": {
        "match": {
            "content": "How much does the iPad Pro cost?"
        }
    }
}
query3_4 = {
    "query": {
        "match": {
            "content": "What was discovered on Mars?"
        }
    }
}
query3_5 = {
    "query": {
        "match": {
            "content": "How many people flee to Germany?"
        }
    }
}
query3_6 = {
    "query": {
        "match": {
            "content": "Which artists are the headliners at Apple Music Festival?"
        }
    }
}
query3_7 = {
    "query": {
        "match": {
            "content": "When did Edward Snowden write his first Twitter post?"
        }
    }
}
query3_8 = {
    "query": {
        "match": {
            "content": "Which country launched its first space observatory into space?"
        }
    }
}
query3_9 = {
    "query": {
        "match": {
            "content": "What is Chancellor Merkl asking of Facebook?"
        }
    }
}
query3_10 = {
    "query": {
        "match": {
            "content": "Why went Facebook down?"
        }
    }
}


if __name__ == "__main__":

    # Aufgabe 1a+b: Index for the first 10_000 articles + Pre-processing step
    # index_articles(10000)

    # Aufgabe 1c: Repeat with num_docs = 100
    index_articles(100)

    # Aufgabe 2: Search
    # 2a
    fields = ['id', 'title']
    # search_optional_params(query=query1, index_name='processed_100_data', fields=fields)
    # search_optional_params(query=query2, index_name='processed_100_data', fields=fields)

    # 2b
    search_index(query=query3, index_name='processed_100_data')
    # search_index(query=query4, index_name='processed_100_data')

    # Aufgabe 3
    # search_index(query=query3_1, index_name='processed_100_data')
    # search_index(query=query3_2, index_name='processed_100_data')
    # search_index(query=query3_3, index_name='processed_100_data')
    # search_index(query=query3_4, index_name='processed_100_data')
    # search_index(query=query3_5, index_name='processed_100_data')
    # search_index(query=query3_6, index_name='processed_100_data')
    # search_index(query=query3_7, index_name='processed_100_data')
    # search_index(query=query3_8, index_name='processed_100_data')
    # search_index(query=query3_9, index_name='processed_100_data')
    # search_index(query=query3_10, index_name='processed_100_data')


