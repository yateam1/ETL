es_settings = {
    "refresh_interval": "1s",
    "analysis": {
        "filter": {
            "russian_stop": {
                "type": "stop",
                "stopwords": "_russian_"
            },
            "russian_stemmer": {
                "type": "stemmer",
                "language": "russian"
            }
        },
        "analyzer": {
            "ru": {
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "russian_stop",
                    "russian_stemmer"
                ]
            }
        }
    }
}
es_mappings = {
    "dynamic": "strict",
    "properties": {
        "text_field": {
            "type": "text",
            "analyzer": "ru"
        },
        "number": {
            "type": "long"
        }
    }
}
