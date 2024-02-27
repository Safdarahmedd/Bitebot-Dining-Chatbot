from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import json

# Define AWS credentials
aws_region = 'us-east-1'
aws_access_key = 'ACCESS_KEY'
aws_secret_key = 'SECRET_KEY'
aws_session_token = 'your-session-token'  # If you're using temporary credentials

# Initialize Elasticsearch client
es_host = 'ES_URL'
es_index = 'restaurants'
doc_type = 'Restaurant'
out_file = r"C:/Search_Engines/Crawler/seed_files/yelp_scrap_api.json"

aws_auth = AWS4Auth(
    aws_access_key,
    aws_secret_key,
    aws_region,
    'es',
    #session_token=aws_session_token
)

es = Elasticsearch(
    hosts=[{'host': es_host, 'port': 443}],
    http_auth=aws_auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


# Check if the index exists
if es.indices.exists(index=es_index):
    # Delete the index
    es.indices.delete(index=es_index)
    print(f"Index '{es_index}' deleted successfully.")
else:
    print(f"Index '{es_index}' does not exist.")

# Define index mapping
index_mapping = {
    "mappings": {
        "properties": {
            "id": {"type": "text"},
            "cuisine": {"type": "keyword"}
        }
    }
}

# Create index
es.indices.create(index=es_index, body=index_mapping)

# Load JSON data from file
with open(out_file, 'r') as file:
    restaurant_data = json.load(file)

# Index only id and cuisine for each restaurant document
for restaurant in restaurant_data:
    try:
        cuisine = restaurant["cuisine"]
    except KeyError:
        print(f"Skipping restaurant with missing 'cuisine' field: {restaurant}")
        continue
    doc = {
        "id": str(restaurant["id"]),  # Convert id to string for text type
        "cuisine": restaurant["cuisine"]
    }
    es.index(index=es_index, body=doc)

print("Restaurant data stored in OpenSearch.")

print(f"Elasticsearch index '{es_index}' created successfully.")
