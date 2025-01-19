import requests
from concurrent.futures import ThreadPoolExecutor
import time

# URL to be requested
url = 'http://20.197.14.100:8987/solr/solr-collection-config/select?indent=true&q.op=OR&q=*%3A*&useParams='
#url = 'http://20.197.20.112:8987/solr/solr-collection-config/select?indent=true&q.op=OR&q=*%3A*&useParams='
#url = 'http://20.197.20.112:8987/solr/solr-collection-config/select?indent=true&q.op=OR&q=*%3A*&useParams=&shards.preference=replica.type:PULL'
# Function to make a single HTTP request
def make_request():
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print('Request successful')
        else:
            print(f'Failed with status code: {response.status_code}')
    except Exception as e:
        print(f'An error occurred: {e}')

# Function to continuously make requests in parallel
def continuous_requests():
    with ThreadPoolExecutor(max_workers=4) as executor:
        while True:
            futures = [executor.submit(make_request) for _ in range(10)]
            for future in futures:
                future.result()  # Wait for all futures to complete

# Main function
if __name__ == '__main__':
    continuous_requests()

