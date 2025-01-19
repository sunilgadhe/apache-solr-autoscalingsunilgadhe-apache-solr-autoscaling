import requests
from concurrent.futures import ThreadPoolExecutor
import time

# URL to be requested
url = 'http://20.197.14.100:8987/solr/#/solr-collection-config/query?q=*:*&q.op=OR&indent=true&useParams='

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

# Function to execute a batch of queries
def execute_batch(num_queries):
    with ThreadPoolExecutor(max_workers=num_queries) as executor:
        futures = [executor.submit(make_request) for _ in range(num_queries)]
        for future in futures:
            future.result()  # Wait for all futures to complete

# Main function
if __name__ == '__main__':
    queries_per_minute = 1500
    seconds_per_minute = 60
    queries_per_second = int(queries_per_minute / seconds_per_minute)  # Convert to integer
    fraction_of_second = (queries_per_minute / seconds_per_minute) - queries_per_second
    sleep_time = 1 / queries_per_second  # Time to sleep between each query

    while True:
        start_time = time.time()
        queries_executed = 0

        while queries_executed < queries_per_minute:
            batch_size = queries_per_second
            if queries_executed + batch_size > queries_per_minute:
                batch_size = queries_per_minute - queries_executed

            execute_batch(batch_size)
            queries_executed += batch_size
            time.sleep(sleep_time)  # Sleep to maintain the query rate

        # Wait for the rest of the minute to ensure exactly 1000 queries per minute
        elapsed_time = time.time() - start_time
        if elapsed_time < seconds_per_minute:
            time.sleep(seconds_per_minute - elapsed_time)

