import requests
from auto_scale_common import get_logger


# KIBANA_CLUSTERING_AVG_TIME_URL = "http://cl.dweave.net/api_v2/getdata?_key=text_clustering.status&range_time={start_time},{end_time}&aggs=text_clustering_country&metrics=avg&metric_key=text_clustering_solr_time_taken"
KIBANA_CLUSTERING_AVG_TIME_URL = "http://cl.dweave.net/api_v2/getdata?_key=text_clustering.status&range_time={start_time},{end_time}&aggs=text_clustering_country&metrics=avg&metric_key=text_clustering_solr_time_taken_per_source"
KIBANA_SUGGESTION_AVG_TIME_URL = "http://cl.dweave.net/api_v2/getdata?_key=text_suggestion.status&range_time={start_time},{end_time}&metrics=avg&metric_key=suggestion_solr_qtime"

logger = get_logger('kibana_resp_time')

def get_avg_response_time(key, start_time, end_time):
    """
    Fetches the response time of last 1 hour of clustering or suggestion usages based on key value.
    :param key: str, the type of usage, clustering or suggestion
    :param start_time: str, the start time to look for, to be used in query
    :param end_time: str, the end time to look for, to be used in query
    :return: float, the average response time.
    """

    if key == 'suggestion':
        url = KIBANA_SUGGESTION_AVG_TIME_URL.format(start_time=start_time, end_time=end_time)
    elif key == 'clustering':
        url = KIBANA_CLUSTERING_AVG_TIME_URL.format(start_time=start_time, end_time=end_time)

    logger.info("requesting : {}".format(url))
    resp = requests.get(url)
    logger.info("resp:{}".format(str(resp)))
    if resp.status_code == 500:
        logger.exception(resp.content.decode())

    resp = resp.json()
    avg_resp_time = resp.get("metrics_data", {}).get('avg')
    if avg_resp_time:
        avg_resp_time = float(avg_resp_time)
        # if key == 'clustering':
        #     avg_resp_time = avg_resp_time/1000  # clustering returns in milliseconds
    else:
        avg_resp_time = 0.0

    logger.info('avg response time for {} since {} to current time : {}'.format(key, start_time, avg_resp_time))

    return avg_resp_time
