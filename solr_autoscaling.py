import json
import time
from datetime import datetime, timedelta
from collections import defaultdict

from azure_vmss_utils import AzureVmss
from solr_utils import SolrUtil
from kibana_utils import get_avg_response_time
from scaling import scale_in, scale_out, find_empty_nodes, remove_empty_nodes_solr

from autoscale_conf import collections_to_monitor, collection_weights, WAIT_BEFORE_NODE_DELETION_SECS, MAX_MACHINES
from autoscale_conf import vmss_resource_group, vmss_name
from autoscale_conf import SOLR_BASE_NODES, SOLR_DOMAIN
from autoscale_conf import HIGH_SEARCH_RATE_THRESHOLD, LOW_SEARCH_RATE_THRESHOLD
from autoscale_conf import SKIP_KIBANA, MAX_NEW_REPLICAS_IN_AS_VMS
from autoscale_conf import KIBANA_RESPONSE_FOR_LAST_MINS_SUGGESTION, KIBANA_RESPONSE_FOR_LAST_MINS_CLUSTERING
from autoscale_conf import KIBANA_CLUSTERING_RESPONSE_TIME_LOW_THRESHOLD, KIBANA_CLUSTERING_RESPONSE_TIME_HIGH_THRESHOLD
from autoscale_conf import KIBANA_SUGGESTION_RESPONSE_TIME_LOW_THRESHOLD, KIBANA_SUGGESTION_RESPONSE_TIME_HIGH_THRESHOLD
from autoscale_conf import KIBANA_CLUSTERING_KEY, KIBANA_SUGGESTION_KEY

from auto_scale_common import get_logger
from auto_scale_common import clog


def get_timestamp_last_n_mins(n=60):
    """
    calculates the current time stamp with date and subtracts the no. of minutes given
    :param n: int, no. of minutes to subtract from current time
    :return: str, the timestamp of format '%Y%m%d%H%M%S' after subtracting n minutes
    """
    return (datetime.now() - timedelta(minutes=n)).strftime('%Y%m%d%H%M%S')


def kibana_resp_time_scaling(key):
    """
    fetches the kibana response time for clustering and suggestion based input key and finds out whether to scale in or scale out or none.
    if avg response time is less than threshold KIBANA_RESPONSE_TIME_THRESHOLD, then sets all col.shard values to 0 in order to scale in everything.
    :param key: str, values expected: "clustering" or "suggestion"
    :return: dict, dictates based on kibana response time thresholds whether to scale in or scale out or none.
    """

    logger.info('Key: {}'.format(key))
    res = {"scale_in": True, "scale_out": True}
    avg_key_res_time = None

    if key == 'clustering':
        last_n_min_timestamp = get_timestamp_last_n_mins(n=KIBANA_RESPONSE_FOR_LAST_MINS_CLUSTERING)
        logger.info('Fetching kibana logs from time: {} to now'.format(last_n_min_timestamp))
        avg_key_res_time = get_avg_response_time(key='clustering', start_time=last_n_min_timestamp, end_time='')
        logger.info('clustering average time: {}'.format(avg_key_res_time))

        low_threshold = KIBANA_CLUSTERING_RESPONSE_TIME_LOW_THRESHOLD
        high_threshold = KIBANA_CLUSTERING_RESPONSE_TIME_HIGH_THRESHOLD

    elif key == 'suggestion':
        last_n_min_timestamp = get_timestamp_last_n_mins(n=KIBANA_RESPONSE_FOR_LAST_MINS_SUGGESTION)
        logger.info('Fetching kibana logs from time: {} to now'.format(last_n_min_timestamp))
        avg_key_res_time = get_avg_response_time(key='suggestion', start_time=last_n_min_timestamp, end_time='')
        logger.info('suggestion average time: {}'.format(avg_key_res_time))

        low_threshold = KIBANA_SUGGESTION_RESPONSE_TIME_LOW_THRESHOLD
        high_threshold = KIBANA_SUGGESTION_RESPONSE_TIME_HIGH_THRESHOLD

    else:  # avoid kibana impact if key is wrong
        avg_key_res_time = 0.0
        logger.warning(
            "invalid key: {}, so override kibana logs and decide only based on solr avg response times (solr metrics)".format(
                key))

        low_threshold = 0
        high_threshold = 100

    if avg_key_res_time is None:  # avoid kibana impact if anything is wrong with kibana response itself
        avg_key_res_time = 1
        low_threshold = 0
        high_threshold = 2

    if low_threshold < avg_key_res_time < high_threshold:
        res['scale_in'] = True
        res['scale_out'] = True
    elif avg_key_res_time < low_threshold:
        res['scale_in'] = True
        res['scale_out'] = False
    elif avg_key_res_time > high_threshold:
        res['scale_in'] = False
        res['scale_out'] = True

    return avg_key_res_time, res


def avg_search_rates_for_shards(collection_search_rates):
    """
    given collection level search rates of each shard and each replica,
    the function averages the search rates of all replicas of respective shards and returns dict with shard as key,
    and value as dict contains 1minRate, 5minRate, 15minRate which are avgs of all replicas
    :param collection_search_rates: dict, with key as shard.replica and value as rates dict
    :return: dict, with shard as key, and value as dict contains 1minRate, 5minRate, 15minRate which are avgs of all replicas of the corresponding shard.
    """
    shard_rates = defaultdict(list)
    logger.info(f'collection search rates: {collection_search_rates}')
    for shard_replica, rates in collection_search_rates.items():
        shard, replica = shard_replica.split(".")[-2:]
        #if not replica.split('_')[-1].startswith('p'):  # use only pull replicas for calculating hot/cold shards
        #   continue
        rates['replica'] = shard_replica
        shard_rates[shard].append(rates)

    logger.info("shard rates: {}".format(shard_rates))

    avg_shard_rates = {}
    for shard, rates_list in shard_rates.items():
        one_mins = []
        five_mins = []
        fifteen_mins = []
        for dict_obj in rates_list:
            one_mins.append(dict_obj.get('1minRate', 0))
            five_mins.append(dict_obj.get('5minRate', 0))
            fifteen_mins.append(dict_obj.get('15minRate', 0))

        avg_shard_rates[shard] = {
            '1minRate': round(sum(one_mins) / len(one_mins), 2) if one_mins else 0,
            '5minRate': round(sum(five_mins) / len(five_mins), 2) if five_mins else 0,
            '15minRate': round(sum(fifteen_mins) / len(fifteen_mins), 2) if fifteen_mins else 0
        }

    return avg_shard_rates


def find_hot_cold_shards(azure_obj, solr_obj):
    """
    the actual scaling logic which use the AzureVmss and SolrUtil objects to fetch the current machines in vmss group and the current shards in vms and other details,
    then fetches the get average search rates from solr metrics api for all shards of all collections and based on average search rates decide which shards are hot or cold and add how many replicas should there be currently.
    the value of no. of replicas for each collection.shard calculated represents how many replicas should be present immaterial of how many are there running in vmss group,
    hom many new is to be calculated by scale in and scale out part by subtracting how many are current present in the vmss group.

    :param azure_obj: AzureVMSS object to interact with azure vmss
    :param solr_obj: SolrUtil object to interact with solr
    :return: dict, the dict with key col.shard and value as no. of required replicas in total
    """
    try:
        _, machines_ip_ids = azure_obj.get_current_machines()
    except Exception as e:
        logger.error(str(e))
        machines_ip_ids = {}

    logger.info("current VMs: {}".format(machines_ip_ids))

    if len(machines_ip_ids) > 0:
        vm_shards, shard_ip_mapping = solr_obj.get_vm_solr_status(list(machines_ip_ids.keys()), collections_to_monitor)
    else:
        vm_cores = {}
        shard_ip_mapping = {}

    logger.info("current shards to VMs distribution: {}".format(shard_ip_mapping))

    search_rates = solr_obj.get_search_rates()
    avg_search_rates = {}

    scale_op = {}

    for collection in collections_to_monitor:
        collection_search_rates = search_rates.get(collection, {})

        logger.info('fetching search rates of collection: {}'.format(collection))
        shard_search_rates = avg_search_rates_for_shards(collection_search_rates)
        avg_search_rates[collection] = shard_search_rates

        logger.info("collection: {} avg search rates of shards: {}".format(collection, shard_search_rates))

        for shard, rates in shard_search_rates.items():

            replicas_in_vm = shard_ip_mapping.get(collection + "." + shard, [])
            n_replicas_in_vm = len(replicas_in_vm)

            n_required = 0

            if (rates.get('1minRate') > HIGH_SEARCH_RATE_THRESHOLD * 2) and (
                    rates.get('5minRate') > HIGH_SEARCH_RATE_THRESHOLD * 2):
                mult = rates.get('1minRate') // (HIGH_SEARCH_RATE_THRESHOLD * 2)

                n_required = min(mult, MAX_NEW_REPLICAS_IN_AS_VMS)  # - n_replicas_in_vm

            elif rates.get('1minRate') > HIGH_SEARCH_RATE_THRESHOLD and rates.get(
                    '5minRate') > HIGH_SEARCH_RATE_THRESHOLD * 0.75:
                n_required = 1

            elif rates.get('1minRate') <= LOW_SEARCH_RATE_THRESHOLD and rates.get(
                    '5minRate') <= LOW_SEARCH_RATE_THRESHOLD * 1.5:
                n_required = 0  # -n_replicas_in_vm
            logger.info("{}.{} requires {} total replicas".format(collection, shard, n_required))

            scale_op[collection + "." + shard] = n_required

    return scale_op, avg_search_rates


if __name__ == '__main__':
    start_time = datetime.today().strftime("%Y%m%d%H%M")
    as_log = {"start_time": start_time, "event_name": "solr_autoscaling"}

    try:

        logger = get_logger('auto_scale')
        logger.info(
            "vmss_resource_group: {}, vmss_name: {}, max machines: {}, solr domain: {}, solr base cluster nodes: {} - {}".format(
                vmss_resource_group,
                vmss_name,
                MAX_MACHINES,
                SOLR_DOMAIN,
                len(SOLR_BASE_NODES),
                SOLR_BASE_NODES))

        as_log['error'] = False
        as_log['skip_kibana'] = SKIP_KIBANA
        if not SKIP_KIBANA:
            try:
                cluster_resp_time, scale_res_clustering = kibana_resp_time_scaling(key=KIBANA_CLUSTERING_KEY)
                logger.info("the scale res for clustering: {}".format(scale_res_clustering))
                as_log['cluster_resp_time'] = cluster_resp_time

                sugg_resp_time, scale_res_suggestion = kibana_resp_time_scaling(key=KIBANA_SUGGESTION_KEY)
                logger.info("the scale res for suggestion: {}".format(scale_res_suggestion))
                as_log['sugg_resp_time'] = sugg_resp_time

                scale_in_flag = scale_res_clustering.get('scale_in') and scale_res_suggestion.get('scale_in')  # imp
                scale_out_flag = scale_res_clustering.get('scale_out') or scale_res_suggestion.get('scale_out')  # imp

            except Exception as e:
                as_log['skip_kibana'] = True
                as_log['kibana_error_details'] = str(e)
                scale_in_flag = True
                scale_out_flag = True

        else:

            scale_in_flag = True
            scale_out_flag = True

        as_log['scale_in'] = scale_in_flag
        as_log['scale_out'] = scale_out_flag

        if scale_in_flag or scale_out_flag:

            azure_inst = AzureVmss(vmss_resource_group=vmss_resource_group, vmss_name=vmss_name,
                                   max_machines=MAX_MACHINES)
            solr_inst = SolrUtil(solr_domain=SOLR_DOMAIN, solr_nodes=SOLR_BASE_NODES)

            scale_ops, avg_search_rates = find_hot_cold_shards(azure_obj=azure_inst, solr_obj=solr_inst)
            logger.info("the scale op : {}".format(scale_ops))
            as_log['avg_search_rates'] = avg_search_rates
            as_log['hot_cold_shards'] = scale_ops

            if scale_in_flag:
                logger.info("Beginning scale in operation")
                # delete_replica_dict, rebalance_dict, empty_nodes
                scale_in_res = scale_in(scale_ops, azure_obj=azure_inst, solr_obj=solr_inst, delete_empty_vms=False)
            else:
                logger.info("Not Scaling In, as kibana response time in limits, (above low response time threshold)")
                scale_in_res = {}
                # delete_replica_dict, rebalance_dict, empty_nodes = [], [], []

            as_log['deleted_replicas'] = scale_in_res.get('deleted_replicas', {})
            as_log['rebalance'] = scale_in_res.get("move_replica_dict", {})

            if scale_out_flag:
                time.sleep(20)
                logger.info("Beginning scale out operation")
                scale_out_res = scale_out(scale_ops, azure_obj=azure_inst, solr_obj=solr_inst)
            else:
                logger.info(
                    "Not Scaling Out, as either kibana response time in limits (below high response time threshold) or search rates in limits")
                scale_out_res = {}

            as_log['n_new_VMs_required'] = scale_out_res.get('n_new_VMs_required', 0)
            as_log['n_empty_nodes_already_alive'] = scale_out_res.get("n_empty_nodes_already_alive", 0)
            as_log['empty_VM_already_alive'] = scale_out_res.get("empty_VM_already_alive", [])
            as_log['new_VM_ips'] = scale_out_res.get('new_VM_ips', [])

            as_log['add_replicas'] = scale_out_res.get("added_replicas_to_vms", [])
        else:
            logger.info('Neither scaling in nor scaling out')

        clean_up_at_end = True

        if clean_up_at_end:
            time.sleep(WAIT_BEFORE_NODE_DELETION_SECS * 5)
            empty_nodes = find_empty_nodes(azure_obj=azure_inst, solr_obj=solr_inst)
            empty_nodes = [item for item in empty_nodes if item not in as_log.get('add_replicas', [])]
            logger.info("cleaning up empty nodes: {}".format(empty_nodes))
            remove_empty_nodes_solr(empty_nodes, solr_obj=solr_inst)
            if empty_nodes:
                time.sleep(WAIT_BEFORE_NODE_DELETION_SECS)

                # DELETE VMS
                azure_inst.delete_machines(empty_nodes)
                # for vm_ip in empty_nodes:
                #     {'deleted_VM': vm_ip}
            as_log['removed_vms'] = empty_nodes

        _, machines_ip_ids = azure_inst.get_current_machines()
        as_log['current_VM_count'] = len(machines_ip_ids)

        clog(data_dict=as_log)

    except Exception as e:
        logger.error(e, exc_info=True)
        as_log.update({'error': True, 'error_details': str(e)})
        clog(data_dict=as_log)

    logger.info("finished!!")


