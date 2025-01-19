import requests
from collections import defaultdict
import datetime
import time
from auto_scale_common import get_logger


class SolrUtil:
    """
    Class to interact with solr, requires solr_domain and base solr cluster nodes
    """
    SELECT_METRIC_API = "solr/admin/metrics?regex=QUERY\./select\.requestTimes.*"

    ADD_REPLICA_API = "solr/admin/collections?action=ADDREPLICA&collection={collection}&shard={shard}&node={node}&tlogReplicas={tlog}&pullReplicas={pull}&nrtReplicas={nrt}"
    DELETE_REPLICA_API = "solr/admin/collections?action=DELETEREPLICA&collection={collection}&shard={shard}&replica={replica}"
    MOVE_REPLICA_API = "solr/admin/collections?action=MOVEREPLICA&collection={collection}&shard={shard}&replica={replica}&sourceNode={source_node}&targetNode={target_node}"

    DELETE_NODE_API = "solr/admin/collections?action=DELETENODE&node={node}"

    CLUSTER_STATUS_API = "solr/admin/collections?action=CLUSTERSTATUS"

    REQUEST_STATUS_API = "solr/admin/collections?action=REQUESTSTATUS&requestid={async_id}"

    def __init__(self, solr_domain, solr_nodes):
        """
        :param solr_domain: (str) solr environment domain or ip with port which would be used to request using solr Rest APIs
        :param solr_nodes: (list) all the solr nodes (domains or ip) along with port in the base cluster
        """
        self.solr_domain = solr_domain
        self.solr_nodes = solr_nodes
        self.logger = get_logger('solr_details')

        self.logger.info('solr domain: {}, live solr nodes in base cluster: {}'.format(solr_nodes, solr_nodes))

    def get_search_rates(self):
        """
        :return: Based on the solr metric API returns the average search rates of each of the collection.shard replica combination.
        """
        data = defaultdict(dict)

        for node in self.solr_nodes:
            url = node + self.SELECT_METRIC_API
            self.logger.info('requesting metric api at: {}'.format(url))

            try:
                res = requests.get(url)
            except Exception as e:
                self.logger.exception('Error: {} for solr node: {}'.format(str(e), node), exc_info=True)
                continue

            for key, value in res.json().get('metrics').items():
                val = value.get("QUERY./select.requestTimes")
                if ".system" in key or "_designer" in key:
                    continue
                key_splits = key.split(".")

                print (key)
                collection, shard, replica = key_splits[2:]
                one_min_rate = val.get("1minRate")
                five_min_rate = val.get("5minRate")
                fifteen_min_rate = val.get("15minRate")

                increasing = False
                if one_min_rate >= five_min_rate:  # >= fifteen_min_rate:
                    increasing = True

                if not shard + ":" + replica in data[collection]:
                    data[collection][shard + "." + replica] = {
                        "1minRate": round(one_min_rate, 2),
                        "5minRate": round(five_min_rate, 2),
                        "15minRate": round(fifteen_min_rate, 2),
                        "increasing_usage": increasing,
                        "node":node
                    }
        self.logger.info("rates dict: {}".format(dict(data)))

        return dict(data)

    def _get_new_async_id(self):
        """
        :return: generates a new async id for asynchronous requests to solr, which is current time stamp to ensure its different every time
        """
        current_time = datetime.datetime.today()
        current_time_str = current_time.strftime("%Y/%m/%d-%H:%M:%S")

        async_id = current_time_str

        return async_id

    def poll_async_req(self, async_id, sleep_interval=10, stop_after_secs=300):
        """

        :param async_id: (str) request async id
        :param sleep_interval: (int) no. of secs to wait for each poll, default 10 secs
        :param stop_after_secs: (int) total no. of secs to run polling for, default 300 secs
        :return: returns the status of the request, completed, failed etc.
        """

        total_sleep_time = 0

        # print("collection:", collection)
        # req_id = date_str + "_" + collection + "_restore"
        status_url = self.solr_domain + self.REQUEST_STATUS_API.format(async_id=async_id)
        self.logger.info("polling for async request with aysnc id: {}, request url: {}".format(async_id, status_url))
        res = requests.get(status_url)
        res = res.json()
        status = res.get("status", {}).get('state')

        if status == 'running':
            while status and status != 'completed':
                time.sleep(sleep_interval)
                total_sleep_time += sleep_interval
                res = requests.get(status_url)
                res = res.json()
                status = res.get("status", {}).get('state')

                if status not in ["running", 'completed']:
                    self.logger.info('Failure state: {}'.format(res))
                    break

                if total_sleep_time >= stop_after_secs:
                    self.logger.info("its over {}, so breaking the loop".format(stop_after_secs))
                    break
        self.logger.info('async id: {}, status: {}'.format(async_id, status))
        return status  # completed

    def set_node_name(self, node, new_vm_port):
        """
        Solr identifies nodes with <node-ip>:<port>_solr, this method returns that for a given node and port
        :param node: (str) new node ip from vmss, which is to be used in subsequent requests
        :param new_vm_port: the port on which solr is running on the node
        :return: return the ip:port_solr which is the node value to be used in subsequent requests
        """
        if not node.endswith('_solr'):
            node = node + ":" + new_vm_port + "_solr"
        return node

    def get_solr_live_nodes(self):
        """
        finds out the no. of live nodes in solr cluster
        :return: returns a list of active nodes in current solr cluster
        """
        url = self.solr_domain + self.CLUSTER_STATUS_API
        self.logger.info('request url for cluster status: {}'.format(url))

        res = requests.get(url)

        res = res.json()
        return res.get('cluster').get("live_nodes")

    def add_replica(self, collection, shard, node, tlog=0, nrt=0, pull=1, async_req=False):
        """
        adds replica of a given collection shard on given node as difined type
        :param collection: str, collection name
        :param shard: str, shard name
        :param node: str, node name, as <ip>:<port>_solr
        :param tlog: int, no. of tlog replicas to be added, default 0
        :param nrt: int, no. of nrt replicas to be added, default 0
        :param pull: int, no. of pull replicas to be added, default 1
        :param async_req: bool, set for async request, default False
        :return: returns the response object if async_req is false, else returns async_id which can be used in polling for completion
        """

        url = self.solr_domain + self.ADD_REPLICA_API
        url = url.format(collection=collection, shard=shard, node=node, tlog=tlog, nrt=nrt, pull=pull)

        self.logger.info('request url for adding replica: {}'.format(url))

        if async_req:
            async_id = self._get_new_async_id()
            self.logger.info("making asyc request, with async id: {}".format(async_id))
            url = url + "&async=" + async_id
            res = requests.get(url)

            return async_id
        else:
            res = requests.get(url)
            self.logger.info('add replica response: {}'.format(res))
            return res

    def remove_replica(self, collection, shard, replica, async_req=False):
        """
        removes the given replica of given collection and shard
        :param collection: str, collection name
        :param shard: str, shard name
        :param replica: str, replica or core name to remove
        :param async_req: bool, set for async request, default False
        :return: returns the response object if async_req is false, else returns async_id which can be used in polling for completion
        """

        url = self.solr_domain + self.DELETE_REPLICA_API
        url = url.format(collection=collection, shard=shard, replica=replica)

        self.logger.info('request url for removing replica: {}'.format(url))

        if async_req:
            async_id = self._get_new_async_id()
            self.logger.info("making asyc request, with async id: {}".format(async_id))
            url = url + "&async=" + async_id
            res = requests.get(url)

            return async_id
        else:
            res = requests.get(url)
            self.logger.info('remove replica response: {}'.format(res))

            return res

    def move_replica(self, collection, shard, replica, source_node, target_node, async_req=False):
        """
        move the given replica of given collection and shard to given target node from given source node.
         :param collection: str, collection name
        :param shard: str, shard name
        :param replica: str, replica or core name to move
        :param source_node: the source node to move the replica from, as <ip>:<port>_solr
        :param target_node: the target node to move the replica to, as <ip>:<port>_solr
        :param async_req: bool, set for async request, default False
        :return: returns the response object if async_req is false, else returns async_id which can be used in polling for completion
        """

        url = self.solr_domain + self.MOVE_REPLICA_API
        url = url.format(collection=collection, shard=shard, replica=replica, source_node=source_node,
                         target_node=target_node)

        self.logger.info('request url for moving replica: {}'.format(url))

        if async_req:
            async_id = self._get_new_async_id()
            self.logger.info("making asyc request, with async id: {}".format(async_id))
            url = url + "&async=" + async_id
            res = requests.get(url)

            return async_id
        else:
            res = requests.get(url)
            self.logger.info('move replica response: {}'.format(res))

            return res

    def delete_node(self, node, async_req=False):
        """
        given a node as <ip>:<port>_solr, delete all replicas from it, so as it can be removed from cluster, only removes replicas, removing node has to be done separately
        :param node: str, node name, as <ip>:<port>_solr
        :param async_req:
        :param async_req: bool, set for async request, default False
        :return: returns the response object if async_req is false, else returns async_id which can be used in polling for completion
        """

        url = self.solr_domain + self.DELETE_NODE_API
        url = url.format(node=node)

        self.logger.info('request url for deleting node: {}'.format(url))

        if async_req:
            async_id = self._get_new_async_id()
            self.logger.info("making asyc request, with async id: {}".format(async_id))
            url = url + "&async=" + async_id
            res = requests.get(url)

            return async_id
        else:
            res = requests.get(url)
            self.logger.info('delete node response: {}'.format(res))

            return res

    def get_vm_solr_status(self, vm_ips, collections_to_monitor):
        """
        given the VM ips list and collections lists calculates the for each VM (ip) what shards it has and also what shard is present on what ips
        :param vm_ips: list of ips from
        :param collections_to_monitor: list of collections to process on
        :return: returns dict, dict, where the first dict contains the vm ip as key and list of dict as values contains collection, shard, core details on the vm,
        and second returning dict contains the shard to ip mapping where key is collection.shard and value is a list of ips the collection.shard is present in.
        """
        url = self.solr_domain + self.CLUSTER_STATUS_API
        self.logger.info('request url for getting cluster status: {}'.format(url))
        res = requests.get(url)
        res = res.json().get('cluster', {})
        collections = res.get("collections", {})

        vm_cores = defaultdict(list)
        shard_ip_mapping = defaultdict(list)
        # vm_to_col_shard_core = defaultdict(list)

        for col, details in collections.items():
            if col not in collections_to_monitor:
                continue

            shards = details.get('shards', {})
            for shard_name, shard_details in shards.items():
                if shard_details.get('state', "") != 'active':
                    self.logger.warning('shard {} not active'.format(shard_name))
                    continue
                for core_name, core_details in shard_details.get('replicas').items():
                    if core_details.get("state", "") == 'active' and core_details.get('type') == 'PULL' \
                            and core_details.get("node_name").split(":")[0] in vm_ips:
                        replica_name = core_details.get('core').split(shard_name)[-1].strip("_")
                        vm_cores[core_details.get("node_name").split(":")[0]].append(
                            {'core': core_name, 'shard': shard_name, 'replica': replica_name, 'collection': col})

                        shard_ip_mapping[col + "." + shard_name].append(core_details.get("node_name").split(":")[0])

        return dict(vm_cores), shard_ip_mapping

    def is_shard_in_vm(self, collection, shard, shard_ip_mapping):
        """
        finds whether a given collection and its given shard is present in any of the new vms or now.
        :param collection: str, collection name
        :param shard: str, shard name
        :param shard_ip_mapping: dict, shard_ip_mapping containing collection.shard as key and list of ips which contain the shard replica as values
        :return: bool, returns whether the shard (collection.shard) is present in any of the vms, this is calculated using shard_ip_mapping returned by get_vm_solr_status
        """
        flag = False
        if shard_ip_mapping:
            flag = True if shard_ip_mapping.get(collection + "." + shard, []) else False

        return flag


