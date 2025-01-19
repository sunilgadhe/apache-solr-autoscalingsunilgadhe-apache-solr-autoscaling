import time

from collections import defaultdict

from autoscale_conf import collections_to_monitor, collection_weights
from autoscale_conf import NEW_VM_SOLR_PORT
from autoscale_conf import MAX_MACHINES, MAX_REPLICAS_PER_NODE, MIN_REPLICAS_TO_SCALE, MAX_COLLECTION_REPLICAS_PER_NODE
from autoscale_conf import WAIT_AFTER_SOLR_REQUEST_SECS, WAIT_AFTER_NODE_CREATION_SECS, WAIT_BEFORE_NODE_DELETION_SECS

from auto_scale_common import get_logger
from auto_scale_common import clog

logger = get_logger('scaling_details')


def get_replicas_to_delete(scale_op, azure_obj, solr_obj):
    """
    calculate the replicas along with the collection, shard and core-name to delete based on load details in scale op dict
    :param scale_op: dict, the dict with key col.shard and value as no. of required replicas in total
    :param azure_obj: AzureVmss object, to interact with azure vmss
    :param solr_obj: SolrUtil object, to interact with solr
    :return: dict, with key as ip and value as the list of dict having core, shard and collection details which can be removed.
    """
    _, machines_ip_ids = azure_obj.get_current_machines()
    vm_shards, shard_ip_mapping = solr_obj.get_vm_solr_status(list(machines_ip_ids.keys()), collections_to_monitor)

    ip_shard_mapping = defaultdict(list)

    logger.info("shard_ip_mapping: {}".format(shard_ip_mapping))

    shards_to_delete = {col_shard: n_replica for col_shard, n_replica in scale_op.items() if
                        n_replica < len(shard_ip_mapping.get(col_shard, []))}
    logger.info("shards_to_delete: {}".format(shards_to_delete))

    to_delete = defaultdict(list)

    for col_shard, ips in shard_ip_mapping.items():
        col, shard = col_shard.split(".")
        for ip in ips:
            ip_shard_mapping[ip].append(col_shard)

    for col_shard, n_replicas in shards_to_delete.items():
        n_replicas = int(n_replicas)
        n_remove = 0
        if n_replicas < len(shard_ip_mapping.get(col_shard, [])):
            n_remove = len(shard_ip_mapping.get(col_shard, [])) - n_replicas

        logger.info("to be deleted :- col_shard: {} and no. of shards to remove: {} ".format(col_shard, n_remove))

        col, shard = col_shard.split(".")
        current_ips = shard_ip_mapping.get(col_shard, [])

        ips_to_remove_from = [current_ips.pop() for _ in range(n_remove)]

        logger.info("current_ips: {} and ips_to_remove_from: {}".format(current_ips, ips_to_remove_from))

        for ip, cores in vm_shards.items():
            if ip not in ips_to_remove_from:
                continue
            for core in cores:
                if core.get('collection') == col and core.get('shard') == shard:
                    to_delete[ip].append({'core': core.get('core'), 'shard': shard, 'collection': col})

    return to_delete


def compute_rebalance(azure_obj, solr_obj):
    """
    traverse over nodes to see if removal has created space on vm where other replicas already present in other vms in vmss group can be moved to empty some nodes.
    only computes which replicas can be moved from what node to what. while traversing and before deciding what to move,
    it considers the weights of collections (which can define a collection as twice or more heavy as others) along with the thresholds like MAX_COLLECTION_REPLICAS_PER_NODE, MAX_REPLICAS_PER_NODE

    :param azure_obj: AzureVmss object, to interact with azure vmss
    :param solr_obj: SolrUtil object, to interact with solr
    :return: dict, dict with key as col.shard and value as the list of list containing core-name, source-ip, target-ip
    """
    _, machines_ip_ids = azure_obj.get_current_machines()
    vm_shards, shard_ip_mapping = solr_obj.get_vm_solr_status(list(machines_ip_ids.keys()), collections_to_monitor)
    ip_shard_mapping = defaultdict(list)
    ip_col_mapping = defaultdict(list)
    col_shard_to_core = {}

    for col_shard, ips in shard_ip_mapping.items():
        col, shard = col_shard.split(".")
        for ip in ips:
            # print("======================", col_shard, ">>>",[col_shard] * collection_weights.get(col, 1))
            ip_shard_mapping[ip].extend([col_shard] * collection_weights.get(col, 1))
            ip_col_mapping[ip].extend([col] * collection_weights.get(col, 1))

            for dict_item in vm_shards.get(ip, []):
                if dict_item.get('collection') == col and dict_item.get('shard') == shard:
                    col_shard_to_core[col_shard] = dict_item.get('core')

    # ip_capacity = {k: MAX_COLLECTION_REPLICAS_PER_NODE - len(v) for k, v in ip_shard_mapping.items()}

    ip_list = list(machines_ip_ids.keys())

    ip_list_reverse = ip_list.copy()
    ip_list_reverse.reverse()

    col_shards = list(shard_ip_mapping.keys())
    move_replica_dict = defaultdict(list)
    logger.info("ip_shard_mapping: {}".format(ip_shard_mapping))
    for i, ip_source in enumerate(ip_list_reverse):
        # print("ip", ip, ">>>>>>", ip_shard_mapping.get(ip, []))
        col_shards_in_ip_source = []
        for col_shard in ip_shard_mapping.get(ip_source, []):
            if col_shard not in col_shards_in_ip_source:
                col_shards_in_ip_source.append(col_shard)

        for col_shard in col_shards_in_ip_source:
            col = col_shard.split(".")[0]

            for ip_target in ip_list:
                if ip_source == ip_target:
                    break  # do not move forward as that reverses thing and causes unnecessary moves
                if len(ip_shard_mapping.get(ip_target,
                                            [])) >= MAX_REPLICAS_PER_NODE or col_shard in ip_shard_mapping.get(
                        ip_target, []) or \
                        ip_col_mapping.get(ip_target, []).count(col) >= MAX_COLLECTION_REPLICAS_PER_NODE:
                    logger.info('col_shard {} on {} CAN NOT be moved to ip {}'.format(col_shard, ip_source, ip_target))
                    continue
                logger.info('col_shard {} on {} CAN be moved to ip {}'.format(col_shard, ip_source, ip_target))

                if col not in ip_col_mapping.get(ip_target, []) or (
                        col in ip_col_mapping.get(ip_target, []) and ip_col_mapping.get(ip_target, []).count(
                        col) < MAX_COLLECTION_REPLICAS_PER_NODE):
                    logger.info("col_shard {} on {} will be moved to ip {}".format(col_shard, ip_source, ip_target))
                    move_replica_dict[col_shard].append([col_shard_to_core.get(col_shard), ip_source, ip_target])
                    ip_shard_mapping[ip_target].extend([col_shard] * collection_weights.get(col, 1))
                    ip_col_mapping[ip_target].extend([col] * collection_weights.get(col, 1))

                    for _ in range(len([col_shard] * collection_weights.get(col, 1))):
                        ip_shard_mapping[ip_source].remove(col_shard)
                        ip_col_mapping[ip_source].remove(col)

                    break
                logger.info('col_shard {} on {} will Not be moved to ip {}'.format(col_shard, ip_source, ip_target))

    return dict(move_replica_dict)


def execute_rebalance(move_replica_dict, solr_obj):
    """
    based on the move_replica_dict (from compute_rebalance) executes the moving of replicas across machines.
    :param move_replica_dict: dict, with key as col.shard and value as list of list containing core-name, source-ip, target-ip
    :param solr_obj: SolrUtil object, to interact with solr
    :return: None
    """
    for col_shard, core_source_target_list in move_replica_dict.items():
        col, shard = col_shard.split(".")

        for core_source_target in core_source_target_list:

            core, source, target = core_source_target
            source = solr_obj.set_node_name(source, NEW_VM_SOLR_PORT)
            target = solr_obj.set_node_name(target, NEW_VM_SOLR_PORT)
            if not core:
                continue
            async_id = solr_obj.move_replica(col, shard, core, source, target, async_req=True)
            logger.info(
                'move replica {} from {} to {} :- async_id: {}, polling for completion now'.format(
                    col + ":" + shard + ":" + core, source, target,
                    async_id))
            status = solr_obj.poll_async_req(async_id=async_id, sleep_interval=5, stop_after_secs=150)
            logger.info(
                'move replica {} from {} to {} :- status: {} after polling'.format(col + ":" + shard + ":" + core,
                                                                                   source, target, status))
            time.sleep(WAIT_AFTER_SOLR_REQUEST_SECS * 2)


def find_empty_nodes(azure_obj, solr_obj):
    """
    function to find the empty nodes if any in the VMSS group, empty nodes mean nodes with no active replica (this can result from rebalancing)
    :param azure_obj: AzureVmss object, to interact with azure vmss
    :param solr_obj: SolrUtil object, to interact with solr
    :return: list of ips which have no active replicas
    """
    _, machines_ip_ids = azure_obj.get_current_machines()
    vm_shards, shard_ip_mapping = solr_obj.get_vm_solr_status(list(machines_ip_ids.keys()), collections_to_monitor)
    logger.info(f'vm_shards: {vm_shards}, shard_ip_mapping: {shard_ip_mapping}, machines_ip_ids: {machines_ip_ids}')
    ip_shard_mapping = defaultdict(list)
    
    empty_nodes = []

    for col_shard, ips in shard_ip_mapping.items():
        for ip in ips:
            ip_shard_mapping[ip].append(col_shard)
    logger.info("ip_shard_mapping : {}".format(ip_shard_mapping))
    for ip in list(machines_ip_ids.keys()):
        if len(ip_shard_mapping.get(ip, [])) == 0:
            empty_nodes.append(ip)
    logger.info("empty_nodes :{}".format(empty_nodes))
    return empty_nodes


def remove_empty_nodes_solr(empty_nodes, solr_obj):
    """
    function to remove all the data (expectedly inactive replicas) if any from the empty nodes, so as to prepare them for deletion or re use.
    :param empty_nodes: list of node to remove
    :param solr_obj: SolrUtil object, to interact with solr
    :return: None
    """
    for node in empty_nodes:
        solr_obj.delete_node(solr_obj.set_node_name(node, NEW_VM_SOLR_PORT))
        logger.info(
            'empty node {} removed from solr, and waiting for {} secs'.format(node, WAIT_AFTER_SOLR_REQUEST_SECS))
        time.sleep(WAIT_AFTER_SOLR_REQUEST_SECS)


def machines_distribution(scale_op, azure_obj, solr_obj):
    """
    function to calculate or distribute the new replicas (to be created) on new machines (to be created). calculates the scale out distribution based on scale op.
    it considers the weights of collections (which can define a collection as twice or more heavy as others) along with the thresholds like MAX_MACHINES, MAX_COLLECTION_REPLICAS_PER_NODE, MAX_REPLICAS_PER_NODE
    :param scale_op: dict, the dict with key col.shard and value as no. of required replicas in total
    :param azure_obj: AzureVmss object, to interact with azure vmss
    :param solr_obj: SolrUtil object, to interact with solr
    :return: dict, dict defining what shard should be placed on what machine no.
    """
    _, machines_ip_ids = azure_obj.get_current_machines()
    vm_shards, shard_ip_mapping = solr_obj.get_vm_solr_status(list(machines_ip_ids.keys()), collections_to_monitor)

    shards_to_add = {col_shard: n_replicas for col_shard, n_replicas in scale_op.items() if
                     n_replicas > len(shard_ip_mapping.get(col_shard, []))}
    logger.info('shards to add: {}'.format(shards_to_add))

    if not shards_to_add:
        return {}

    empty_nodes = find_empty_nodes(azure_obj, solr_obj)
    ip_shard_mapping = defaultdict(list)
    machine_shards = defaultdict(list)

    for col_shard, ips in shard_ip_mapping.items():
        col, shard = col_shard.split(".")
        for ip in ips:
            ip_shard_mapping[ip].append(col_shard)

    if len(shards_to_add) < MIN_REPLICAS_TO_SCALE and len(shard_ip_mapping) == 0:
        logger.info('only {} shards to be scaled which is less than threshold {}, so skipping scaling out'.format(
            len(shards_to_add),
            MIN_REPLICAS_TO_SCALE))
        return {}

    machines = MAX_MACHINES - len(machines_ip_ids)
    machines = machines + len(empty_nodes)
    machines = [id for id in range(machines)]
    all_machines = list(ip_shard_mapping.keys())
    all_machines.extend(machines)
    logger.info('machines capacity possible, including empty and alive: {}'.format(machines))

    replica_counts = [int(val) for val in shards_to_add.values()]
    max_replicas = max(replica_counts) if replica_counts else 0
    logger.info("Max replicas: {}".format(max_replicas))
    for _ in range(max_replicas):
        for col_shard, replica_count in shards_to_add.items():
            replica_count = int(replica_count)
            if replica_count <= 0:
                continue

            col, shard = col_shard.split(".")
            for machine_id in all_machines:
                current_shards = ip_shard_mapping.get(machine_id, [])
                logger.info('machine id: {}, current shards: {}'.format(machine_id, current_shards))
                current_shards_with_weights = []
                for cs in current_shards:
                    current_shards_col = cs.split('.')[0]
                    current_shards_with_weights.extend([cs] * collection_weights.get(current_shards_col, 1))

                collections_present = [item.split(".")[0] for item in current_shards_with_weights]

                logger.info(
                    "col_shard:{} in current_shards:{} :: {}, len(current_shards):{} > MAX_REPLICAS_PER_NODE:{} or collections_present.count(col):{} >= MAX_COLLECTION_REPLICAS_PER_NODE:{}".format(
                        col_shard, current_shards,
                        col_shard in current_shards,
                        len(current_shards),
                        MAX_REPLICAS_PER_NODE,
                        collections_present.count(col),
                        MAX_COLLECTION_REPLICAS_PER_NODE))
                if col_shard in current_shards or len(current_shards) > MAX_REPLICAS_PER_NODE or \
                        collections_present.count(col) >= MAX_COLLECTION_REPLICAS_PER_NODE:
                    logger.warning('skipping {} for machine_id {}'.format(col_shard, machine_id))
                    continue
                machine_shards[machine_id].append(col_shard)
                ip_shard_mapping[machine_id].append(col_shard)
                logger.info("mark add {} to machine {}".format(col_shard, machine_id))
                break

            replica_count = replica_count - 1
            shards_to_add[col_shard] = replica_count

    return machine_shards


def scale_in(scale_op, azure_obj, solr_obj, delete_empty_vms=False):
    """
    The main scale in function, first based on scale in values in scale_op, calculated the replicas to delete then removes the replica to empty the machines,
     then computes and executes the rebalancing and finally cleans up the empty nodes so we can delete those.
    :param scale_op: dict, the dict with key col.shard and value as no. of required replicas in total
    :param azure_obj: AzureVmss object, to interact with azure vmss
    :param solr_obj: SolrUtil object, to interact with solr
    :param delete_empty_vms: bool, if True then delete the empty vms as well, default False, when False it will let the scale out see if any nodes are empty and use them if requried,
    :return: None
    """
    logger.info("scale_in")

    to_delete_ip_cores = get_replicas_to_delete(scale_op.copy(), azure_obj=azure_obj, solr_obj=solr_obj)
    logger.info("to_delete_ip_cores: {}".format(to_delete_ip_cores))

    _, machines_ip_ids = azure_obj.get_current_machines()
    logger.info("machines_ip_ids: {}".format(machines_ip_ids))
    # REMOVE CORES
    for node, details in to_delete_ip_cores.items():
        for core_dict in details:
            col = core_dict.get('collection')
            shard = core_dict.get('shard')
            core = core_dict.get('core')

            try:
                solr_obj.remove_replica(col, shard, core)
                time.sleep(WAIT_AFTER_SOLR_REQUEST_SECS)
            except Exception as e:
                logger.error(str(e))

    # REBALANCE
    move_replica_dict = compute_rebalance(azure_obj=azure_obj, solr_obj=solr_obj)
    logger.info('compute rebalance: {}'.format(move_replica_dict))
    execute_rebalance(move_replica_dict, solr_obj=solr_obj)
    if move_replica_dict:
        time.sleep(WAIT_AFTER_SOLR_REQUEST_SECS)
    logger.info('completed executing rebalance')

    to_log = {"deleted_replicas": dict(to_delete_ip_cores), "move_replica_dict": dict(move_replica_dict)}
    # REMOVE DATA FROM EMPTY NODES
    empty_nodes = find_empty_nodes(azure_obj=azure_obj, solr_obj=solr_obj)
    remove_empty_nodes_solr(empty_nodes, solr_obj=solr_obj)
    to_log['empty_nodes'] = empty_nodes

    if empty_nodes and delete_empty_vms:
        time.sleep(WAIT_BEFORE_NODE_DELETION_SECS)

        # DELETE VMS
        azure_obj.delete_machines(empty_nodes)

        # for vm_ip in empty_nodes:
        #     clog({'deleted_VM': vm_ip})
        to_log['removed_vms'] = empty_nodes
    return to_log


def scale_out(scale_op, azure_obj, solr_obj):
    """
    The main scale out function, based on scale out values in scale_op dict, first ensures what nodes are live from vmss group,
    then calculates the replicas to be added considering whats already present, then calculates how many more machines are required in addition how many empty machines are already there
    finally loops over the count and creates machines one by one while adding the calculated replicas on each started machines/vms in vmss.
    :param scale_op: dict, the dict with key col.shard and value as no. of required replicas in total
    :param azure_obj: AzureVmss object, to interact with azure vmss
    :param solr_obj: SolrUtil object, to interact with solr
    :return: None
    """

    logger.info("scale_out")

    live_nodes = solr_obj.get_solr_live_nodes()
    empty_nodes = find_empty_nodes(azure_obj=azure_obj, solr_obj=solr_obj)

    to_log = {}

    logger.info("check the empty node live in cluster")
    for ip in empty_nodes:
        node = solr_obj.set_node_name(ip, NEW_VM_SOLR_PORT)
        if node not in live_nodes:
            logger.debug('node missing: {}'.format(node))
        else:
            logger.info('Node found live: {}'.format(node))

    machine_shards = machines_distribution(scale_op.copy(), azure_obj=azure_obj, solr_obj=solr_obj)
    logger.info("shard distribution computed : {}".format(machine_shards))

    _, machines_ip_ids = azure_obj.get_current_machines()

    # if len(machine_shards) == 0:
    #     logger.info("no scaling required as distribution computed is {}".format(machine_shards))
    #     return

    new_machines_required = len(machine_shards) - len(empty_nodes) if len(machine_shards) else 0

    if new_machines_required <= 0:
        logger.info('empty nodes are enough')

    else:
        logger.info(
            'current empty nodes are not enough, no. of new machines required: {}'.format(new_machines_required))

    logger.info('current scaling required machines: {}'.format(len(machine_shards)))
    logger.info('empty nodes: {} : {}'.format(len(empty_nodes), empty_nodes))
    logger.info('new machines requried: {}'.format(new_machines_required))
    logger.info('Existing machines: {}'.format(len(machines_ip_ids)))

    logger.info('empty_node: {}'.format(empty_nodes))
    to_log.update({"n_new_VMs_required": new_machines_required,
                   "n_empty_nodes_already_alive": len(empty_nodes),
                   "empty_VM_already_alive": [],
                   "new_VM_ips": [],
                   "existing_node_used": []
                   })
    adding_replicas_to_vms = defaultdict(list)

    for i, (ind, col_shards) in zip(range(len(machine_shards)), machine_shards.items()):
        node_ip = None
        # empty_nodes = find_empty_nodes(azure_obj=azure_obj, solr_obj=solr_obj)
        logger.info('taking iteration/machine {} and col_shards: {}'.format(i, col_shards))
        logger.info('new_machines_required: {}, n_empty_nodes: {}'.format(new_machines_required, len(empty_nodes)))
        if i > new_machines_required + len(empty_nodes):
            logger.info("i > new_machines_required + len(empty_nodes) - True, i.e total capacity reached, breaking")
            break

        if isinstance(ind, str) and ind.count('.') > 1:  # existing IP
            node_ip = ind
            logger.info('Utilizing Existing node: {}'.format(node_ip))
            to_log["existing_node_used"].append(node_ip)
        elif empty_nodes:
            node_ip = empty_nodes.pop(0)
            logger.info('utilizing empty node: {}'.format(node_ip))
            to_log["empty_VM_already_alive"].append(node_ip)
        else:
            logger.info('no existing node can be used and empty nodes also exhausted! create new one')
            res = azure_obj.create_machines()
            logger.info("new node created!")
            time.sleep(WAIT_AFTER_NODE_CREATION_SECS)
            new_empty_nodes = find_empty_nodes(azure_obj=azure_obj, solr_obj=solr_obj)
            new_empty_nodes = [node_ip for node_ip in new_empty_nodes if node_ip not in adding_replicas_to_vms]

            # empty_nodes.extend(new_empty_nodes)
            logger.info('new list of empty_nodes : {}'.format(new_empty_nodes))
            node_ip = new_empty_nodes.pop()
            logger.info('using newly created node: {}'.format(node_ip))
            to_log['new_VM_ips'].append(node_ip)

        for col_shard in col_shards:
            col, shard = col_shard.split(".")
            node = solr_obj.set_node_name(node_ip, NEW_VM_SOLR_PORT)

            logger.info("adding replica for collection {} shard {} on node {}".format(col, shard, node))

            solr_obj.add_replica(col, shard, node)
            time.sleep(WAIT_AFTER_SOLR_REQUEST_SECS)

            adding_replicas_to_vms[node_ip].append(col + "." + shard)

    to_log['added_replicas_to_vms'] = dict(adding_replicas_to_vms)
    return to_log
