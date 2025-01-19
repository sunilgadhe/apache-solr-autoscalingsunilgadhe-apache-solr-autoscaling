# get current no. of machines
# az vmss nic list --resource-group Production --vmss-name prod-platforms-eu2-solr-autoscaling
# scale up
# az vmss scale --new-capacity 1 --resource-group Production --name prod-platforms-eu2-solr-autoscaling
# scale down
# az vmss delete-instances --resource-group Production --name prod-platforms-eu2-solr-autoscaling --instance-ids <space separated vm ids to delete>

import json
import subprocess
import time
from auto_scale_common import get_logger


class AzureVmss:
    """
    Class to interact with the azure VMSS using az cli. requires subprocess python module to run the commands. Must have az client (access) setup on the running machine.
    """

    SLEEP_AFTER_VM_CREATION = 30

    def __init__(self, vmss_resource_group, vmss_name, max_machines=5):
        """
        :param vmss_resource_group: str, vmss resource group name
        :param vmss_name: str, vmss name
        :param max_machines: int, max no. of machines allowed to be used in VMSS group. default 5
        """
        self.max_machines = max_machines
        self.vmss_resource_group = vmss_resource_group
        self.vmss_name = vmss_name

        self.logger = get_logger('azure_vmss')

        self.logger.info("vmss resource group: {}, vmss name: {} and max machines: {}".format(vmss_resource_group, vmss_name, max_machines))

    def _run_az_cli(self, az_cli_cmd):
        """
        Private Method used by other methods of the class to execute the az commands.
        :param az_cli_cmd: str, the command to run
        :return: dict, returns the result of the command as dict
        """
        self.logger.info("executing cmd: {}".format(az_cli_cmd))
        cmd_run = subprocess.run(az_cli_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        cmd_run_stdout = cmd_run.stdout.decode("utf-8")
        cmd_run_stderr = cmd_run.stderr.decode("utf-8")
        try:
            cmd_run_details = json.loads(cmd_run_stdout)
            # self.logger.info(cmd_run_details)
        except Exception as e:
            cmd_run_details = cmd_run_stdout
            self.logger.error(str(e))
            self.logger.error(cmd_run_details)

        return cmd_run_details

    def get_current_machines(self):
        """
        finds out the current fleet details of the vmss
        :return: dict,dict response dict of the command, dict with key as machine ip and value as vmss machien id which is required when deleting the machine.
        """
        response = self._run_az_cli(
            "az vmss nic list --resource-group " + self.vmss_resource_group + " --vmss-name " + self.vmss_name)

        machines_ip_ids = {}

       
        for vm in response:
           
            ip_address = vm['ipConfigurations'][0]['privateIPAddress']
            instance_id = vm['virtualMachine']['id'].split('/')[-1]

            machines_ip_ids[ip_address] = instance_id
        # self.logger.info("machine ip ids: {}".format(machines_ip_ids))
        return response, machines_ip_ids

    def create_machines(self, vm_to_add=1):
        """
        Method to create new machines in the current VMSS group
        :param vm_to_add: no of machines to have, this will be total no. of machines (currently running + new required)
        :return: dict, response of the command
        """
        # vm_to_add is total number of new machines to be created
        machines_ip_ids = self.get_current_machines()[1]
        if len(machines_ip_ids) <= self.max_machines:
            new_capacity = len(machines_ip_ids) + vm_to_add
            response = self._run_az_cli("az vmss scale --new-capacity " + str(
                new_capacity) + " --resource-group " + self.vmss_resource_group + " --name " + self.vmss_name)
            self.logger.info("Machines added, current status: {}".format(self.get_current_machines()))
            time.sleep(self.SLEEP_AFTER_VM_CREATION)
        else:
            self.logger.info("Cant Add machines, Running at max capacity, current status: {}".format(self.get_current_machines()[-1]))

        return response

    def delete_machines(self, vm_to_delete):
        """
        Method used to delete the machines whose ids (not ips) are passed in list
        :param vm_to_delete: list, list of ids of machines to delete from VMSS group
        :return: dict, response of the command
        """
        # vm_to_delete is total number of new machines to be deleted
        self.logger.info("vms to delete: {}".format(vm_to_delete))
        cmd = "az vmss delete-instances --resource-group " + self.vmss_resource_group + " --name " + self.vmss_name + " --instance-ids"
        _, machine_ip_ids = self.get_current_machines()
        instance_ids = [str(id) for ip, id in machine_ip_ids.items() if ip in vm_to_delete]
        cmd = cmd + " " + " ".join(instance_ids)
        self.logger.info("Deleting machines: {} with cmd".format(vm_to_delete, cmd))
        response = self._run_az_cli(cmd)
        self.logger.info("Machines deleted and current machines: {} ".format(self.get_current_machines()[-1]))

        return response
