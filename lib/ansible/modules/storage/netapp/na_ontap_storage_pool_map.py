#!/usr/bin/python

# (c) 2018, NetApp, Inc
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type


ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}


DOCUMENTATION = '''
module: na_ontap_storage_pool_map
short_description: NetApp ONTAP manage mapping between aggregates and storage pool to allocate unit(s).
extends_documentation_fragment:
    - netapp.na_ontap
version_added: '2.8'
author: Storage Engineering (@Albinpopote) <ansible@black-perl.fr>

description:
- Create and manage mapping between aggregates and storage pool on ONTAP.

options:
  state:
    description:
    - Whether the specified aggregate should exist or not.
    choices: ['present']
    default: present

  aggregate_name:
    required: true
    description:
    - The name of the aggregate in the mapping.

  storage_pool:
    description:
    - Name of the storage pool in the mapping.

  allocation_units:
    description:
    - Specifies the number of allocation units
    choices: [1, 2, 3, 4]
    type: int

  timeout:
    description:
    - Time in seconds to wait the reassign allocation between nodes if needed
    - default is 120 seconds
    default: 120
'''

EXAMPLES = """
- name: Map aggregate to storage pool
  na_ontap_aggregate:
    state: present
    aggregate_name: aggr1
    storage_pool: FlashPool
    allocation_units: 2
    hostname: "{{ netapp_hostname }}"
    username: "{{ netapp_username }}"
    password: "{{ netapp_password }}"
"""

RETURN = """
"""

import traceback
import operator
import time

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils._text import to_native
import ansible.module_utils.netapp as netapp_utils
from ansible.module_utils.netapp_module import NetAppModule

HAS_NETAPP_LIB = netapp_utils.has_netapp_lib()

import json


class NetAppOntapStoragePoolMap(object):
    ''' object initialize and class methods '''

    def __init__(self):
        self.argument_spec = netapp_utils.na_ontap_host_argument_spec()
        self.argument_spec.update(dict(
            state=dict(required=False, choices=['present'], default='present'),
            aggregate_name=dict(required=True, type='str'),
            storage_pool=dict(required=True, type='str'),
            allocation_units=dict(required=True, type='int', choices=[1, 2, 3, 4]),
            timeout=dict(required=False, type='int', default=120)
        ))

        self.current = None
        self.hybrid_enabled = False

        self.module = AnsibleModule(
            argument_spec=self.argument_spec,
            supports_check_mode=True
        )

        self.na_helper = NetAppModule()
        self.parameters = self.na_helper.set_parameters(self.module.params)

        if HAS_NETAPP_LIB is False:
            self.module.fail_json(msg="the python NetApp-Lib module is required")
        else:
            self.server = netapp_utils.setup_na_ontap_zapi(module=self.module)

    def get_aggregate(self):
        """
        Return aggr-get-iter query results
        :return: Dictionnary of agregate details if found, None otherwise
        """
        aggr_get_iter = netapp_utils.zapi.NaElement('aggr-get-iter')
        query_details = netapp_utils.zapi.NaElement.create_node_with_children(
            'aggr-attributes', **{'aggregate-name': self.parameters.get('aggregate_name')})
        query = netapp_utils.zapi.NaElement('query')
        query.add_child_elem(query_details)
        aggr_get_iter.add_child_elem(query)

        try:
            result = self.server.invoke_successfully(aggr_get_iter, enable_tunneling=False)
        except netapp_utils.zapi.NaApiError as error:
            # Error 13040 denotes an aggregate not being found.
            if to_native(error.code) == "13040":
                return None
            else:
                self.module.fail_json(msg='Error getting details from aggregate %s: %s' %
                                      (self.parameters.get('aggregate_name'), to_native(error)),
                                      exception=traceback.format_exc())

        return_value = None
        if result.get_child_by_name('num-records') and \
                int(result.get_child_content('num-records')) == 1:
            return_value = {}
            attributes = result.get_child_by_name('attributes-list').get_child_by_name('aggr-attributes')
            return_value['node'] = attributes.get_child_by_name('aggr-ownership-attributes').get_child_content('owner-name')
            if attributes.get_child_by_name('aggr-raid-attributes').get_child_content('is-hybrid-enabled') == 'true':
                self.hybrid_enabled = True

        return return_value

    def get_available_capacity(self):
        """
        Get available allocation unit(s) on storage pool
        :return: Dictionnary with total and per node(s) allocation unit(s) availabled
        """
        capacity_get_iter = netapp_utils.zapi.NaElement('storage-pool-available-capacity-get-iter')
        query_details = netapp_utils.zapi.NaElement.create_node_with_children(
            'storage-pool-available-capacity-info', **{'storage-pool': self.parameters.get('storage_pool')})
        query = netapp_utils.zapi.NaElement('query')
        query.add_child_elem(query_details)
        capacity_get_iter.add_child_elem(query)

        try:
            result = self.server.invoke_successfully(capacity_get_iter, enable_tunneling=True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error getting available capacity from storage pool %s: %s' %
                                  (self.parameters.get('storage_pool'), to_native(error)),
                                  exception=traceback.format_exc())

        return_value = {'total': 0, 'nodes': {}}
        if result.get_child_by_name('num-records') and \
                int(result.get_child_content('num-records')) > 0:
            attributes = result.get_child_by_name('attributes-list').get_children()
            for elem in attributes:
                allocation_units = int(elem.get_child_content('allocation-unit-count'))
                node = elem.get_child_content('node')
                return_value['total'] += allocation_units
                return_value['nodes'][node] = allocation_units

        return return_value

    def get_mapping(self):
        """
        Get association between stoarage pool and aggregate
        :return: Dictionnary of link details if found, None otherwise
        """
        storage_aggr_get_iter = netapp_utils.zapi.NaElement('storage-pool-aggregate-get-iter')
        query_details = netapp_utils.zapi.NaElement.create_node_with_children(
            'storage-pool-aggregate-info', **{'aggregate': self.parameters.get('aggregate_name')})
        query = netapp_utils.zapi.NaElement('query')
        query.add_child_elem(query_details)
        storage_aggr_get_iter.add_child_elem(query)
        try:
            result = self.server.invoke_successfully(storage_aggr_get_iter, enable_tunneling=True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error setting option %s to %s on aggregate %s: %s' %
                                  (name, value, self.parameters.get('aggregate_name'), to_native(error)),
                                  exception=traceback.format_exc())

        return_value = None
        if result.get_child_by_name('num-records') and \
                int(result.get_child_content('num-records')) == 1:

            attributes = result.get_child_by_name('attributes-list').get_child_by_name('storage-pool-aggregate-info')
            return_value = {
                'aggregate_name': attributes.get_child_content('aggregate'),
                'storage_pool': attributes.get_child_content('storage-pool'),
                'allocation_units': int(attributes.get_child_content('allocated-unit-count'))
            }
        return return_value

    def set_aggregate_option(self, name, value):
        """
        Set option on aggregate. Used to set hybrid otion necessary to associate storage pool on aggregate
        """
        options = {'aggregate': self.parameters.get('aggregate_name'), 'option-name': name, 'option-value': value}
        aggr_set_option = netapp_utils.zapi.NaElement.create_node_with_children(
            'aggr-set-option', **options)

        try:
            result = self.server.invoke_successfully(aggr_set_option, enable_tunneling=True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error setting option %s to %s on aggregate %s: %s' %
                                  (name, value, self.parameters.get('aggregate_name'), to_native(error)),
                                  exception=traceback.format_exc())

    def set_storage_capacity(self, wanted_allocation_units):
        """
        Check capacity and reassign allocation unit(s) from one node to another if needed
        :param: allocation unit number you want (created or added)
        """
        capacity_available = self.get_available_capacity()
        if capacity_available is None:
            self.module.fail_json(msg='Failed to get available capacity on storage pool %s' % self.parameters.get('storage_pool'))

        if wanted_allocation_units > capacity_available['total']:
            self.module.fail_json(msg='Not enough available capacity on storage pool %s to allocate %d units.' %
                                  (self.parameters.get('storage_pool'), wanted_allocation_units))

        aggregate_info = self.get_aggregate()
        if aggregate_info is None:
            self.module.fail_json(msg='Failed to get details from aggregate %s.' % self.parameters.get('aggregate_name'))

        aggregate_node = aggregate_info['node']
        if aggregate_node not in capacity_available['nodes']:
            self.module.fail_json(msg='Unable to find node %s on the available capacity of storage pool %s.' %
                                  (aggregate_node, self.parameters.get('storage_pool')))

        current_node_capacity = int(capacity_available['nodes'][aggregate_node])
        # node has enough allocation units to create or modify mapping
        if current_node_capacity >= wanted_allocation_units:
            return

        missing_allocation_units = wanted_allocation_units - current_node_capacity
        # Order list of node with capacity units from bigger to lower
        ordered_available_capacity = sorted(capacity_available['nodes'].items(), key=operator.itemgetter(1), reverse=True)
        # Node with biger allocation units can meet the request allocatin units need
        if ordered_available_capacity[0][1] >= missing_allocation_units:
            self.reassign_storage_capacity(ordered_available_capacity[0][0], aggregate_node, missing_allocation_units)
        else:
            # Reallocate allocation units from each available nodes in the storage pool to the owner node of aggregate
            for k in ordered_available_capacity:
                available_allocation_units = int(ordered_available_capacity[k][1])
                if missing_allocation_units > available_allocation_units:
                    missing_allocation_units = missing_allocation_units - available_allocation_units
                    request_allocation_units = available_allocation_units
                else:
                    request_allocation_units = missing_allocation_units
                    missing_allocation_units = 0
                self.reassign_storage_capacity(ordered_available_capacity[k][0], aggregate_node, request_allocation_units)
                if missing_allocation_units == 0:
                    break

    def reassign_storage_capacity(self, from_node, to_node, allocation_units):
        """
        Reassign storage pool allocation unit(s) from one node to antoher
        :param: from_node - node with allocation unit(s) to reassign to another
        :param: to_node - destination node to assign allocation unit(s)
        :param: allocation_units - allocation unit number you want to reassign
        """
        options = {'storage-pool': self.parameters.get('storage_pool'),
                   'from-node': from_node, 'to-node': to_node,
                   'allocation-units': str(allocation_units)}
        storage_reassign = netapp_utils.zapi.NaElement.create_node_with_children(
            'storage-pool-reassign', **options)

        try:
            result = self.server.invoke_successfully(storage_reassign, enable_tunneling=True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Failed to reassign allocation units from node %s to node %s on storage pool %s: %s' %
                                  (from_node, to_node, self.parameters.get('storage_pool'),
                                   to_native(error)), exception=traceback.format_exc())

        if result.get_child_content('result-status') == 'in_progress':
            job_id = int(result.get_child_content('result-jobid'))
            self.wait_job(job_id)

    def add_storage_allocation_unit(self, allocation_units):
        """
        Add storage pool allocation unit(s) on aggregate
        :param: allocation unit you want to add on aggregate
        """
        self.set_storage_capacity(allocation_units)

        options = {'storage-pool': self.parameters.get('storage_pool'),
                   'aggregate': self.parameters.get('aggregate_name'),
                   'allocation-units': str(allocation_units)}

        aggr_add = netapp_utils.zapi.NaElement.create_node_with_children(
            'aggr-add', **options)
        try:
            result = self.server.invoke_successfully(aggr_add, enable_tunneling=True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error adding allocation units to aggregate %s from strage pool %s: %s' %
                                  (self.parameters.get('storage_pool'), self.parameters.get('aggregate_name'), to_native(error)),
                                  exception=traceback.format_exc())

        if result.get_child_content('result-status') == 'in_progress':
            job_id = int(result.get_child_content('result-jobid'))
            self.wait_job(job_id)

    def wait_job(self, job_id):
        """
        Wait reassign job unitl it is finihed or timeout
        :param: job Id to monitor
        """
        job_status = None
        function_time = 0

        options = {'job-id': str(job_id)}
        job_get_iter = netapp_utils.zapi.NaElement('job-get-iter')
        query_details = netapp_utils.zapi.NaElement.create_node_with_children(
            'job-info', **options)
        query = netapp_utils.zapi.NaElement('query')
        query.add_child_elem(query_details)
        job_get_iter.add_child_elem(query)

        while job_status != 'success':
            time.sleep(1)
            function_time += 1

            try:
                result = self.server.invoke_successfully(job_get_iter, enable_tunneling=True)
            except netapp_utils.zapi.NaApiError as error:
                self.module.fail_json(msg='Failed to get information from allocation reassign job id %d: %s' %
                                      (job_id, to_native(error)), exception=traceback.format_exc())

            if result.get_child_by_name('num-records'):
                records = int(result.get_child_content('num-records'))
                if records == 0:
                    job_status = 'success'
                elif records == 1:
                    attributes = result.get_child_by_name('attributes-list').get_child_by_name('job-info')
                    job_status = attributes.get_child_content('job-state')

            if function_time > self.parameters.get('timeout'):
                self.module.fail_json(msg='Allocation reassign job with id %d was timeout.' %
                                      job_id, exception=traceback.format_exc())

    def create_mapping(self):
        """
        Create relationship between storage pool and aggregate
        """
        self.get_aggregate()
        if self.hybrid_enabled is False:
            self.set_aggregate_option('hybrid_enabled', 'true')

        self.add_storage_allocation_unit(self.parameters.get('allocation_units'))

    def modify_mapping(self, modify):
        """
        Modify storage pool allocation unit(s) assigned on the aggregate
        :param modify: dictionary of parameters to be modified
        """
        allocation_units = self.parameters.get('allocation_units') - self.current.get('allocation_units')
        if allocation_units < 1:
            self.module.fail_json(msg='Allocation units cannot be reduced for aggregate %s on storage pool %s.' %
                                  (self.parameters.get('aggregate_name'), self.parameters.get('storage_pool')),
                                  exception=traceback.format_exc())

        self.add_storage_allocation_unit(allocation_units)

    def apply(self):
        """
        Apply action to the storage pool mapping
        """
        self.current = self.get_mapping()
        # rename and create are mutually exclusive
        cd_action = None
        cd_action = self.na_helper.get_cd_action(self.current, self.parameters)
        modify = self.na_helper.get_modified_attributes(self.current, self.parameters)

        if self.na_helper.changed:
            if self.module.check_mode:
                pass
            else:
                if cd_action == 'create':
                    self.create_mapping()
                elif modify:
                    self.modify_mapping(modify)
        self.module.exit_json(changed=self.na_helper.changed)


def main():
    """
    Create storage pool map class instance and invoke apply
    """
    obj_aggr = NetAppOntapStoragePoolMap()
    obj_aggr.apply()


if __name__ == '__main__':
    main()
