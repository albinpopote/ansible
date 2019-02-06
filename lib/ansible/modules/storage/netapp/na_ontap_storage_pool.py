#!/usr/bin/python

# (c) 2018, NetApp, Inc
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type


ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = """
module: na_ontap_storage_pool
short_description: NetApp ONTAP create, modify and delete storage pool.
extends_documentation_fragment:
    - netapp.na_ontap
version_added: '2.8'
author:  Storage Engineering (@Albinpopote) <ansible@black-perl.fr>
description:
- Create, modify, destroy the storage pool
options:
  state:
    description:
    - Whether the specified network interface group should exist or not.
    choices: ['present', 'absent']
    default: present

  disk_count:
    description:
    - Specify the number of disks that are part of the storage pool.

  name:
    description:
    - Specify the storage pool name.
    required: true

  from_name:
    description:
    - Name of the storage pool to be renamed
    - The rename function is only supported with ONTAP version 9.5 and later.

  disk_list:
    description:
    - Specify the list of disks that should be used to create the storage pool.

  nodes:
    description:
    - Specify the list of nodes in which the storage pool resides.

  from_node:
    description:
    - Specify the source node

  to_node:
    description:
    - Specify the destination node

  allocation_units:
    description:
    - Specify units tata toto test

  timeout:
    description:
    - Time in seconds to wait the reassign allocation between nodes if needed
    - default is 300 seconds
    default: 300
"""

EXAMPLES = """
    - name: create storage pool
      na_ontap_storage_pool:
        state: present
        username: "{{ netapp_username }}"
        password: "{{ netapp_password }}"
        hostname: "{{ netapp_hostname }}"
        disk_count: 5
        nodes: [ 'OPFP3CHEFAS8200PP-01', 'OPFP3CHEFAS8200PP-02' ]
        name: FlashPool
    - name: delete storage pool
      na_ontap_storage_pool:
        state: absent
        username: "{{ netapp_username }}"
        password: "{{ netapp_password }}"
        hostname: "{{ netapp_hostname }}"
        name:  FlashPool
    - name: rename storage pool
      na_ontap_storage_pool:
        state: present
        username: "{{ netapp_username }}"
        password: "{{ netapp_password }}"
        hostname: "{{ netapp_hostname }}"
        name: NewFlashPool
        from_name: FlashPool
    - name: reassign allocation unit to node
      na_ontap_storage_pool:
        state: present
        username: "{{ netapp_username }}"
        password: "{{ netapp_password }}"
        hostname: "{{ netapp_hostname }}"
        name: FlashPool
        from_node: VSIMNODE01
        to_node: VSIMNODE02
        allocation_units: 2
"""

RETURN = """

"""

import traceback
import time

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils._text import to_native
import ansible.module_utils.netapp as netapp_utils
from ansible.module_utils.netapp_module import NetAppModule

from distutils.version import StrictVersion

HAS_NETAPP_LIB = netapp_utils.has_netapp_lib()


class NetAppOntapStoragePool(object):
    """
    Create, Modify and Destroy a storage pool
    """
    def __init__(self):
        """
        Initialize the ONTAP Storage Pool class
        """
        self.argument_spec = netapp_utils.na_ontap_host_argument_spec()
        self.argument_spec.update(dict(
            state=dict(required=False, choices=['present', 'absent'], default='present'),
            name=dict(required=True, type='str'),
            from_name=dict(required=False, type='str'),
            disk_count=dict(required=False, type='int'),
            disk_list=dict(required=False, type='list'),
            nodes=dict(required=False, type='list'),
            timeout=dict(required=False, type='int', default=300),
            from_node=dict(required=False, type='str'),
            to_node=dict(required=False, type='str'),
            allocation_units=dict(required=False, type='int')
        ))

        self.module = AnsibleModule(
            argument_spec=self.argument_spec,
            required_together=[['from_node', 'to_node', 'allocation_units']],
            supports_check_mode=True
        )
        self.na_helper = NetAppModule()
        self.parameters = self.na_helper.set_parameters(self.module.params)

        if HAS_NETAPP_LIB is False:
            self.module.fail_json(msg="the python NetApp-Lib module is required")
        else:
            self.server = netapp_utils.setup_na_ontap_zapi(module=self.module)
        return

    def get_storage_pool(self, name=None):
        """
        Return details about the storage pool
        :param:
            name : Name of the storage pool
        :return: Details about the storage pool. None if not found.
        :rtype: dict
        """
        if name is None:
            name = self.parameters.get('name')

        pool_iter = netapp_utils.zapi.NaElement('storage-pool-get-iter')
        pool_info = netapp_utils.zapi.NaElement('storage-pool-info')
        pool_info.add_new_child('storage-pool', name)

        query = netapp_utils.zapi.NaElement('query')
        query.add_child_elem(pool_info)

        pool_iter.add_child_elem(query)

        result = self.server.invoke_successfully(pool_iter, True)

        return_value = None
        # check if query returns the expected storage pool
        if result.get_child_by_name('num-records') and \
                int(result.get_child_content('num-records')) == 1:

            pool_attributes = result.get_child_by_name('attributes-list').get_child_by_name('storage-pool-info')
            disk_count = int(pool_attributes.get_child_content('disk-count'))
            name = pool_attributes.get_child_content('storage-pool')
            nodes_obj = pool_attributes.get_child_by_name('nodes')
            nodes = [each.get_content() for each in nodes_obj.get_children()]
            disk_list_obj = pool_attributes.get_child_by_name('disk-list')
            if disk_list_obj is None:
                disk_list = []
            else:
                disk_list = [each.get_content() for each in disk_list_obj.get_children()]

            return_value = {
                'name': name,
                'disk_count': disk_count,
                'disk_list': disk_list,
                'nodes': nodes
            }

        return return_value

    def get_aggregate_used(self):
        """
        Get information about utilization of storage pool by the aggregate
        """
        aggr_iter = netapp_utils.zapi.NaElement('storage-pool-aggregate-get-iter')
        agggr_info = netapp_utils.zapi.NaElement('storage-pool-aggregate-info')
        agggr_info.add_new_child('storage-pool', self.parameters.get('name'))

        query = netapp_utils.zapi.NaElement('query')
        query.add_child_elem(agggr_info)

        aggr_iter.add_child_elem(query)

        try:
            result = self.server.invoke_successfully(aggr_iter, True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error getting aggregate info from storage pool %s: %s' % (self.parameters.get('name'),
                                  to_native(error)), exception=traceback.format_exc())
        return result

    def get_ontap_version(self):
        """
        Get ontap version to check if rename function is supported on the API
        """
        system = netapp_utils.zapi.NaElement('system-get-version')
        try:
            result = self.server.invoke_successfully(system, True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error getting ONTAP version: %s' % to_native(error),
                                  exception=traceback.format_exc())

        system_version = None
        version_tuple = result.get_child_by_name('version-tuple').get_child_by_name('system-version-tuple')
        if version_tuple:
            system_version = version_tuple.get_child_content('generation') + '.' + version_tuple.get_child_content('major')

        if system_version is None:
            self.module.fail_json(msg='Unable to find ontap version', exception=traceback.format_exc())

        return system_version

    def create_storage_pool(self):
        """
        Create a new storage pool
        """
        options = {'storage-pool': self.parameters.get('name')}

        pool_create = netapp_utils.zapi.NaElement.create_node_with_children(
            'storage-pool-create', **options)

        if self.parameters.get('disk_count'):
            pool_create.add_new_child('disk-count', str(self.parameters.get('disk_count')))
        if self.parameters.get('disk_list'):
            pool_disks = netapp_utils.zapi.NaElement('disk-list')
            pool_create.add_child_elem(pool_disks)
            for disk_name in self.parameters.get('disk_list'):
                pool_disks.add_new_child('disk-name', disk_name)
        if self.parameters.get('nodes'):
            pool_nodes = netapp_utils.zapi.NaElement('nodes')
            pool_create.add_child_elem(pool_nodes)
            for node in self.parameters.get('nodes'):
                pool_nodes.add_new_child('node-name', node)

        try:
            result = self.server.invoke_successfully(pool_create, True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error creating storage pool %s: %s' % (self.parameters.get('name'), to_native(error)),
                                  exception=traceback.format_exc())

        if result.get_child_content('result-status') == 'in_progress':
            job_id = int(result.get_child_content('result-jobid'))
            self.wait_job(job_id)

    def delete_storage_pool(self):
        """
        Delete a storage pool
        """
        agrr_list = self.get_aggregate_used()
        if agrr_list.get_child_by_name('num-records') and \
                int(agrr_list.get_child_content('num-records')) == 1:
            self.module.fail_json(msg='Cannot delete the used storage pool %s' % self.parameters.get('name'),
                                  exception=traceback.format_exc())

        pool_delete = netapp_utils.zapi.NaElement.create_node_with_children(
            'storage-pool-delete', **{'storage-pool': self.parameters.get('name')})

        try:
            self.server.invoke_successfully(pool_delete, True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error deleting storage pool %s: %s' % (self.parameters.get('name'), to_native(error)),
                                  exception=traceback.format_exc())

    def rename_storage_pool(self):
        """
        Rename the storage pool
        """
        options = {'storage-pool': self.parameters.get('from_name'),
                   'new-name': self.parameters.get('name')}

        pool_rename = netapp_utils.zapi.NaElement.create_node_with_children(
            'storage-pool-rename', **options)

        try:
            self.server.invoke_successfully(pool_rename, True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error renaming storage pool %s: %s' % (self.parameters.get('name'), to_native(error)),
                                  exception=traceback.format_exc())

    def reassign_allocation_units(self):
        """
        Reassign allocation unit(s) from one node to another
        """
        options = {'from-node': self.parameters.get('from_node'),
                   'to-node': self.parameters.get('to_node'),
                   'allocation-units': str(self.parameters.get('allocation_units')),
                   'storage-pool': self.parameters.get('name')}
        pool_reassign = netapp_utils.zapi.NaElement.create_node_with_children(
            'storage-pool-reassign', **options)

        try:
            self.server.invoke_successfully(pool_reassign, True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error reallocating units on storage pool %s: %s' % (self.parameters.get('name'), to_native(error)),
                                  exception=traceback.format_exc())

    def add_storage_pool(self, current):
        """
        Add disk to storage pool
        """
        options = {'storage-pool': self.parameters.get('name')}

        pool_add = netapp_utils.zapi.NaElement.create_node_with_children(
            'storage-pool-add', **options)

        modify = self.na_helper.get_modified_attributes(current, self.parameters)
        if "disk_count" in modify:
            if int(current['disk_count']) > int(self.parameters.get('disk_count')):
                self.module.fail_json(msg='Error modifying storage pool %s: cannot decrease disk count.' % self.parameters.get('name'),
                                      exception=traceback.format_exc())

            add_disk = int(self.parameters.get('disk_count')) - int(current['disk_count'])
            pool_add.add_new_child('disk-count', str(add_disk))
        elif "disk_list" in modify:
            if len(current['disk_list']) > len(self.parameters.get('disk_list')):
                self.module.fail_json(msg='Error modifying storage pool %s: cannot decrease disk list.' % self.parameters.get('name'),
                                      exception=traceback.format_exc())
            if not all(elem in current['disk_list'] for elem in self.parameters.get('disk_list')):
                self.module.fail_json(msg='Error modifying storage pool %s: cannot remove a disk from the existing disks list.' % self.parameters.get('name'),
                                      exception=traceback.format_exc())

            pool_disks = netapp_utils.zapi.NaElement('disk-list')
            pool_add.add_child_elem(pool_disks)
            for disk_name in list(set(self.parameters.get('disk_list')) - set(current['disk_list'])):
                pool_disks.add_new_child('disk-name', disk_name)

        try:
            result = self.server.invoke_successfully(pool_add, True)
        except netapp_utils.zapi.NaApiError as error:
            self.module.fail_json(msg='Error getting ONTAP version: %s' % to_native(error),
                                  exception=traceback.format_exc())

    def wait_job(self, job_id):
        """
        Wait reassign job until it is finihed or timeout
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

    def apply(self):
        '''Apply action to storage pool'''

        ontap_version = self.get_ontap_version()
        current = self.get_storage_pool()
        cd_action, rename = None, None

        if self.parameters.get('from_name'):
            rename = self.na_helper.is_rename_action(self.get_storage_pool(self.parameters.get('from_name')), current)
            if rename is None:
                self.module.fail_json(msg="Error renaming: storage pool %s does not exist" %
                                      self.parameters.get('from_name'))
            if StrictVersion(ontap_version) < StrictVersion('9.5'):
                self.module.fail_json(msg="Error renaming storage pool %s: rename api is only available with DOT 9.5 or later" %
                                      self.parameters.get('from_name'))

            self.rename_storage_pool()
        elif self.parameters.get('from_node'):
            if current is None:
                self.module.fail_json(msg="Error reassign: storage pool %s does not exist" %
                                      self.parameters.get('name'))
            self.reassign_allocation_units()
        else:
            cd_action = self.na_helper.get_cd_action(current, self.parameters)

            modify = self.na_helper.get_modified_attributes(current, self.parameters)
            if self.na_helper.changed:
                if self.module.check_mode:
                    pass
                else:
                    if cd_action == 'create':
                        self.create_storage_pool()
                    elif cd_action == 'delete':
                        self.delete_storage_pool()
                    elif modify:
                        self.add_storage_pool(current)
        self.module.exit_json(changed=self.na_helper.changed)


def main():
    """
    Creates the NetApp ONTAP Net Route object and runs the correct play task
    """
    pool_obj = NetAppOntapStoragePool()
    pool_obj.apply()


if __name__ == '__main__':
    main()
