# (c) 2018, NetApp, Inc
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

''' unit test template for ONTAP Ansible module '''

from __future__ import print_function
import json
import pytest

from units.compat import unittest
from units.compat.mock import patch
from ansible.module_utils import basic
from ansible.module_utils._text import to_bytes
import ansible.module_utils.netapp as netapp_utils

from ansible.modules.storage.netapp.na_ontap_storage_pool_map \
    import NetAppOntapStoragePoolMap as my_module  # module under test

if not netapp_utils.has_netapp_lib():
    pytestmark = pytest.mark.skip('skipping as missing required netapp_lib')


def set_module_args(args):
    """prepare arguments so that they will be picked up during module creation"""
    args = json.dumps({'ANSIBLE_MODULE_ARGS': args})
    basic._ANSIBLE_ARGS = to_bytes(args)  # pylint: disable=protected-access


class AnsibleExitJson(Exception):
    """Exception class to be raised by module.exit_json and caught by the test case"""
    pass


class AnsibleFailJson(Exception):
    """Exception class to be raised by module.fail_json and caught by the test case"""
    pass


def exit_json(*args, **kwargs):  # pylint: disable=unused-argument
    """function to patch over exit_json; package return data into an exception"""
    if 'changed' not in kwargs:
        kwargs['changed'] = False
    raise AnsibleExitJson(kwargs)


def fail_json(*args, **kwargs):  # pylint: disable=unused-argument
    """function to patch over fail_json; package return data into an exception"""
    kwargs['failed'] = True
    raise AnsibleFailJson(kwargs)


class MockONTAPConnection(object):
    ''' mock server connection to ONTAP host '''

    def __init__(self, kind=None, data=None):
        ''' save arguments '''
        self.type = kind
        self.params = data
        self.xml_in = None
        self.xml_out = None

        self.available_allocation_units = {'node-01': 0, 'node-02': 0}
        self.hybrid_enabled = False
        self.timeout = 0

    def invoke_successfully(self, xml, enable_tunneling):  # pylint: disable=unused-argument
        ''' mock invoke_successfully returning xml data '''
        self.xml_in = xml

        if self.type == 'mapping':
            if xml.get_name() == 'storage-pool-aggregate-get-iter':
                nameObj = xml.get_child_by_name('query').get_child_by_name('storage-pool-aggregate-info').get_child_by_name('aggregate')
                xml_name = nameObj.get_content()
                if xml_name == self.params.get('aggregate_name'):
                    xml = self.build_storage_pool_mapping_info(self.params)
            if xml.get_name() == 'storage-pool-reassign':
                xml = netapp_utils.zapi.NaElement('xml')
                attributes = {
                    'result-status': 'in_progress',
                    'result-jobid': 1024
                }
                xml.translate_struct(attributes)
            if xml.get_name() == 'job-get-iter':
                job_status = 'success'
                if self.timeout > 0:
                    job_status = 'in_progress'

                xml = netapp_utils.zapi.NaElement('xml')
                attributes = {
                    'num-records': 1,
                    'attributes-list': {
                        'job-info': {
                            'job-state': job_status
                        }
                    }
                }
                xml.translate_struct(attributes)
        elif self.type == 'mapping-fail':
            raise netapp_utils.zapi.NaApiError(code='TEST', message="This exception is from the unit test")

        if xml.get_name() == 'storage-pool-available-capacity-get-iter':
            nameObj = xml.get_child_by_name('query').get_child_by_name('storage-pool-available-capacity-info').get_child_by_name('storage-pool')
            xml_name = nameObj.get_content()
            if xml_name == self.params['storage_pool']:
                xml = netapp_utils.zapi.NaElement('xml')
                attributes = {
                    'num-records': 1,
                    'attributes-list': [
                        {'storage-pool-available-capacity-info': {
                            'allocation-unit-count': str(self.available_allocation_units['node-01']),
                            'storage-pool': self.params['storage_pool'],
                            'node': 'node-01'
                        }},
                        {'storage-pool-available-capacity-info': {
                            'allocation-unit-count': str(self.available_allocation_units['node-02']),
                            'storage-pool': self.params['storage_pool'],
                            'node': 'node-02'
                        }}
                    ]
                }
                xml.translate_struct(attributes)
        elif xml.get_name() == 'aggr-get-iter':
            nameObj = xml.get_child_by_name('query').get_child_by_name('aggr-attributes').get_child_by_name('aggregate-name')
            xml_name = nameObj.get_content()
            if xml_name == self.params.get('aggregate_name'):
                xml = netapp_utils.zapi.NaElement('xml')
                hybrid_enabled = 'false'
                if self.hybrid_enabled:
                    hybrid_enabled = 'true'
                attributes = {
                    'num-records': 1,
                    'attributes-list': {
                        'aggr-attributes': {
                            'aggr-ownership-attributes': {
                                'owner-name': 'node-01'
                            },
                            'aggr-raid-attributes': {
                                'is-hybrid-enabled': hybrid_enabled
                            }
                        }
                    }
                }
                xml.translate_struct(attributes)

        return xml

    @staticmethod
    def build_storage_pool_mapping_info(data):
        ''' build xml data for storage-pool-aggregate-info '''
        xml = netapp_utils.zapi.NaElement('xml')
        attributes = {
            'num-records': 1,
            'attributes-list': {
                'storage-pool-aggregate-info': {
                    'aggregate': data['aggregate_name'],
                    'storage-pool': data['storage_pool'],
                    'allocated-unit-count': 2
                }
            }
        }
        xml.translate_struct(attributes)
        return xml


class TestMyModule(unittest.TestCase):
    ''' a group of related Unit Tests '''

    def setUp(self):
        self.mock_module_helper = patch.multiple(basic.AnsibleModule,
                                                 exit_json=exit_json,
                                                 fail_json=fail_json)
        self.mock_module_helper.start()
        self.addCleanup(self.mock_module_helper.stop)
        self.server = MockONTAPConnection()

    def set_default_args(self):
        return dict({
            'aggregate_name': 'aggr1',
            'storage_pool': 'test_pool',
            'allocation_units': 2,
            'hostname': 'hostname',
            'username': 'username',
            'password': 'password'
        })

    def test_module_fail_when_required_args_missing(self):
        ''' required arguments are reported as errors '''
        with pytest.raises(AnsibleFailJson) as exc:
            set_module_args({})
            my_module()
        print('Info: %s' % exc.value.args[0]['msg'])

    def test_ensure_get_called(self):
        ''' test get_mapping for non-existent storage pool mapping'''
        set_module_args(self.set_default_args())
        my_obj = my_module()
        my_obj.server = self.server
        assert my_obj.get_mapping() is None

    def test_ensure_get_called_existing(self):
        ''' test get_mapping for existing storage pool mapping'''
        data = self.set_default_args()
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='mapping', data=data)
        assert my_obj.get_mapping() is not None

    @patch('ansible.modules.storage.netapp.na_ontap_storage_pool_map.NetAppOntapStoragePoolMap.create_mapping')
    def test_successful_create(self, create_mapping):
        ''' creating storage pool and testing idempotency '''
        data = self.set_default_args()
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(data=data)
        my_obj.server.available_allocation_units = {'node-01': 2, 'node-02': 2}
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert exc.value.args[0]['changed']
        create_mapping.assert_called_with()

        # to reset na_helper from remembering the previous 'changed' value
        data = self.set_default_args()
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='mapping', data=data)
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert not exc.value.args[0]['changed']

    def test_successful_modify_on_current_node(self):
        ''' modifying storage pool mapping with adding allocation unit on the current node '''
        data = self.set_default_args()
        data.update({'allocation_units': data['allocation_units'] + 1})
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='mapping', data=self.set_default_args())
        my_obj.server.available_allocation_units = {'node-01': 2, 'node-02': 0}
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert exc.value.args[0]['changed']

    def test_successful_modify_on_other_node(self):
        ''' modifying storage pool mapping with adding allocation unit on another node '''
        data = self.set_default_args()
        data.update({'allocation_units': data['allocation_units'] + 1})
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='mapping', data=self.set_default_args())
        my_obj.server.available_allocation_units = {'node-01': 0, 'node-02': 2}
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert exc.value.args[0]['changed']

    def test_fail_modify_with_decrease_unit(self):
        ''' testing fail modifying with decrease of one allocation unit '''
        data = self.set_default_args()
        data.update({'allocation_units': data['allocation_units'] - 1})
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='mapping', data=self.set_default_args())
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.apply()
        assert 'cannot be reduced for aggregate' in exc.value.args[0]['msg']

    def test_fail_modify_with_no_available_unit(self):
        ''' testing fail modifying with no available allocation unit on the cluster '''
        data = self.set_default_args()
        data.update({'allocation_units': data['allocation_units'] + 1})
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='mapping', data=self.set_default_args())
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.apply()
        assert 'Not enough available capacity' in exc.value.args[0]['msg']

    def test_fail_modify_with_job_timeout(self):
        ''' testing fail modifying with staorage pool reassign job timeout '''
        data = self.set_default_args()
        data.update({'allocation_units': data['allocation_units'] + 2, 'timeout': 3})
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='mapping', data=self.set_default_args())
        my_obj.server.timeout = 3
        my_obj.server.available_allocation_units = {'node-01': 0, 'node-02': 2}
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.apply()
        assert 'Allocation reassign job with id 1024 was timeout' in exc.value.args[0]['msg']

    def test_if_all_methods_catch_exception(self):
        ''' testing fail XML requests on all other module functions '''
        data = self.set_default_args()
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='mapping-fail', data=data)
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.get_aggregate()
        assert 'Error getting details from aggregate' in exc.value.args[0]['msg']
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.get_available_capacity()
        assert 'Error getting available capacity from storage pool' in exc.value.args[0]['msg']
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.set_aggregate_option('hybrid-enabled', 'true')
        assert 'Error setting option' in exc.value.args[0]['msg']
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.reassign_storage_capacity('node-02', 'node-01', 1)
        assert 'Failed to reassign allocation units from node' in exc.value.args[0]['msg']
