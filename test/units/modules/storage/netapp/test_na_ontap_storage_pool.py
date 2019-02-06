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

from ansible.modules.storage.netapp.na_ontap_storage_pool \
    import NetAppOntapStoragePool as my_module  # module under test

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

        self.ontap_version = '9.5'
        self.storage_has_aggregate = False

    def invoke_successfully(self, xml, enable_tunneling):  # pylint: disable=unused-argument
        ''' mock invoke_successfully returning xml data '''
        self.xml_in = xml

        if self.type == 'storage-pool':
            if xml.get_name() == 'storage-pool-get-iter':
                nameObj = xml.get_child_by_name('query').get_child_by_name('storage-pool-info').get_child_by_name('storage-pool')
                xml_name = nameObj.get_content()
                if xml_name == self.params.get('name'):
                    xml = self.build_storage_pool_info(self.params)
        elif self.type == 'storage-pool-fail':
            raise netapp_utils.zapi.NaApiError(code='TEST', message="This exception is from the unit test")

        if xml.get_name() == 'system-get-version':
            pos = self.ontap_version.find('.')
            xml = netapp_utils.zapi.NaElement('xml')
            attributes = {
                'version-tuple': {
                    'system-version-tuple': {
                        'generation': self.ontap_version[:pos],
                        'major': self.ontap_version[pos + 1:],
                        'minor': '0'
                    }
                }
            }
            xml.translate_struct(attributes)
        elif xml.get_name() == 'storage-pool-aggregate-get-iter':
            if self.storage_has_aggregate:
                xml = netapp_utils.zapi.NaElement('xml')
                attributes = {
                    'num-records': 1,
                    'attibutes-list': {
                        'storage-pool-aggregate-info': {
                            'storage-pool': self.params['name'],
                            'aggregate': 'aggr1'
                        }
                    }
                }
                xml.translate_struct(attributes)
        self.xml_out = xml
        return xml

    @staticmethod
    def build_storage_pool_info(data):
        ''' build xml data for storage-pool-info '''
        xml = netapp_utils.zapi.NaElement('xml')
        nodes = []
        for elem in data['nodes']:
            nodes.append({'node-name': elem})
        attributes = {
            'num-records': 1,
            'attributes-list': {
                'storage-pool-info': {
                    'storage-pool': data['name'],
                    'disk-count': data['disk_count'],
                    'nodes': nodes
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
            'name': 'test_pool',
            'hostname': 'hostname',
            'username': 'username',
            'password': 'password',
            'disk_count': 5,
            'nodes': ['VSIMNODE01', 'VSIMNODE02']
        })

    def test_module_fail_when_required_args_missing(self):
        ''' required arguments are reported as errors '''
        with pytest.raises(AnsibleFailJson) as exc:
            set_module_args({})
            my_module()
        print('Info: %s' % exc.value.args[0]['msg'])

    def test_ensure_get_called(self):
        ''' test get_storage_pool for non-existent storage pool'''
        set_module_args(self.set_default_args())
        my_obj = my_module()
        my_obj.server = self.server
        assert my_obj.get_storage_pool() is None

    def test_ensure_get_called_existing(self):
        ''' test get_storage_pool for existing storage pool'''
        data = self.set_default_args()
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='storage-pool', data=data)
        assert my_obj.get_storage_pool() is not None

    @patch('ansible.modules.storage.netapp.na_ontap_storage_pool.NetAppOntapStoragePool.create_storage_pool')
    def test_successful_create(self, create_storage_pool):
        ''' creating storage pool and testing idempotency '''
        data = self.set_default_args()
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = self.server
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert exc.value.args[0]['changed']
        create_storage_pool.assert_called_with()

        # to reset na_helper from remembering the previous 'changed' value
        data = self.set_default_args()
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='storage-pool', data=data)
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert not exc.value.args[0]['changed']

    @patch('ansible.modules.storage.netapp.na_ontap_storage_pool.NetAppOntapStoragePool.rename_storage_pool')
    def test_successful_rename(self, rename_storage_pool):
        ''' renaming storage pool '''
        data = self.set_default_args()
        data.update({'from_name': data['name'], 'name': 'new_test_pool'})
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='storage-pool', data=self.set_default_args())
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert exc.value.args[0]['changed']

    @patch('ansible.modules.storage.netapp.na_ontap_storage_pool.NetAppOntapStoragePool.delete_storage_pool')
    def test_successful_delete(self, delete_storage_pool):
        ''' deleting storage pool and testing idempotency '''
        data = self.set_default_args()
        data['state'] = 'absent'
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='storage-pool', data=data)
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert exc.value.args[0]['changed']
        delete_storage_pool.assert_called_with()

        # to reset na_helper from remembering the previous 'changed' value
        my_obj = my_module()
        my_obj.server = self.server
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert not exc.value.args[0]['changed']

    def test_fail_delete(self):
        ''' testing fail deletion with a used storage pool '''
        data = self.set_default_args()
        data['state'] = 'absent'
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='storage-pool', data=data)
        my_obj.server.storage_has_aggregate = True
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.apply()
        assert 'Cannot delete the used storage pool' in exc.value.args[0]['msg']

    def test_successful_modify(self):
        ''' modifying storage pool with adding one disk '''
        data = self.set_default_args()
        data.update({'disk_count': data['disk_count'] + 1})
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='storage-pool', data=self.set_default_args())
        with pytest.raises(AnsibleExitJson) as exc:
            my_obj.apply()
        assert exc.value.args[0]['changed']

    def test_fail_modify(self):
        ''' testing fail modifying with deletion of one disk '''
        data = self.set_default_args()
        data.update({'disk_count': data['disk_count'] - 1})
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='storage-pool', data=self.set_default_args())
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.apply()
        assert 'cannot decrease disk count' in exc.value.args[0]['msg']

    def test_if_all_methods_catch_exception(self):
        data = self.set_default_args()
        set_module_args(data)
        my_obj = my_module()
        my_obj.server = MockONTAPConnection(kind='storage-pool-fail', data=data)
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.create_storage_pool()
        assert 'Error creating storage pool' in exc.value.args[0]['msg']
        with pytest.raises(AnsibleFailJson) as exc:
            my_obj.rename_storage_pool()
        assert 'Error renaming storage pool' in exc.value.args[0]['msg']
