# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Unit tests for the DB API"""

import datetime

from nova import test
from nova import context
from nova import db
from nova import exception
from nova import flags
from nova import utils

FLAGS = flags.FLAGS


def _setup_networking(instance_id, ip='1.2.3.4', flo_addr='1.2.1.2'):
    ctxt = context.get_admin_context()
    network_ref = db.project_get_networks(ctxt,
                                           'fake',
                                           associate=True)[0]
    vif = {'address': '56:12:12:12:12:12',
           'network_id': network_ref['id'],
           'instance_id': instance_id}
    vif_ref = db.virtual_interface_create(ctxt, vif)

    fixed_ip = {'address': ip,
                'network_id': network_ref['id'],
                'virtual_interface_id': vif_ref['id'],
                'allocated': True,
                'instance_id': instance_id}
    db.fixed_ip_create(ctxt, fixed_ip)
    fix_ref = db.fixed_ip_get_by_address(ctxt, ip)
    db.floating_ip_create(ctxt, {'address': flo_addr,
                                 'fixed_ip_id': fix_ref['id']})


class DbApiTestCase(test.TestCase):
    def setUp(self):
        super(DbApiTestCase, self).setUp()
        self.user_id = 'fake'
        self.project_id = 'fake'
        self.context = context.RequestContext(self.user_id, self.project_id)

    def test_instance_get_project_vpn(self):
        values = {'instance_type_id': FLAGS.default_instance_type,
                  'image_ref': FLAGS.vpn_image_id,
                  'project_id': self.project_id,
                 }
        instance = db.instance_create(self.context, values)
        result = db.instance_get_project_vpn(self.context.elevated(),
                                             self.project_id)
        self.assertEqual(instance['id'], result['id'])

    def test_instance_get_project_vpn_joins(self):
        values = {'instance_type_id': FLAGS.default_instance_type,
                  'image_ref': FLAGS.vpn_image_id,
                  'project_id': self.project_id,
                 }
        instance = db.instance_create(self.context, values)
        _setup_networking(instance['id'])
        result = db.instance_get_project_vpn(self.context.elevated(),
                                             self.project_id)
        self.assertEqual(instance['id'], result['id'])
        self.assertEqual(result['fixed_ips'][0]['floating_ips'][0].address,
                         '1.2.1.2')

    def test_instance_get_all_by_filters(self):
        args = {'reservation_id': 'a', 'image_ref': 1, 'host': 'host1'}
        inst1 = db.instance_create(self.context, args)
        inst2 = db.instance_create(self.context, args)
        result = db.instance_get_all_by_filters(self.context, {})
        self.assertTrue(2, len(result))

    def test_instance_get_all_by_filters_deleted(self):
        args1 = {'reservation_id': 'a', 'image_ref': 1, 'host': 'host1'}
        inst1 = db.instance_create(self.context, args1)
        args2 = {'reservation_id': 'b', 'image_ref': 1, 'host': 'host1'}
        inst2 = db.instance_create(self.context, args2)
        db.instance_destroy(self.context, inst1.id)
        result = db.instance_get_all_by_filters(self.context.elevated(), {})
        self.assertEqual(2, len(result))
        self.assertIn(inst1.id, [result[0].id, result[1].id])
        self.assertIn(inst2.id, [result[0].id, result[1].id])
        if inst1.id == result[0].id:
            self.assertTrue(result[0].deleted)
        else:
            self.assertTrue(result[1].deleted)

    def test_migration_get_all_unconfirmed(self):
        ctxt = context.get_admin_context()

        # Ensure no migrations are returned.
        results = db.migration_get_all_unconfirmed(ctxt, 10)
        self.assertEqual(0, len(results))

        # Ensure one migration older than 10 seconds is returned.
        updated_at = datetime.datetime(2000, 01, 01, 12, 00, 00)
        values = {"status": "FINISHED", "updated_at": updated_at}
        migration = db.migration_create(ctxt, values)
        results = db.migration_get_all_unconfirmed(ctxt, 10)
        self.assertEqual(1, len(results))
        db.migration_update(ctxt, migration.id, {"status": "CONFIRMED"})

        # Ensure the new migration is not returned.
        updated_at = datetime.datetime.utcnow()
        values = {"status": "FINISHED", "updated_at": updated_at}
        migration = db.migration_create(ctxt, values)
        results = db.migration_get_all_unconfirmed(ctxt, 10)
        self.assertEqual(0, len(results))
        db.migration_update(ctxt, migration.id, {"status": "CONFIRMED"})

    def test_instance_get_all_hung_in_rebooting(self):
        ctxt = context.get_admin_context()

        # Ensure no instances are returned.
        results = db.instance_get_all_hung_in_rebooting(ctxt, 10)
        self.assertEqual(0, len(results))

        # Ensure one rebooting instance with updated_at older than 10 seconds
        # is returned.
        updated_at = datetime.datetime(2000, 01, 01, 12, 00, 00)
        values = {"task_state": "rebooting", "updated_at": updated_at}
        instance = db.instance_create(ctxt, values)
        results = db.instance_get_all_hung_in_rebooting(ctxt, 10)
        self.assertEqual(1, len(results))
        db.instance_update(ctxt, instance.id, {"task_state": None})

        # Ensure the newly rebooted instance is not returned.
        updated_at = datetime.datetime.utcnow()
        values = {"task_state": "rebooting", "updated_at": updated_at}
        instance = db.instance_create(ctxt, values)
        results = db.instance_get_all_hung_in_rebooting(ctxt, 10)
        self.assertEqual(0, len(results))
        db.instance_update(ctxt, instance.id, {"task_state": None})

    def test_network_create_safe(self):
        ctxt = context.get_admin_context()
        values = {'host': 'localhost', 'project_id': 'project1'}
        network = db.network_create_safe(ctxt, values)
        self.assertNotEqual(None, network.uuid)
        self.assertEqual(36, len(network.uuid))
        db_network = db.network_get(ctxt, network.id)
        self.assertEqual(network.uuid, db_network.uuid)

    def test_instance_update_with_instance_id(self):
        """ test instance_update() works when an instance id is passed """
        ctxt = context.get_admin_context()

        # Create an instance with some metadata
        metadata = {'host': 'foo'}
        values = {'metadata': metadata}
        instance = db.instance_create(ctxt, values)

        # Update the metadata
        metadata = {'host': 'bar'}
        values = {'metadata': metadata}
        db.instance_update(ctxt, instance.id, values)

        # Retrieve the metadata to ensure it was successfully updated
        instance_meta = db.instance_metadata_get(ctxt, instance.id)
        self.assertEqual('bar', instance_meta['host'])

    def test_instance_update_with_instance_uuid(self):
        """ test instance_update() works when an instance UUID is passed """
        ctxt = context.get_admin_context()

        # Create an instance with some metadata
        metadata = {'host': 'foo'}
        values = {'metadata': metadata}
        instance = db.instance_create(ctxt, values)

        # Update the metadata
        metadata = {'host': 'bar'}
        values = {'metadata': metadata}
        db.instance_update(ctxt, instance.uuid, values)

        # Retrieve the metadata to ensure it was successfully updated
        instance_meta = db.instance_metadata_get(ctxt, instance.id)
        self.assertEqual('bar', instance_meta['host'])

    def test_instance_fault_create(self):
        """Ensure we can create an instance fault"""
        ctxt = context.get_admin_context()
        uuid = str(utils.gen_uuid())

        # Create a fault
        fault_values = {
            'message': 'message',
            'details': 'detail',
            'instance_uuid': uuid,
            'code': 404,
        }
        db.instance_fault_create(ctxt, fault_values)

        # Retrieve the fault to ensure it was successfully added
        instance_fault = db.instance_fault_get_by_instance(ctxt, uuid)
        self.assertEqual(404, instance_fault['code'])

    def test_instance_fault_get_by_instance(self):
        """ ensure we can retrieve an instance fault by  instance UUID """
        ctxt = context.get_admin_context()

        # Create faults
        uuid = str(utils.gen_uuid())
        fault_values = {
            'message': 'message',
            'details': 'detail',
            'instance_uuid': uuid,
            'code': 404,
        }
        db.instance_fault_create(ctxt, fault_values)

        uuid2 = str(utils.gen_uuid())
        fault_values = {
            'message': 'message',
            'details': 'detail',
            'instance_uuid': uuid2,
            'code': 500,
        }
        db.instance_fault_create(ctxt, fault_values)

        # Retrieve the fault to ensure it was successfully added
        instance_fault = db.instance_fault_get_by_instance(ctxt, uuid2)
        self.assertEqual(500, instance_fault['code'])

    def test_instance_fault_get_by_instance_first_fault(self):
        """Instance_fault_get_by_instance should return the latest fault """
        ctxt = context.get_admin_context()

        # Create faults
        uuid = str(utils.gen_uuid())
        fault_values = {
            'message': 'message',
            'details': 'detail',
            'instance_uuid': uuid,
            'code': 404,
        }
        db.instance_fault_create(ctxt, fault_values)

        fault_values = {
            'message': 'message',
            'details': 'detail',
            'instance_uuid': uuid,
            'code': 500,
        }
        db.instance_fault_create(ctxt, fault_values)

        # Retrieve the fault to ensure it was successfully added
        instance_fault = db.instance_fault_get_by_instance(ctxt, uuid)
        self.assertEqual(500, instance_fault['code'])


class CapacityTestCase(test.TestCase):
    def setUp(self):
        super(CapacityTestCase, self).setUp()
        self.user_id = 'fake'
        self.project_id = 'fake'
        self.context = context.RequestContext(self.user_id, self.project_id)

        self.ctxt = context.get_admin_context()

        service_dict = dict(host='host1', binary='binary1',
                            topic='compute', report_count=1,
                            disabled=False)
        self.service = db.service_create(self.ctxt, service_dict)

        compute_node_dict = dict(vcpus=2, memory_mb=1024, local_gb=2048,
                                 vcpus_used=0, memory_mb_used=0,
                                 local_gb_used=0, hypervisor_type="xen",
                                 hypervisor_version=1, cpu_info="",
                                 service_id=self.service.id)

        self.compute_node = db.compute_node_create(self.ctxt,
                                                   compute_node_dict)

        self.flags(reserved_host_memory_mb=0)
        self.flags(reserved_host_disk_mb=0)

    def test_capacity_new(self):
        try:
            db.capacity_new(self.ctxt, 'unknown')
            self.fail("Should not be able to find that host.")
        except exception.NotFound:
            pass

        item = db.capacity_new(self.ctxt, 'host1')
        self.assertEquals(item.free_ram_mb, 1024)
        self.assertEquals(item.free_disk_gb, 2048)
        self.assertEquals(item.running_vms, 0)
        self.assertEquals(item.current_workload, 0)

    def test_capacity_new_with_reservations(self):
        self.flags(reserved_host_memory_mb=256)
        item = db.capacity_new(self.ctxt, 'host1')
        self.assertEquals(item.free_ram_mb, 1024 - 256)

    def test_capacity_set(self):
        item = db.capacity_new(self.ctxt, 'host1')

        x = db.capacity_set(self.ctxt, 'host1', free_ram_mb=2048,
                            free_disk_gb=4096)
        self.assertEquals(x.free_ram_mb, 2048)
        self.assertEquals(x.free_disk_gb, 4096)
        self.assertEquals(x.running_vms, 0)
        self.assertEquals(x.current_workload, 0)

        x = db.capacity_set(self.ctxt, 'host1', work=3)
        self.assertEquals(x.free_ram_mb, 2048)
        self.assertEquals(x.free_disk_gb, 4096)
        self.assertEquals(x.current_workload, 3)
        self.assertEquals(x.running_vms, 0)

        x = db.capacity_set(self.ctxt, 'host1', vms=5)
        self.assertEquals(x.free_ram_mb, 2048)
        self.assertEquals(x.free_disk_gb, 4096)
        self.assertEquals(x.current_workload, 3)
        self.assertEquals(x.running_vms, 5)

    def test_capacity_cache_update(self):
        item = db.capacity_new(self.ctxt, 'host1')

        x = db.capacity_update(self.ctxt, 'host1', free_ram_mb_delta=-24)
        self.assertEquals(x.free_ram_mb, 1000)
        self.assertEquals(x.free_disk_gb, 2048)
        self.assertEquals(x.running_vms, 0)
        self.assertEquals(x.current_workload, 0)

        x = db.capacity_update(self.ctxt, 'host1', free_disk_gb_delta=-48)
        self.assertEquals(x.free_ram_mb, 1000)
        self.assertEquals(x.free_disk_gb, 2000)
        self.assertEquals(x.running_vms, 0)
        self.assertEquals(x.current_workload, 0)

        x = db.capacity_update(self.ctxt, 'host1', work_delta=3)
        self.assertEquals(x.free_ram_mb, 1000)
        self.assertEquals(x.free_disk_gb, 2000)
        self.assertEquals(x.current_workload, 3)
        self.assertEquals(x.running_vms, 0)

        x = db.capacity_update(self.ctxt, 'host1', work_delta=-1)
        self.assertEquals(x.free_ram_mb, 1000)
        self.assertEquals(x.free_disk_gb, 2000)
        self.assertEquals(x.current_workload, 2)
        self.assertEquals(x.running_vms, 0)

        x = db.capacity_update(self.ctxt, 'host1', vm_delta=5)
        self.assertEquals(x.free_ram_mb, 1000)
        self.assertEquals(x.free_disk_gb, 2000)
        self.assertEquals(x.current_workload, 2)
        self.assertEquals(x.running_vms, 5)
