# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2011 OpenStack LLC
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
"""
Tests For Simple Scheduler
"""

from nova import context
from nova import db
from nova import exception
from nova import utils
from nova.scheduler import driver
from nova.scheduler import simple
from nova.tests.scheduler import test_chance_scheduler
from nova.tests.scheduler import test_scheduler


def _create_service(**kwargs):
    """Create a compute service."""

    t = utils.utcnow()
    service = {'id': 1,
               'binary': 'nova-compute',
               'topic': 'compute',
               'report_count': 0,
               'availability_zone': 'zone1',
               'host': 'host1',
               'created_at': t,
               'updated_at': t,
               'disabled': False,
               'compute_node': []}
    service.update(kwargs)
    return service


def _create_compute_node(service, **kwargs):

    compute = {'id': 1,
               'service_id': service['id'],
               'vcpus': 16,
               'vcpus_used': 0,
               'memory_mb': 32,
               'memory_mb_used': 0,
               'local_gb': 100,
               'local_gb_used': 0,
               'hypervisor_type': 'qemu',
               'hypervisor_version': 12003,
               'cpu_info': ''}
    compute.update(kwargs)
    service['compute_node'].append(compute)
    return compute


def _create_volume(**kwargs):
    volume = {'id': 1, 'size': 20}
    volume.update(kwargs)
    return volume


class SimpleSchedulerTestCase(test_chance_scheduler.ChanceSchedulerTestCase):
    """Test case for simple driver"""

    driver_cls = simple.SimpleScheduler

    # Overrides the Chance test... this is tested below w/ 'test_no_hosts'
    def test_basic_schedule_run_instance_no_hosts(self):
        pass

    # Overrides the Chance test
    def test_basic_schedule_run_instance(self):

        ctxt = context.RequestContext('fake', 'fake', False)
        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}
        instance_opts = {'fake_opt1': 'meow'}
        request_spec = {'num_instances': 2,
                        'instance_properties': instance_opts}
        instance1 = {'uuid': 'fake-uuid1'}
        instance2 = {'uuid': 'fake-uuid2'}
        instance1_encoded = {'uuid': 'fake-uuid1', '_is_precooked': False}
        instance2_encoded = {'uuid': 'fake-uuid2', '_is_precooked': False}

        # create_instance_db_entry() usually does this, but we're
        # stubbing it.
        def _add_uuid1(ctxt, request_spec):
            request_spec['instance_properties']['uuid'] = 'fake-uuid1'

        def _add_uuid2(ctxt, request_spec):
            request_spec['instance_properties']['uuid'] = 'fake-uuid2'

        self.mox.StubOutWithMock(self.driver, '_schedule_instance')
        self.mox.StubOutWithMock(self.driver, 'create_instance_db_entry')
        self.mox.StubOutWithMock(driver, 'cast_to_compute_host')
        self.mox.StubOutWithMock(driver, 'encode_instance')

        # instance 1
        self.driver._schedule_instance(ctxt, instance_opts,
                *fake_args, **fake_kwargs).AndReturn('host1')
        self.driver.create_instance_db_entry(ctxt,
                request_spec).WithSideEffects(_add_uuid1).AndReturn(
                instance1)
        driver.cast_to_compute_host(ctxt, 'host1', 'run_instance',
                instance_uuid=instance1['uuid'], **fake_kwargs)
        driver.encode_instance(instance1).AndReturn(instance1_encoded)
        # instance 2
        self.driver._schedule_instance(ctxt, instance_opts,
                *fake_args, **fake_kwargs).AndReturn('host2')
        self.driver.create_instance_db_entry(ctxt,
                request_spec).WithSideEffects(_add_uuid2).AndReturn(
                instance2)
        driver.cast_to_compute_host(ctxt, 'host2', 'run_instance',
                instance_uuid=instance2['uuid'], **fake_kwargs)
        driver.encode_instance(instance2).AndReturn(instance2_encoded)

        self.mox.ReplayAll()
        result = self.driver.schedule_run_instance(ctxt, request_spec,
                *fake_args, **fake_kwargs)
        expected = [instance1_encoded, instance2_encoded]
        self.assertEqual(result, expected)

    def test_basic_schedule_start_instance(self):

        ctxt = context.RequestContext('fake', 'fake', False)
        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}
        instance_opts = 'fake_instance_opts'
        request_spec = {'num_instances': 2,
                        'instance_properties': instance_opts}
        instance1 = {'id': 1}

        self.mox.StubOutWithMock(db, 'instance_get')
        self.mox.StubOutWithMock(self.driver, '_schedule_instance')
        self.mox.StubOutWithMock(driver, 'cast_to_compute_host')

        # instance 1
        db.instance_get(ctxt, instance1['id']).AndReturn(instance1)
        self.driver._schedule_instance(ctxt, instance1,
                *fake_args, **fake_kwargs).AndReturn('host1')
        driver.cast_to_compute_host(ctxt, 'host1', 'start_instance',
                instance_id=instance1['id'], **fake_kwargs)

        self.mox.ReplayAll()
        self.driver.schedule_start_instance(ctxt, instance1['id'],
                *fake_args, **fake_kwargs)

    def test_schedule_instance_avail_zone_no_host(self):
        self.flags(max_cores=10)

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        compute1 = _create_compute_node(service1)
        service2 = _create_service(id=2, host='host2',
                availability_zone='zone2')
        compute2 = _create_compute_node(service2, id=2)

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn(
                [(service1, 0), (service2, 0)])
        # Should have picked service2/host2 (zone matches)
        utils.service_is_up(service2).AndReturn(True)

        self.mox.ReplayAll()
        instance_opts = dict(availability_zone='zone2',
                vcpus=1, image_ref='fake')
        result = self.driver._schedule_instance(ctxt, instance_opts)
        self.assertEqual(result, 'host2')

    def test_schedule_instance_avail_zone_no_host_one_host_down(self):
        self.flags(max_cores=10)

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        compute1 = _create_compute_node(service1)
        service2 = _create_service(id=2, host='host2')
        compute2 = _create_compute_node(service2, id=2)

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn(
                [(service1, 0), (service2, 0)])
        utils.service_is_up(service1).AndReturn(False)
        utils.service_is_up(service2).AndReturn(True)

        self.mox.ReplayAll()
        instance_opts = dict(availability_zone='zone1',
                vcpus=1, image_ref='fake')
        result = self.driver._schedule_instance(ctxt, instance_opts)
        self.assertEqual(result, 'host2')

    def test_schedule_instance_avail_zone_no_host_all_hosts_down(self):
        self.flags(max_cores=10)

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        compute1 = _create_compute_node(service1)
        service2 = _create_service(id=2, host='host2')
        compute2 = _create_compute_node(service2, id=2)

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn(
                [(service1, 0), (service2, 0)])
        utils.service_is_up(service1).AndReturn(False)
        utils.service_is_up(service2).AndReturn(False)

        self.mox.ReplayAll()
        instance_opts = dict(availability_zone='zone1',
                vcpus=1, image_ref='fake')
        self.assertRaises(exception.NoValidHost,
                self.driver._schedule_instance, ctxt, instance_opts)

    def test_schedule_instance_avail_zone_with_host_not_admin(self):
        self.flags(max_cores=10)

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        compute1 = _create_compute_node(service1)
        service2 = _create_service(id=2, host='host2')
        compute2 = _create_compute_node(service2, id=2)

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn(
                [(service1, 0), (service2, 0)])
        utils.service_is_up(service1).AndReturn(True)

        self.mox.ReplayAll()
        instance_opts = dict(availability_zone='zone1:host2',
                vcpus=1, image_ref='fake')
        result = self.driver._schedule_instance(ctxt, instance_opts)
        # Non-admin should ignore host part of availability_zone
        self.assertEqual(result, 'host1')

    def test_schedule_instance_avail_zone_with_host_admin(self):
        self.flags(max_cores=10)

        ctxt = context.RequestContext('fake', 'fake', True)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_by_args')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_by_args(ctxt_elevated, 'host2',
                'nova-compute').AndReturn(service1)
        utils.service_is_up(service1).AndReturn(True)

        self.mox.ReplayAll()
        instance_opts = dict(availability_zone='zone1:host2',
                vcpus=1, image_ref='fake')
        result = self.driver._schedule_instance(ctxt, instance_opts)
        self.assertEqual(result, 'host2')

    def test_schedule_instance_avail_zone_with_host_down(self):
        self.flags(max_cores=10)

        ctxt = context.RequestContext('fake', 'fake', True)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_by_args')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_by_args(ctxt_elevated, 'host2',
                'nova-compute').AndReturn(service1)
        utils.service_is_up(service1).AndReturn(False)

        self.mox.ReplayAll()
        instance_opts = dict(availability_zone='zone1:host2',
                vcpus=1, image_ref='fake')
        self.assertRaises(exception.WillNotSchedule,
                self.driver._schedule_instance, ctxt, instance_opts)

    def test_schedule_instance_last_host_gets_instance(self):
        """Ensures the host with less cores gets the next one when
        isolated image doesn't match because the DB query returns list
        of services sorted by cores with smallest cores first.
        """
        self.flags(max_cores=10, isolated_images=['hotmess'],
                isolated_hosts=['host2'])

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        compute1 = _create_compute_node(service1)
        service2 = _create_service(id=2, host='host2')
        compute2 = _create_compute_node(service2, id=2)

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn(
                [(service1, 2), (service2, 5)])
        utils.service_is_up(service1).AndReturn(True)

        self.mox.ReplayAll()
        instance_opts = dict(vcpus=1, image_ref='no-match')
        result = self.driver._schedule_instance(ctxt, instance_opts)
        self.assertEqual(result, 'host1')

    def test_schedule_instance_no_hosts(self):
        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn([])

        self.mox.ReplayAll()
        instance_opts = dict(vcpus=1, image_ref='fake')
        self.assertRaises(exception.NoValidHost,
                self.driver._schedule_instance, ctxt, instance_opts)

    def test_schedule_instance_too_many_cores(self):
        self.flags(max_cores=10)

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        compute1 = _create_compute_node(service1)
        service2 = _create_service(id=2, host='host2')
        compute2 = _create_compute_node(service2, id=2)

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn(
                [(service1, 10), (service2, 10)])

        self.mox.ReplayAll()
        instance_opts = dict(vcpus=1, image_ref='fake')
        self.assertRaises(exception.NoValidHost,
                self.driver._schedule_instance, ctxt, instance_opts)

    def test_schedule_instance_isolation_of_image_matches(self):
        """If image_href matches isolated image, make sure isolated
        host gets the instance despite another host having less cores.
        """
        self.flags(max_cores=11)
        self.flags(isolated_images=['hotmess'], isolated_hosts=['host2'])

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        compute1 = _create_compute_node(service1)
        service2 = _create_service(id=2, host='host2')
        compute2 = _create_compute_node(service2, id=2)

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn(
                [(service1, 2), (service2, 9)])
        utils.service_is_up(service2).AndReturn(True)

        self.mox.ReplayAll()
        instance_opts = dict(vcpus=2, image_ref='hotmess')
        result = self.driver._schedule_instance(ctxt, instance_opts)
        self.assertEqual(result, 'host2')

    def test_schedule_instance_isolated_image_checks_cores(self):
        """There should be checking of cores when isolated image matches
        and FLAGS.skip_isolated_core_check is False.
        """
        self.flags(max_cores=10, skip_isolated_core_check=False)
        self.flags(isolated_images=['hotmess'], isolated_hosts=['host2'])

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        compute1 = _create_compute_node(service1)
        service2 = _create_service(id=2, host='host2')
        compute2 = _create_compute_node(service2, id=2)

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn(
                [(service1, 2), (service2, 9)])

        self.mox.ReplayAll()
        instance_opts = dict(vcpus=2, image_ref='hotmess')
        self.assertRaises(exception.NoValidHost,
                self.driver._schedule_instance, ctxt, instance_opts)

    def test_schedule_instance_isolated_image_doesnt_check_cores(self):
        """No checking of cores when isolated image matches and
        FLAGS.skip_isolated_core_check is True.
        """
        self.flags(max_cores=10, skip_isolated_core_check=True)
        self.flags(isolated_images=['hotmess'], isolated_hosts=['host2'])

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        compute1 = _create_compute_node(service1)
        service2 = _create_service(id=2, host='host2')
        compute2 = _create_compute_node(service2, id=2)

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'service_get_all_compute_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.service_get_all_compute_sorted(ctxt_elevated).AndReturn(
                [(service1, 2), (service2, 9)])
        utils.service_is_up(service2).AndReturn(True)

        self.mox.ReplayAll()
        instance_opts = dict(vcpus=2, image_ref='hotmess')
        result = self.driver._schedule_instance(ctxt, instance_opts)
        self.assertEqual(result, 'host2')

    def test_create_volume_last_host_gets_it(self):
        """Ensures the host with least gigabytes gets the next one,
        because the DB query returns list of services sorted by gigabytes
        with smallest gigabytes first.
        """
        self.flags(max_gigabytes=100)

        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}
        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        service2 = _create_service(id=2, host='host2')
        volume = _create_volume()

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'volume_get')
        self.mox.StubOutWithMock(db, 'service_get_all_volume_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')
        self.mox.StubOutWithMock(driver, 'cast_to_volume_host')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.volume_get(ctxt, volume['id']).AndReturn(volume)
        db.service_get_all_volume_sorted(ctxt_elevated).AndReturn(
                [(service1, 20), (service2, 50)])
        utils.service_is_up(service1).AndReturn(True)
        driver.cast_to_volume_host(ctxt, 'host1', 'create_volume',
                volume_id=volume['id'], **fake_kwargs)

        self.mox.ReplayAll()
        self.driver.schedule_create_volume(ctxt,
                volume['id'], *fake_args, **fake_kwargs)

    def test_create_volume_no_hosts(self):
        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}
        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        volume = _create_volume()

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'volume_get')
        self.mox.StubOutWithMock(db, 'service_get_all_volume_sorted')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.volume_get(ctxt, volume['id']).AndReturn(volume)
        db.service_get_all_volume_sorted(ctxt_elevated).AndReturn([])

        self.mox.ReplayAll()
        self.assertRaises(exception.NoValidHost,
                self.driver.schedule_create_volume, ctxt,
                volume_id=volume['id'], **fake_kwargs)

    def test_create_volume_too_many_gigs(self):
        self.flags(max_gigabytes=100)

        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        service2 = _create_service(id=2, host='host2')
        volume = _create_volume()

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'volume_get')
        self.mox.StubOutWithMock(db, 'service_get_all_volume_sorted')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.volume_get(ctxt, volume['id']).AndReturn(volume)
        db.service_get_all_volume_sorted(ctxt_elevated).AndReturn(
                [(service1, 81), (service2, 90)])

        self.mox.ReplayAll()
        self.assertRaises(exception.NoValidHost,
                self.driver.schedule_create_volume, ctxt,
                volume_id=volume['id'], **fake_kwargs)

    def test_create_volume_avail_zone_no_host(self):
        self.flags(max_gigabytes=100)

        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        service2 = _create_service(id=2, host='host2',
                availability_zone='zone2')
        volume = _create_volume(**{'availability_zone': 'zone2'})

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'volume_get')
        self.mox.StubOutWithMock(db, 'service_get_all_volume_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')
        self.mox.StubOutWithMock(driver, 'cast_to_volume_host')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.volume_get(ctxt, volume['id']).AndReturn(volume)
        db.service_get_all_volume_sorted(ctxt_elevated).AndReturn(
                [(service1, 0), (service2, 0)])
        # Should have picked service2/host2 (zone matches)
        utils.service_is_up(service2).AndReturn(True)
        driver.cast_to_volume_host(ctxt, 'host2', 'create_volume',
                volume_id=volume['id'], **fake_kwargs)

        self.mox.ReplayAll()
        self.driver.schedule_create_volume(ctxt,
                volume['id'], *fake_args, **fake_kwargs)

    def test_create_volume_avail_zone_no_host_one_host_down(self):
        self.flags(max_gigabytes=100)

        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        service2 = _create_service(id=2, host='host2')
        volume = _create_volume(**{'availability_zone': 'zone1'})

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'volume_get')
        self.mox.StubOutWithMock(db, 'service_get_all_volume_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')
        self.mox.StubOutWithMock(driver, 'cast_to_volume_host')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.volume_get(ctxt, volume['id']).AndReturn(volume)
        db.service_get_all_volume_sorted(ctxt_elevated).AndReturn(
                [(service1, 0), (service2, 0)])
        utils.service_is_up(service1).AndReturn(False)
        utils.service_is_up(service2).AndReturn(True)
        driver.cast_to_volume_host(ctxt, 'host2', 'create_volume',
                volume_id=volume['id'], **fake_kwargs)

        self.mox.ReplayAll()
        self.driver.schedule_create_volume(ctxt,
                volume['id'], *fake_args, **fake_kwargs)

    def test_create_volume_avail_zone_no_host_all_hosts_down(self):
        self.flags(max_gigabytes=100)

        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        service2 = _create_service(id=2, host='host2')
        volume = _create_volume(**{'availability_zone': 'zone1'})

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'volume_get')
        self.mox.StubOutWithMock(db, 'service_get_all_volume_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.volume_get(ctxt, volume['id']).AndReturn(volume)
        db.service_get_all_volume_sorted(ctxt_elevated).AndReturn(
                [(service1, 0), (service2, 0)])
        utils.service_is_up(service1).AndReturn(False)
        utils.service_is_up(service2).AndReturn(False)

        self.mox.ReplayAll()
        self.assertRaises(exception.NoValidHost,
            self.driver.schedule_create_volume, ctxt, volume['id'],
            *fake_args, **fake_kwargs)

    def test_create_volume_avail_zone_with_host_not_admin(self):
        self.flags(max_gigabytes=100)

        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}

        ctxt = context.RequestContext('fake', 'fake', False)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        service2 = _create_service(id=2, host='host2')
        volume = _create_volume(**{'availability_zone': 'zone1:host2'})

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'volume_get')
        self.mox.StubOutWithMock(db, 'service_get_all_volume_sorted')
        self.mox.StubOutWithMock(utils, 'service_is_up')
        self.mox.StubOutWithMock(driver, 'cast_to_volume_host')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.volume_get(ctxt, volume['id']).AndReturn(volume)
        db.service_get_all_volume_sorted(ctxt_elevated).AndReturn(
                [(service1, 0), (service2, 0)])
        utils.service_is_up(service1).AndReturn(True)
        # Non-admin should ignore host part of availability_zone
        driver.cast_to_volume_host(ctxt, 'host1', 'create_volume',
                volume_id=volume['id'], **fake_kwargs)

        self.mox.ReplayAll()
        self.driver.schedule_create_volume(ctxt,
                volume['id'], *fake_args, **fake_kwargs)

    def test_create_volume_avail_zone_with_host_admin(self):
        self.flags(max_gigabytes=100)

        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}

        ctxt = context.RequestContext('fake', 'fake', True)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        service2 = _create_service(id=2, host='host2')
        volume = _create_volume(**{'availability_zone': 'zone1:host2'})

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'volume_get')
        self.mox.StubOutWithMock(db, 'service_get_by_args')
        self.mox.StubOutWithMock(utils, 'service_is_up')
        self.mox.StubOutWithMock(driver, 'cast_to_volume_host')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.volume_get(ctxt, volume['id']).AndReturn(volume)
        db.service_get_by_args(ctxt_elevated, 'host2',
                'nova-volume').AndReturn(service2)
        utils.service_is_up(service2).AndReturn(True)
        driver.cast_to_volume_host(ctxt, 'host2', 'create_volume',
                volume_id=volume['id'], **fake_kwargs)

        self.mox.ReplayAll()
        self.driver.schedule_create_volume(ctxt,
                volume['id'], *fake_args, **fake_kwargs)

    def test_create_volume_avail_zone_with_host_down(self):
        self.flags(max_gigabytes=100)

        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}

        ctxt = context.RequestContext('fake', 'fake', True)
        ctxt_elevated = 'fake-context-elevated'

        service1 = _create_service()
        service2 = _create_service(id=2, host='host2')
        volume = _create_volume(**{'availability_zone': 'zone1:host2'})

        self.mox.StubOutWithMock(ctxt, 'elevated')
        self.mox.StubOutWithMock(db, 'volume_get')
        self.mox.StubOutWithMock(db, 'service_get_by_args')
        self.mox.StubOutWithMock(utils, 'service_is_up')
        self.mox.StubOutWithMock(driver, 'cast_to_volume_host')

        ctxt.elevated().AndReturn(ctxt_elevated)
        db.volume_get(ctxt, volume['id']).AndReturn(volume)
        db.service_get_by_args(ctxt_elevated, 'host2',
                'nova-volume').AndReturn(service2)
        utils.service_is_up(service2).AndReturn(False)

        self.mox.ReplayAll()
        self.assertRaises(exception.WillNotSchedule,
            self.driver.schedule_create_volume, ctxt, volume['id'],
            *fake_args, **fake_kwargs)
