# Copyright (c) 2012 Openstack, LLC
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
"""
Tests For ZonesScheduler
"""

from nova import flags
from nova import rpc
from nova import test
from nova.tests.zones import fakes
from nova.zones import scheduler as zones_scheduler


FLAGS = flags.FLAGS


class ZonesSchedulerTestCase(test.TestCase):
    """Test case for ZonesScheduler class"""

    def setUp(self):
        super(ZonesSchedulerTestCase, self).setUp()
        self.flags(zone_name='me')
        fakes.init()

        self.zones_manager = fakes.FakeZonesManager(
                _test_case=self,
                _my_name=FLAGS.zone_name,
                zones_driver_cls=fakes.FakeZonesDriver,
                zones_scheduler_cls=zones_scheduler.ZonesScheduler)
        self.scheduler = self.zones_manager.scheduler
        # Fudge our child zones so we only have 'zone2' as a child
        for key in self.zones_manager.child_zones.keys():
            if key != 'zone2':
                del self.zones_manager.child_zones[key]
        # Also nuke our parents so we can see the instance_update
        self.zones_manager.parent_zones = {}

    def test_setup(self):
        self.assertEqual(self.scheduler.manager, self.zones_manager)

    def test_schedule_run_instance_happy_day(self):
        # Tests that requests make it to child zone, instance is created,
        # and an update is returned back upstream
        fake_context = 'fake_context'
        fake_topic = 'compute'
        fake_instance_props = {'uuid': 'fake_uuid',
                               'vm_state': 'fake_vm_state',
                               'other_stuff': 'meow'}
        fake_request_spec = {'instance_properties': fake_instance_props,
                             'image': 'fake_image',
                             'instance_type': 'fake_instance_type',
                             'security_group': 'fake_security_group',
                             'block_device_mapping': 'fake_bd_mapping'}
        fake_filter_properties = 'fake_filter_properties'

        # The grandchild zone is where this should get scheduled
        gc_mgr = fakes.FAKE_ZONE_MANAGERS['grandchild']

        call_info = {'create_called': 0, 'cast_called': 0,
                     'update_called': 0}

        def fake_create_db_entry(context, instance_type, image,
                base_options, security_group, bd_mapping):
            self.assertEqual(context, fake_context)
            self.assertEqual(image, 'fake_image')
            self.assertEqual(instance_type, 'fake_instance_type')
            self.assertEqual(security_group, 'fake_security_group')
            self.assertEqual(bd_mapping, 'fake_bd_mapping')
            call_info['create_called'] += 1
            return fake_instance_props

        def fake_rpc_cast(context, topic, message):
            args = {'topic': fake_topic,
                    'request_spec': fake_request_spec,
                    'filter_properties': fake_filter_properties}
            expected_message = {'method': 'run_instance',
                                'args': args}
            self.assertEqual(context, fake_context)
            self.assertEqual(message, expected_message)
            call_info['cast_called'] += 1

        # Called in top level.. should be pushed up from GC zone
        def fake_instance_update(context, instance_info, routing_path):
            props = fake_instance_props.copy()
            props.pop('other_stuff')
            self.assertEqual(routing_path, 'grandchild.zone2.me')
            self.assertEqual(context, fake_context)
            self.assertEqual(instance_info, props)
            call_info['update_called'] += 1

        self.stubs.Set(gc_mgr.scheduler.compute_api,
                'create_db_entry_for_new_instance',
                fake_create_db_entry)
        self.stubs.Set(rpc, 'cast', fake_rpc_cast)
        self.stubs.Set(self.zones_manager, 'instance_update',
                fake_instance_update)

        self.zones_manager.schedule_run_instance(fake_context,
                topic=fake_topic,
                request_spec=fake_request_spec,
                filter_properties=fake_filter_properties)
        self.assertEqual(call_info['create_called'], 1)
        self.assertEqual(call_info['cast_called'], 1)
        self.assertEqual(call_info['update_called'], 1)
