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
Tests For Zones API
"""

from nova import flags
from nova import rpc
from nova import test
from nova.zones import api as zones_api
from nova.zones import common as zones_common


FLAGS = flags.FLAGS


class ZonesAPITestCase(test.TestCase):
    """Test case for zones.api interfaces."""

    def setUp(self):
        super(ZonesAPITestCase, self).setUp()

    def test_zone_call(self):
        fake_context = 'fake_context'
        fake_zone_name = 'fake_zone_name'
        fake_method = 'fake_method'
        fake_method_kwargs = {'kwarg1': 10, 'kwarg2': 20}
        fake_response = 'fake_response'
        fake_wrapped_message = 'fake_wrapped_message'

        def fake_form_routing_message(name, direction, method,
                method_kwargs, need_response=False):
            self.assertEqual(name, fake_zone_name)
            self.assertEqual(method, fake_method)
            self.assertEqual(method_kwargs, fake_method_kwargs)
            self.assertEqual(direction, 'down')
            self.assertTrue(need_response)
            return fake_wrapped_message

        def fake_rpc_call(context, topic, message):
            self.assertEqual(context, fake_context)
            self.assertEqual(topic, FLAGS.zones_topic)
            self.assertEqual(message, fake_wrapped_message)
            return fake_response

        self.stubs.Set(zones_common, 'form_routing_message',
                fake_form_routing_message)
        self.stubs.Set(rpc, 'call', fake_rpc_call)

        result = zones_api.zone_call(fake_context,
                fake_zone_name, fake_method,
                **fake_method_kwargs)
        self.assertEqual(result, fake_response)

    def test_zone_cast(self):
        fake_context = 'fake_context'
        fake_zone_name = 'fake_zone_name'
        fake_method = 'fake_method'
        fake_method_kwargs = {'kwarg1': 10, 'kwarg2': 20}
        fake_wrapped_message = 'fake_wrapped_message'

        def fake_form_routing_message(name, direction, method,
                method_kwargs, need_response=False):
            self.assertEqual(name, fake_zone_name)
            self.assertEqual(method, fake_method)
            self.assertEqual(method_kwargs, fake_method_kwargs)
            self.assertEqual(direction, 'down')
            self.assertFalse(need_response)
            return fake_wrapped_message

        call_info = {'cast_called': 0}

        def fake_rpc_cast(context, topic, message):
            self.assertEqual(context, fake_context)
            self.assertEqual(topic, FLAGS.zones_topic)
            self.assertEqual(message, fake_wrapped_message)
            call_info['cast_called'] += 1

        self.stubs.Set(zones_common, 'form_routing_message',
                fake_form_routing_message)
        self.stubs.Set(rpc, 'cast', fake_rpc_cast)

        zones_api.zone_cast(fake_context, fake_zone_name, fake_method,
                **fake_method_kwargs)
        self.assertEqual(call_info['cast_called'], 1)

    def test_zone_broadcast_up(self):
        fake_context = 'fake_context'
        fake_method = 'fake_method'
        fake_method_kwargs = {'kwarg1': 10, 'kwarg2': 20}
        fake_wrapped_message = 'fake_wrapped_message'

        def fake_form_broadcast_message(direction, method, method_kwargs):
            self.assertEqual(method, fake_method)
            self.assertEqual(method_kwargs, fake_method_kwargs)
            self.assertEqual(direction, 'up')
            return fake_wrapped_message

        call_info = {'cast_called': 0}

        def fake_rpc_cast(context, topic, message):
            self.assertEqual(context, fake_context)
            self.assertEqual(topic, FLAGS.zones_topic)
            self.assertEqual(message, fake_wrapped_message)
            call_info['cast_called'] += 1

        self.stubs.Set(zones_common, 'form_broadcast_message',
                fake_form_broadcast_message)
        self.stubs.Set(rpc, 'cast', fake_rpc_cast)

        zones_api.zone_broadcast_up(fake_context, fake_method,
                **fake_method_kwargs)
        self.assertEqual(call_info['cast_called'], 1)

    def test_cast_service_api_method(self):
        fake_context = 'fake_context'
        fake_zone_name = 'fake_zone_name'
        fake_method = 'fake_method'
        fake_service = 'fake_service'
        fake_method_args = (1, 2)
        fake_method_kwargs = {'kwarg1': 10, 'kwarg2': 20}

        expected_method_info = {'method': fake_method,
                                'method_args': fake_method_args,
                                'method_kwargs': fake_method_kwargs}

        call_info = {'cast_called': 0}

        def fake_zone_cast(context, zone_name, method, service_name,
                method_info):
            self.assertEqual(context, fake_context)
            self.assertEqual(zone_name, fake_zone_name)
            self.assertEqual(method, 'run_service_api_method')
            self.assertEqual(service_name, fake_service)
            self.assertEqual(method_info, expected_method_info)
            call_info['cast_called'] += 1

        self.stubs.Set(zones_api, 'zone_cast', fake_zone_cast)

        zones_api.cast_service_api_method(fake_context,
                fake_zone_name, fake_service, fake_method,
                *fake_method_args, **fake_method_kwargs)
        self.assertEqual(call_info['cast_called'], 1)

    def test_call_service_api_method(self):
        fake_context = 'fake_context'
        fake_zone_name = 'fake_zone_name'
        fake_method = 'fake_method'
        fake_service = 'fake_service'
        fake_method_args = (1, 2)
        fake_method_kwargs = {'kwarg1': 10, 'kwarg2': 20}
        fake_response = 'fake_response'

        expected_method_info = {'method': fake_method,
                                'method_args': fake_method_args,
                                'method_kwargs': fake_method_kwargs}

        def fake_zone_call(context, zone_name, method, service_name,
                method_info):
            self.assertEqual(context, fake_context)
            self.assertEqual(zone_name, fake_zone_name)
            self.assertEqual(method, 'run_service_api_method')
            self.assertEqual(service_name, fake_service)
            self.assertEqual(method_info, expected_method_info)
            return fake_response

        self.stubs.Set(zones_api, 'zone_call', fake_zone_call)

        result = zones_api.call_service_api_method(fake_context,
                fake_zone_name, fake_service, fake_method,
                *fake_method_args, **fake_method_kwargs)
        self.assertEqual(result, fake_response)

    def test_schedule_run_instance(self):
        fake_context = 'fake_context'
        fake_kwargs = {'kwarg1': 10, 'kwarg2': 20}

        expected_message = {'method': 'schedule_run_instance',
                            'args': fake_kwargs}

        call_info = {'cast_called': 0}

        def fake_rpc_cast(context, topic, message):
            self.assertEqual(context, fake_context)
            self.assertEqual(topic, FLAGS.zones_topic)
            self.assertEqual(message, expected_message)
            call_info['cast_called'] += 1

        self.stubs.Set(rpc, 'cast', fake_rpc_cast)

        zones_api.schedule_run_instance(fake_context,
                **fake_kwargs)
        self.assertEqual(call_info['cast_called'], 1)

    def test_instance_update(self):
        self.flags(enable_zones=True)
        fake_context = 'fake_context'
        fake_instance = 'fake_instance'
        fake_formed_message = 'fake_formed_message'

        call_info = {'cast_called': 0}

        def fake_form_instance_update_broadcast_message(instance):
            self.assertEqual(instance, fake_instance)
            return fake_formed_message

        def fake_rpc_cast(context, topic, message):
            self.assertEqual(context, fake_context)
            self.assertEqual(topic, FLAGS.zones_topic)
            self.assertEqual(message, fake_formed_message)
            call_info['cast_called'] += 1

        self.stubs.Set(zones_common,
                'form_instance_update_broadcast_message',
                fake_form_instance_update_broadcast_message)
        self.stubs.Set(rpc, 'cast', fake_rpc_cast)

        zones_api.instance_update(fake_context, fake_instance)
        self.assertEqual(call_info['cast_called'], 1)

    def test_instance_destroy(self):
        self.flags(enable_zones=True)
        fake_context = 'fake_context'
        fake_instance = 'fake_instance'
        fake_formed_message = 'fake_formed_message'

        call_info = {'cast_called': 0}

        def fake_form_instance_destroy_broadcast_message(instance):
            self.assertEqual(instance, fake_instance)
            return fake_formed_message

        def fake_rpc_cast(context, topic, message):
            self.assertEqual(context, fake_context)
            self.assertEqual(topic, FLAGS.zones_topic)
            self.assertEqual(message, fake_formed_message)
            call_info['cast_called'] += 1

        self.stubs.Set(zones_common,
                'form_instance_destroy_broadcast_message',
                fake_form_instance_destroy_broadcast_message)
        self.stubs.Set(rpc, 'cast', fake_rpc_cast)

        zones_api.instance_destroy(fake_context, fake_instance)
        self.assertEqual(call_info['cast_called'], 1)
