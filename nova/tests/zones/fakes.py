# Copyright 2011 OpenStack LLC.
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
Fakes For Zones tests.
"""

from nova import context
from nova import flags
from nova.zones import manager

FLAGS = flags.FLAGS

MY_ZONE_NAME = FLAGS.zone_name
FAKE_ZONES = {}

FAKE_ZONES_REFRESH = [dict(id=5, name='zone1', is_parent=False),
                      dict(id=4, name='zone2', is_parent=True),
                      dict(id=3, name='zone3', is_parent=True),
                      dict(id=1, name='zone5', is_parent=False),
                      dict(id=7, name='zone6', is_parent=False)]

TEST_METHOD_EXPECTED_KWARGS = {'kwarg1': 10, 'kwarg2': 20}
TEST_METHOD_EXPECTED_RESULT = 'test_method_expected_result'

FAKE_ZONE_MANAGERS = {}


def init():
    global FAKE_ZONES
    global FAKE_ZONE_MANAGERS
    global FAKE_ZONE_NAME

    # zone_name could have been changed after this module was loaded
    MY_ZONE_NAME = FLAGS.zone_name
    FAKE_ZONES = {
            MY_ZONE_NAME: [dict(id=1, name='zone1', is_parent=True),
                           dict(id=2, name='zone2', is_parent=False),
                           dict(id=3, name='zone3', is_parent=True),
                           dict(id=4, name='zone4', is_parent=False),
                           dict(id=5, name='zone5', is_parent=False)],
            'zone2': [dict(id=1, name=MY_ZONE_NAME, is_parent=True),
                      dict(id=2, name='grandchild', is_parent=False)],
            'grandchild': [dict(id=1, name='zone2', is_parent=True)]}
    FAKE_ZONE_MANAGERS = {}


class FakeZonesScheduler(object):
    def __init__(self, manager, *args, **kwargs):
        pass

    def schedule_test_method(self):
        pass


class FakeZonesDriver(object):
    def __init__(self, manager, *args, **kwargs):
        pass

    def send_message_to_zone(self, context, zone_info, message):
        pass


class FakeZonesManager(manager.ZonesManager):
    def __init__(self, *args, **kwargs):
        self._test_case = kwargs.pop('_test_case')
        _my_name = kwargs.pop('_my_name')
        self._test_call_info = {'test_method': 0, 'send_message': 0}
        super(FakeZonesManager, self).__init__(**kwargs)
        # Now fudge some things for testing
        self.my_zone_info.name = _my_name
        self._refresh_zones_from_db(context.get_admin_context())
        FAKE_ZONE_MANAGERS[_my_name] = self
        for zone in self.child_zones.values() + self.parent_zones.values():
            if zone.name not in FAKE_ZONE_MANAGERS:
                # This will end up stored in FAKE_ZONE_MANAGERS
                FakeZonesManager(*args, _test_case=self._test_case,
                        _my_name=zone.name, **kwargs)

    def _zone_get_all(self, context):
        return FAKE_ZONES.get(self.my_zone_info.name, [])

    def test_method(self, context, routing_path, **kwargs):
        self._test_case.assertEqual(kwargs, TEST_METHOD_EXPECTED_KWARGS)
        self._test_call_info['test_method'] += 1
        self._test_call_info['routing_path'] = routing_path
        return TEST_METHOD_EXPECTED_RESULT

    def send_raw_message_to_zone(self, context, zone, message):
        self._test_call_info['send_message'] += 1
        mgr = FAKE_ZONE_MANAGERS.get(zone.name)
        if mgr:
            method = getattr(mgr, message['method'])
            method(context, **message['args'])


def stubout_zone_get_all_for_refresh(mgr):
    def _zone_get_all(context):
        return FAKE_ZONES_REFRESH

    mgr._test_case.stubs.Set(mgr, '_zone_get_all', _zone_get_all)
    return FAKE_ZONES_REFRESH


def find_a_child_zone(my_name):
    zones = FAKE_ZONES[my_name]
    return [zone for zone in zones if not zone['is_parent']][0]


def find_a_parent_zone(my_name):
    zones = FAKE_ZONES[my_name]
    return [zone for zone in zones if zone['is_parent']][0]
