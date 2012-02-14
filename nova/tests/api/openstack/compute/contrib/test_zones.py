# Copyright 2011-2012 OpenStack LLC.
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

import copy

from lxml import etree

from nova.api.openstack.compute.contrib import zones as zones_ext
from nova.api.openstack import xmlutil
from nova import db
from nova import flags
from nova.scheduler import api as scheduler_api
from nova import test
from nova.tests.api.openstack import fakes
from nova.zones import api as zones_api


FLAGS = flags.FLAGS

FAKE_ZONES = [
        dict(id=1, name='zone1', username='bob', is_parent=True,
                weight_scale=1.0, weight_offset=0.0,
                rpc_host='r1.example.org', password='xxxx'),
        dict(id=2, name='zone2', username='alice', is_parent=False,
                weight_scale=1.0, weight_offset=0.0,
                rpc_host='r2.example.org', password='qwerty')]


FAKE_CAPABILITIES = [
        {'cap1': '0,1', 'cap2': '2,3'},
        {'cap3': '4,5', 'cap4': '5,6'}]


def fake_db_zone_get(context, zone_id):
    return FAKE_ZONES[zone_id - 1]


def fake_db_zone_create(context, values):
    zone = dict(id=1)
    zone.update(values)
    return zone


def fake_db_zone_update(context, zone_id, values):
    zone = fake_db_zone_get(context, zone_id)
    zone.update(values)
    return zone


def fake_zones_api_get_all_zone_info(*args):
    zones = copy.deepcopy(FAKE_ZONES)
    del zones[0]['password']
    del zones[1]['password']
    for i, zone in enumerate(zones):
        zone['capabilities'] = FAKE_CAPABILITIES[i]
    return zones


def fake_db_zone_get_all(context):
    return FAKE_ZONES


def fake_sched_api_get_service_capabilities(context):
    return {'service_cap1': (10, 20), 'service_cap2': (30, 40)}


class ZonesTest(test.TestCase):
    def setUp(self):
        super(ZonesTest, self).setUp()
        fakes.stub_out_networking(self.stubs)
        fakes.stub_out_rate_limiting(self.stubs)

        self.stubs.Set(db, 'zone_get', fake_db_zone_get)
        self.stubs.Set(db, 'zone_get_all', fake_db_zone_get_all)
        self.stubs.Set(db, 'zone_update', fake_db_zone_update)
        self.stubs.Set(db, 'zone_create', fake_db_zone_create)
        self.stubs.Set(scheduler_api, 'get_service_capabilities',
                fake_sched_api_get_service_capabilities)
        self.stubs.Set(zones_api, 'get_all_zone_info',
                fake_zones_api_get_all_zone_info)

        self.controller = zones_ext.Controller()

    def test_index(self):
        req = fakes.HTTPRequest.blank('/v2/fake/zones')
        res_dict = self.controller.index(req)

        self.assertEqual(len(res_dict['zones']), 2)
        for i, zone in enumerate(res_dict['zones']):
            self.assertEqual(zone['name'], FAKE_ZONES[i]['name'])
            self.assertEqual(zone['capabilities'], FAKE_CAPABILITIES[i])
            self.assertNotIn('password', zone)

    def test_get_zone_by_id(self):
        req = fakes.HTTPRequest.blank('/v2/fake/zones/1')
        res_dict = self.controller.show(req, 1)
        zone = res_dict['zone']

        self.assertEqual(zone['id'], 1)
        self.assertEqual(zone['rpc_host'], 'r1.example.org')
        self.assertNotIn('password', zone)

    def test_zone_delete(self):
        call_info = {'delete_called': 0}

        def fake_db_zone_delete(context, zone_id):
            self.assertEqual(zone_id, 999)
            call_info['delete_called'] += 1

        self.stubs.Set(db, 'zone_delete', fake_db_zone_delete)

        req = fakes.HTTPRequest.blank('/v2/fake/zones/999')
        self.controller.delete(req, 999)
        self.assertEqual(call_info['delete_called'], 1)

    def test_zone_create(self):
        body = dict(zone=dict(name='meow', username='fred',
                        password='fubar', rpc_host='r3.example.org'))

        req = fakes.HTTPRequest.blank('/v2/fake/zones')
        res_dict = self.controller.create(req, body)
        zone = res_dict['zone']

        self.assertEqual(zone['id'], 1)
        self.assertEqual(zone['name'], 'meow')
        self.assertEqual(zone['username'], 'fred')
        self.assertEqual(zone['rpc_host'], 'r3.example.org')
        self.assertNotIn('password', zone)

    def test_zone_update(self):
        body = dict(zone=dict(username='zeb', password='sneaky'))

        req = fakes.HTTPRequest.blank('/v2/fake/zones/1')
        res_dict = self.controller.update(req, 1, body)
        zone = res_dict['zone']

        self.assertEqual(zone['id'], 1)
        self.assertEqual(zone['rpc_host'], FAKE_ZONES[0]['rpc_host'])
        self.assertEqual(zone['username'], 'zeb')
        self.assertNotIn('password', zone)

    def test_zone_info(self):
        caps = ['cap1=a;b', 'cap2=c;d']
        self.flags(zone_name='darksecret', zone_capabilities=caps)

        req = fakes.HTTPRequest.blank('/v2/fake/zones/info')
        res_dict = self.controller.info(req)
        zone = res_dict['zone']
        zone_caps = zone['capabilities']

        self.assertEqual(zone['name'], 'darksecret')
        self.assertEqual(zone_caps['cap1'], 'a;b')
        self.assertEqual(zone_caps['cap2'], 'c;d')
        self.assertEqual(zone_caps['service_cap1'], '10,20')
        self.assertEqual(zone_caps['service_cap2'], '30,40')


class TestZonesXMLSerializer(test.TestCase):

    def test_index(self):
        serializer = zones_ext.ZonesTemplate()

        fixture = {'zones': fake_zones_api_get_all_zone_info()}

        output = serializer.serialize(fixture)
        res_tree = etree.XML(output)

        self.assertEqual(res_tree.tag, '{%s}zones' % xmlutil.XMLNS_V10)
        self.assertEqual(len(res_tree), 2)
        self.assertEqual(res_tree[0].tag, '{%s}zone' % xmlutil.XMLNS_V10)
        self.assertEqual(res_tree[1].tag, '{%s}zone' % xmlutil.XMLNS_V10)

    def test_show(self):
        serializer = zones_ext.ZoneTemplate()

        zone = {'id': 1,
                'name': 'darksecret',
                'capabilities': {'cap1': 'a;b',
                                 'cap2': 'c;d'}}
        fixture = {'zone': zone}

        output = serializer.serialize(fixture)
        res_tree = etree.XML(output)

        self.assertEqual(res_tree.tag, '{%s}zone' % xmlutil.XMLNS_V10)
        self.assertEqual(res_tree.get('id'), '1')
        self.assertEqual(res_tree.get('name'), 'darksecret')
        self.assertEqual(res_tree.get('password'), None)

        for child in res_tree:
            self.assertEqual(child.tag,
                    '{%s}capabilities' % xmlutil.XMLNS_V10)
            for elem in child:
                self.assertIn(elem.tag, ('{%s}cap1' % xmlutil.XMLNS_V10,
                                          '{%s}cap2' % xmlutil.XMLNS_V10))
            if elem.tag == '{%s}cap1' % xmlutil.XMLNS_V10:
                self.assertEqual(elem.text, 'a;b')
            elif elem.tag == '{%s}cap2' % xmlutil.XMLNS_V10:
                self.assertEqual(elem.text, 'c;d')
