# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

"""The zones extension."""

from nova.api.openstack import common
from nova.api.openstack.compute import servers
from nova.api.openstack import extensions
from nova.api.openstack import xmlutil
from nova.api.openstack import wsgi
from nova.compute import api as compute
from nova import db
from nova import flags
from nova import log as logging
from nova.scheduler import api as scheduler_api
from nova.zones import api as zones_api


LOG = logging.getLogger("nova.api.openstack.compute.contrib.zones")
FLAGS = flags.FLAGS
authorize = extensions.extension_authorizer('compute', 'zones')


def make_zone(elem):
    elem.set('id')
    elem.set('name')
    elem.set('type')
    elem.set('rpc_host')
    elem.set('rpc_port')

    caps = xmlutil.SubTemplateElement(elem, 'capabilities',
            selector='capabilities')
    cap = xmlutil.SubTemplateElement(caps, xmlutil.Selector(0),
            selector=xmlutil.get_items)
    cap.text = 1


zone_nsmap = {None: wsgi.XMLNS_V10}


class ZoneTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('zone', selector='zone')
        make_zone(root)
        return xmlutil.MasterTemplate(root, 1, nsmap=zone_nsmap)


class ZonesTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('zones')
        elem = xmlutil.SubTemplateElement(root, 'zone', selector='zones')
        make_zone(elem)
        return xmlutil.MasterTemplate(root, 1, nsmap=zone_nsmap)


def _filter_keys(item, keys):
    """
    Filters all model attributes except for keys
    item is a dict

    """
    return dict((k, v) for k, v in item.iteritems() if k in keys)


def _scrub_zone(zone):
    zone_info = _filter_keys(zone, ('id', 'name', 'username', 'rpc_host',
            'rpc_port', 'capabilities'))
    zone_info['type'] = 'parent' if zone['is_parent'] else 'child'
    return zone_info


class Controller(object):
    """Controller for Zone resources."""

    def __init__(self):
        self.compute_api = compute.API()

    @wsgi.serializers(xml=ZonesTemplate)
    def index(self, req):
        """Return all zones in brief"""
        authorize(req.environ['nova.context'])
        # Ask the ZonesManager for the most recent data
        items = zones_api.get_all_zone_info(req.environ['nova.context'])
        items = common.limited(items, req)
        items = [_scrub_zone(item) for item in items]
        return dict(zones=items)

    @wsgi.serializers(xml=ZonesTemplate)
    def detail(self, req):
        """Return all zones in detail"""
        return self.index(req)

    @wsgi.serializers(xml=ZoneTemplate)
    def info(self, req):
        """Return name and capabilities for this zone."""
        context = req.environ['nova.context']
        authorize(context)
        zone_capabs = {}
        service_capabs = scheduler_api.get_service_capabilities(context)
        for item, (min_value, max_value) in service_capabs.iteritems():
            zone_capabs[item] = "%s,%s" % (min_value, max_value)
        my_caps = FLAGS.zone_capabilities
        for cap in my_caps:
            key, value = cap.split('=')
            zone_capabs[key] = value
        zone = {'id': 0,
                'name': FLAGS.zone_name,
                'type': 'self',
                'rpc_host': None,
                'rpc_port': 0,
                'capabilities': zone_capabs}
        return dict(zone=zone)

    @wsgi.serializers(xml=ZoneTemplate)
    def show(self, req, id):
        """Return data about the given zone id"""
        context = req.environ['nova.context']
        authorize(context)
        zone_id = int(id)
        zone = db.zone_get(context, zone_id)
        return dict(zone=_scrub_zone(zone))

    def delete(self, req, id):
        """Delete a child zone entry."""
        context = req.environ['nova.context']
        authorize(context)
        zone_id = int(id)
        db.zone_delete(context, zone_id)
        return {}

    @wsgi.serializers(xml=ZoneTemplate)
    @wsgi.deserializers(xml=servers.CreateDeserializer)
    def create(self, req, body):
        """Create a child zone entry."""
        context = req.environ['nova.context']
        authorize(context)
        if 'type' in body['zone']:
            body['zone']['is_parent'] = body['zone']['type'] == 'parent'
            del body['zone']['type']
        else:
            body['zone']['is_parent'] = False
        zone = db.zone_create(context, body['zone'])
        return dict(zone=_scrub_zone(zone))

    @wsgi.serializers(xml=ZoneTemplate)
    def update(self, req, id, body):
        """Update a child zone entry."""
        context = req.environ['nova.context']
        authorize(context)
        zone_id = int(id)
        zone = db.zone_update(context, zone_id, body["zone"])
        return dict(zone=_scrub_zone(zone))


class Zones(extensions.ExtensionDescriptor):
    """Enables zones-related functionality such as adding child zones,
    listing child zones, getting the capabilities of the local zone,
    and returning build plans to parent zones' schedulers
    """

    name = "Zones"
    alias = "os-zones"
    namespace = "http://docs.openstack.org/compute/ext/zones/api/v1.1"
    updated = "2011-09-21T00:00:00+00:00"

    def get_resources(self):
        #NOTE(bcwaldon): This resource should be prefixed with 'os-'
        coll_actions = {
            'detail': 'GET',
            'info': 'GET',
        }

        res = extensions.ResourceExtension('zones',
                                           Controller(),
                                           collection_actions=coll_actions)
        return [res]
