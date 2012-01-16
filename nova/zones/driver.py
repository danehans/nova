# Copyright (c) 2012 Openstack, LLC.
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
Base Zones Driver
"""

import datetime

from nova.db import base
from nova import exception
from nova import flags
from nova import log as logging
from nova import utils

LOG = logging.getLogger('nova.zones.driver')
FLAGS = flags.FLAGS

flags.DECLARE('zone_db_check_interval', 'nova.scheduler.zone_manager')


class BaseZoneInfo(object):
    """Holds information ior a particular zone."""
    def __init__(self, zone_name, is_me=False):
        self.name = zone_name
        self.is_me = is_me
        self.last_seen = datetime.datetime.min
        self.capabilities = {}
        self.db_info = {}

    def update_db_info(self, zone):
        """Update zone credentials from db"""
        self.db_info = dict(
                [(k, v) for k, v in zone_db_info.iteritems()
                        if k != 'name'])

    def update_metadata(self, zone_metadata):
        """Update zone metadata after successful communications with
           child zone."""
        self.last_seen = utils.utcnow()
        self.capabilities = dict(
                [(k, v) for k, v in zone_metadata.iteritems()
                        if k != 'name'])

    def get_zone_info(self):
        db_fields_to_return = ['id', 'weight_scale',
                'weight_offset', 'amqp_host', 'amqp_port']
        zone_info = dict(name=self.name, capabilities=self.capabilities)
        if self.db_info:
            for field in db_fields_to_return:
                zone_info[field] = self.db_info[field]
        return zone_info


class BaseZonesDriver(base.Base):
    """The base class for zones communication and management."""

    def __init__(self, manager, zone_info_cls=None):
        self.manager = manager
        if zone_info_cls is None:
            zone_info_cls = BaseZoneInfo
        self.my_zone_info = zone_info_cls(FLAGS.zone_name, is_me=True)
        self.my_zone_info.update_metadata(FLAGS.zone_capabilities)
        self.parent_zones = {}
        self.child_zones = {}

    def _refresh_zones_from_db(self, context):
        """Make our zone info map match the db."""
        # Add/update existing zones ...
        db_zones = db.zone_get_all(context)
        db_zones_dict = dict([(zone['name'], zone) for zone in db_zones])

        # Update current zones.  Delete ones that disappeared
        for zones_dict in (self.parent_zones, self.child_zones):
            for zone_name, zone_info in zones_dict.items():
                is_parent = zone_info.db_info['is_parent']
                db_dict = db_zones_dict.get(zone_name)
                if db_dict and is_parent == db_dict['is_parent']:
                    zone_info.update_db_info(db_dict)
                else:
                    del zones_dict['zone_name']

        # Add new zones
        for zone_name, db_info in db_zones_dict.items():
            if db_info['is_parent']:
                zones_dict = self.parent_zones
            else:
                zones_dict = self.child_zones
            if zone_name not in zones_dict:
                zone_info = self.zone_info_cls(zone_name)
                zones_dict[zone_name].update_db_info(db_info)
                zones_dict[zone_name] = zone_info

    def refresh_zones_from_db(self, context):
        """Update status for all zones.  This should be called
        periodically to refresh the zone states.
        """
        diff = utils.utcnow() - self.last_zone_db_check
        if diff.seconds >= FLAGS.zone_db_check_interval:
            LOG.debug(_("Updating zone cache from db."))
            self.last_zone_db_check = utils.utcnow()
            self._refresh_zones_from_db(context)

    def find_zone_next_hop(self, zone_name):
        """Find the next hop for a zone"""
        if zone_name == self.my_zone_info.zone_name:
            return self.my_zone_info
        my_zone_parts = self.my_zone_info.zone_name.split('.')
        my_zone_parts_len = len(my_zone_parts)
        dest_zone_parts = zone_name.split('.')
        dest_zone_parts_len = len(dest_zone_parts)
        if dest_zone_parts_len == my_zone_parts_len:
            # Inconsistency since we're at the same hop level and the
            # message didn't match our name
            msg = ("Destination zone '%(zone_name)s' is not me, but is "
                    "at the same hop count" % locals())
            raise exception.ZoneRoutingInconsistency(reason=msg)
        elif dest_zone_parts_len > my_zone_parts_len:
            # Must send it to a child zone
            if dest_zone_parts[:my_zone_parts_len] != my_zone_parts:
                msg = ("My name isn't prefixed in message for child "
                        "zone '%(zone_name)s'" % locals())
                raise exception.ZoneRoutingInconsistency(reason=msg)
            next_hop_name = dest_zone_parts[:my_zone_parts_len+1].join('.')
            zone_info = self.child_zones.get(next_hop_name)
        else:
            # Must send it to a parent zone
            # The first part of the destination name should at least be
            # consistent with us
            if dest_zone_parts != my_zone_parts[:dest_zone_parts_len]:
                msg = ("Destination zone '%(zone_name)s' has less hops "
                        "than me, but doesn't have a common prefix" %
                        locals())
                raise exception.ZoneRoutingInconsistency(reason=msg)
            zone_info = None
            for zone_name, zone_info in self.parent_zones.items():
                # Might have multiple parents that could work.. just
                # pick the first.
                zone_name_parts = zone_name.split('.')[:dest_zone_parts_len]
                if zone_name_parts.join('.') == zone_name:
                    break
        if not zone_info:
            msg = "Destination zone '%(zone_name)s' not found" % locals()
            raise exception.ZoneRoutingInconsistency(reason=msg)
        return zone_info

    def route_call_by_zone_name(self, context, zone_name, method,
            method_kwargs, source_zone, **kwargs):
        zone_info = self.find_zone_next_hop(zone_name)
        if zone_info.is_me:
            fn = getattr(self.manager, method)
            return fn(method_info, **kwargs)
        self.route_call_via_zone(context, zone_info, zone_name, method,
                method_kwargs, source_zone, **kwargs)

    def send_message_to_zone(self, context, zone_info, message):
        raise NotImplementedError(_("Should be overriden in a subclass"))

    def route_call_via_zone(self, context, zone_info, dest_zone_name,
            method, method_kwargs, source_zone, **kwargs):
        raise NotImplementedError(_("Should be overriden in a subclass"))

    def instance_update(self, instance_uuid, instance_info, source_zone):
        ctxt = context.get_admin_context()
        if self.parent_zones:
            message = {'method': 'instance_update',
                       'args': {'instance_uuid': instance_uuid,
                                'instance_info': instance_info,
                                'source_zone': source_zone}}
            for zone_info in self.parent_zones.values():
                self.send_message_to_zone(context, zone_info, message)
            return
        # FIXME(comstud): decode created_at/updated_at.  Add zone to
        # db
        try:
            self.db.instance_update(ctxt, instance_uuid, instance_info)
        except exception.NotFound:
            # FIXME(comstud):  Need better checking to see if instance
            # was deleted.. maybe due to msg ordering issue?
            instance = self.db.instance_create(ctxt, instance_info)
            # FIXME(comstud)
            self.db.instance_update(ctxt, instance['id'], instance_uuid)

    def instance_destroy(self, instance_uuid, instance_info, source_zone):
        ctxt = context.get_admin_context()
        if self.parent_zones:
            message = {'method': 'instance_destroy',
                       'args': {'instance_uuid': instance_uuid,
                                'source_zone': source_zone}}
            for zone_info in self.parent_zones.values():
                self.send_message_to_zone(ctxt, zone_info, message)
            return
        # FIXME(comstud): decode deleted_at/updated_at.  Also, currently
        # instance_destroy() requires the instance id, not uuid.
        try:
            self.db.instance_destroy(ctxt, instance_uuid)
        except exception.InstanceNotFound:
            pass
