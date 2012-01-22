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
Zones Service Manager
"""

from nova.compute import api as compute_api
from nova import context
from nova import db
from nova import flags
from nova import log as logging
from nova import manager
from nova.network import api as network_api
from nova import rpc
from nova import utils
from nova.volume import api as volume_api

LOG = logging.getLogger('nova.zones.manager')
FLAGS = flags.FLAGS
flags.DEFINE_string('zones_driver',
                    'nova.zones.rpc_driver.ZonesRPCDriver',
                    'Zones driver to use')
flags.DEFINE_string('zones_scheduler',
                    'nova.zones.scheduler.ZonesScheduler',
                    'Zones scheduler to use')
flags.DECLARE('zone_db_check_interval', 'nova.scheduler.zone_manager')


class ZoneInfo(object):
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


class ZonesManager(manager.Manager):
    """Handles zone communication."""

    def __init__(self, zones_driver=None, zones_scheduler=None,
            zone_info_cls=None, *args, **kwargs):
        self.api_map = {'compute': compute_api,
                        'network': network_api,
                        'volume': volume_api}
        if not zones_driver:
            zones_driver = FLAGS.zones_driver
        driver_cls = utils.import_class(zones_driver)
        if not zones_scheduler:
            zones_scheduler = FLAGS.zones_scheduler
        scheduler_cls = utils.import_class(zones_scheduler)
        if not zone_info_cls:
            zone_info_cls = ZoneInfo
        self.driver = driver_cls(self)
        self.scheduler = driver_cls(self)
        self.zone_info_cls = zone_info_cls
        self.my_zone_info = zone_info_cls(FLAGS.zone_name, is_me=True)
        self.my_zone_info.update_metadata(FLAGS.zone_capabilities)
        self.parent_zones = {}
        self.child_zones = {}
        self.refresh_zones_from_db(context.get_admin_context())
        super(ZonesManager, self).__init__(*args, **kwargs)

    def __getattr__(self, key):
        """Makes all scheduler_ methods pass through to scheduler"""
        if key.startswith("schedule_"):
            try:
                return getattr(self.scheduler, key)
            except AttributeError, e:
                with utils.save_and_reraise_exception():
                    LOG.error(_("Zones scheduler has no method '%s'") % key)
        raise AttributeError(_("Zones manager has no attribute '%s'") %
                key)

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

    @manager.periodic_task
    def refresh_zones_from_db(self, context):
        """Update status for all zones.  This should be called
        periodically to refresh the zone states.
        """
        diff = utils.utcnow() - self.last_zone_db_check
        if diff.seconds >= FLAGS.zone_db_check_interval:
            LOG.debug(_("Updating zone cache from db."))
            self.last_zone_db_check = utils.utcnow()
            self._refresh_zones_from_db(context)

    def get_child_zones(self):
        """Return list of child zone_infos."""
        return self.child_zones.values()

    def get_parent_zones(self):
        """Return list of parent zone_infos."""
        return self.parent_zones.values()

    def find_zone_next_hop(self, dest_zone_name):
        """Find the next hop for a zone"""
        my_zone_parts = self.my_zone_info.zone_name.split('.')
        my_zone_parts_len = len(my_zone_parts)
        dest_zone_parts = dest_zone_name.split('.')
        dest_zone_parts_len = len(dest_zone_parts)
        if dest_zone_parts_len == my_zone_parts_len:
            # Inconsistency since we're at the same hop level and the
            # message didn't match our name
            msg = ("Destination zone '%(dest_zone_name)s' is not me, but "
                "is at the same hop count" % locals())
            raise exception.ZoneRoutingInconsistency(reason=msg)
        elif dest_zone_parts_len > my_zone_parts_len:
            # Must send it to a child zone
            if dest_zone_parts[:my_zone_parts_len] != my_zone_parts:
                msg = ("My name isn't prefixed in message for child "
                        "zone '%(dest_zone_name)s'" % locals())
                raise exception.ZoneRoutingInconsistency(reason=msg)
            next_hop_name = dest_zone_parts[:my_zone_parts_len + 1].join('.')
            zone_info = self.child_zones.get(next_hop_name)
        else:
            # Must send it to a parent zone
            # The first part of the destination name should at least be
            # consistent with us
            if dest_zone_parts != my_zone_parts[:dest_zone_parts_len]:
                msg = ("Destination zone '%(dest_zone_name)s' has less "
                        "hops than me, but doesn't have a common prefix" %
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
            msg = (_("Destination zone '%(dest_zone_name)s' not found") %
                    locals())
            raise exception.ZoneRoutingInconsistency(reason=msg)
        return zone_info

    def cast_to_method_in_zone_by_name(self, context, dest_zone_name,
            method, method_kwargs, **kwargs):
        if dest_zone_name == self.my_zone_info.zone_name:
            fn = getattr(self, method)
            fn(method_info, **method_kwargs)
            return
        next_hop = self.find_zone_next_hop(dest_zone_name)
        self.cast_to_method_in_zone(context, next_hop, dest_zone_name,
                method, method_kwargs, **kwargs)

    def cast_to_method_in_zone(self, context, next_hop, dest_zone_name,
            method, method_kwargs, **kwargs):
        self.driver.cast_to_method_in_zone(context, next_hop,
                dest_zone_name, method, method_kwargs, **kwargs)

    def call_method_in_zone_by_name(self, context, dest_zone_name,
            method, method_kwargs, **kwargs):
        if dest_zone_name == self.my_zone_info.zone_name:
            fn = getattr(self, method)
            return fn(method_info, **method_kwargs)
        next_hop = self.find_zone_next_hop(dest_zone_name)
        return self.call_method_in_zone(context, next_hop,
                dest_zone_name, method, method_kwargs, **kwargs)

    def call_method_in_zone(self, context, next_hop, dest_zone_name,
            method, method_kwargs, **kwargs):
        return self.driver.call_to_method_in_zone(context, next_hop,
                dest_zone_name, method, method_kwargs, **kwargs)

    def call_service_api_method(self, context, method_info, **kwargs):
        """Caller wants us to call a method in a service API"""
        service_name = method_info['service_name']
        api = self.api_map.get(service_name)
        if not api:
            # FIXME(comstud): raise appropriate error
            raise SystemError
        method = getattr(api, method_info['method'], None)
        if not method:
            # FIXME(comstud): raise appropriate error
            raise SystemError
        # FIXME(comstud): Make more generic later
        args = method_info['args']
        if service_name == 'compute':
            # 1st arg is context
            # 2nd arg is instance_uuid that we need to turn into the
            # instance object.
            instance = db.instance_get_by_uuid(args[0], args[1])
            if len(args) > 2:
                args = (args[0], instance, args[2:])
            else:
                args = (args[0], instance)
        return method(args, **method_info['kwargs'])

    def instance_update(self, context, instance_uuid, instance_info):
        if self.parent_zones:
            message = {'method': 'instance_update',
                       'args': {'instance_uuid': instance_uuid,
                                'instance_info': instance_info}}
            for zone_info in self.get_parent_zones():
                self.send_message_to_zone(context, zone_info, message)
            return
        # FIXME(comstud): decode created_at/updated_at.
        try:
            self.db.instance_update(context, instance_uuid, instance_info)
        except exception.NotFound:
            # FIXME(comstud):  Need better checking to see if instance
            # was deleted.. maybe due to msg ordering issue?
            instance = self.db.instance_create(context, instance_info)
            # FIXME(comstud)
            self.db.instance_update(context, instance['id'], instance_uuid)

    def instance_destroy(self, context, instance_uuid, instance_info):
        if self.parent_zones:
            message = {'method': 'instance_destroy',
                       'args': {'instance_uuid': instance_uuid}}
            for zone_info in self.get_parent_zones():
                self.send_message_to_zone(context, zone_info, message)
            return
        # FIXME(comstud): decode deleted_at/updated_at.  Also, currently
        # instance_destroy() requires the instance id, not uuid.
        try:
            self.db.instance_destroy(context, instance_uuid)
        except exception.InstanceNotFound:
            pass
