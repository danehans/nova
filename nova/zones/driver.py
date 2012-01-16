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

from nova import datetime

from nova.db import base
from nova import flags
from nova import log as logging
from nova import utils

LOG = logging.getLogger('nova.zones.driver')
FLAGS = flags.FLAGS

flags.DECLARE('zone_db_check_interval', 'nova.scheduler.zone_manager')


class BaseZoneInfo(object, zone_name, is_me=False):
    """Holds information ior a particular zone."""
    def __init__(self):
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

    def __init__(self, zone_info_cls=None):
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
