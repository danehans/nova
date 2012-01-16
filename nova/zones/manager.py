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
Zones Service
"""

from nova import context
from nova import db
from nova import flags
from nova import log as logging
from nova import manager
from nova import rpc
from nova import utils

LOG = logging.getLogger('nova.zones.manager')
FLAGS = flags.FLAGS
flags.DEFINE_string('zones_driver',
                    'nova.zones.rpc_driver.ZonesRPCDriver',
                    'Zones driver to use')


class ZonesManager(manager.Manager):
    """Handles zone communication."""

    def __init__(self, zones_driver=None, *args, **kwargs):
        if not zones_driver:
            zones_driver = FLAGS.zones_driver
        self.driver = utils.import_object(zones_driver)
        super(ZonesManager, self).__init__(*args, **kwargs)

    @manager.periodic_task
    def _refresh_zones_from_db(self, context):
        """Poll child zones periodically to get status."""
        self.driver.refresh_zones_from_db(context)

    def direct_route_by_name(context, zone_name, method, method_args,
            **kwargs):
        self.driver.direct_route_by_name(context, zone_name, method,
                method_args, **kwargs)
