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


class ZonesManager(manager.Manager):
    """Handles zone communication."""

    def __init__(self, zones_driver=None, *args, **kwargs):
        if not zones_driver:
            zones_driver = FLAGS.zones_driver
        driver_cls = utils.import_class(zones_driver)
        self.driver = driver_cls(self)
        self.api_map = {'compute': compute_api,
                        'network': network_api,
                        'volume': volume_api}
        self.driver.refresh_zones_from_db(context)
        super(ZonesManager, self).__init__(*args, **kwargs)

    @manager.periodic_task
    def _refresh_zones_from_db(self, context):
        """Poll child zones periodically to get status."""
        self.driver.refresh_zones_from_db(context)

    def route_call_by_name(context, zone_name, method, method_args,
            **kwargs):
        self.driver.route_call_by_name(context, zone_name, method,
                method_args, **kwargs)

    def call_service_api_method(context, method_info, **kwargs):
        api = self.api_map.get(method_info['service_name'])
        if not api:
            # FIXME(comstud): raise appropriate error
            raise SystemError
        method = getattr(api, method_info['method'], None)
        if not method:
            # FIXME(comstud): raise appropriate error
            raise SystemError
        return method(*method_info['args'], **method_info['kwargs'])
