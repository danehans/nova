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
Zones RPC Driver
"""

from nova import flags
from nova import log as logging
from nova import rpc
from nova.zones import driver

LOG = logging.getLogger('nova.zones.rpc_driver')
FLAGS = flags.FLAGS


class ZonesRPCDriver(driver.BaseZonesDriver):
    """Handles zone communication via RPC."""

    def __init__(self, manager):
        super(ZonesRPCDriver, self).__init__(manager)

    def _get_server_params_for_zone(self, context, next_hop):
        param_map = {'username': 'username',
                     'password': 'password',
                     'rpc_host': 'hostname',
                     'rpc_port': 'port',
                     'rpc_virtual_host': 'virtual_host'}
        server_params = {}
        for source, target in param_map.items():
            server_params[target] = next_hop.db_info[source]
        return server_params

    def send_message_to_zone(self, context, zone_info, message):
        server_params = self._get_server_params_for_zone(zone_info)
        rpc.cast_to_server(context, server_params, FLAGS.zones_topic,
                message)

    def broadcast_message_to_zone(self, context, zone_info, message):
        server_params = self._get_server_params_for_zone(zone_info)
        rpc.fanout_cast_to_server(context, server_params,
                FLAGS.zones_topic, message)
