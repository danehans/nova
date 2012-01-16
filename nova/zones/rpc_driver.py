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

    def __init__(self):
       super(ZonesRPCDriver, self).__init__()

    def route_call_via_zone(context, zone_info, dest_zone_name, method,
            method_kwargs, source_zone, **kwargs):
        param_map = {'username': 'userid',
                     'password': 'password',
                     'amqp_host': 'hostname',
                     'amqp_port': 'port',
                     'amqp_virtual_host': 'virtual_host'}
        rabbit_params = {}
        for source, target in param_map.items():
            rabbit_params[target] = zone_info.db_info[source]
        msg = {'method': 'route_call_by_zone_name',
               'zone_name': dest_zone_name,
               'method_kwargs': method_kwargs,
               'source_zone': source_zone}
        msg.update(kwargs)
        rpc.cast_to_zone(context, rabbit_params, msg)
