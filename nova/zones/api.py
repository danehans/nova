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
Handles all requests relating to zones.
"""

from nova import db
from nova import exception
from nova import flags
from nova import log as logging
from nova import rpc
from nova import utils

FLAGS = flags.FLAGS
LOG = logging.getLogger('nova.zones.api')

flags.DEFINE_bool('enable_rpc_zone_routing',
    False,
    'When True, zone communication occurs via RPC.')


def route_call_to_zone(context, zone_name, method, *args, **kwargs):
    message = {'method': 'route_call_by_name',
               'args': {'zone_name': zone_name,
                        'method': method,
                        'method_args': args,
                        'method_kwargs': kwargs,
                        # not used atm
                        'source_zone': FLAGS.zone_name}}
    rpc.cast(context, FLAGS.zones_topic, message)


def call_service_api_method(context, zone_name, service_name, method,
        *args, **kwargs):
    """Encapsulate a call to a service API within a routing call"""
    method_info = {'service_name': service_name,
                   'method': method,
                   'method_args': args,
                   'method_kwargs': kwargs}
    route_call_to_zone(context, zone_name, 'call_service_api_method',
            method_info=method_info)
