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

from nova import flags
from nova import log as logging
from nova import rpc
from nova.zones import common as zones_common

FLAGS = flags.FLAGS
LOG = logging.getLogger('nova.zones.api')


def zone_call(context, zone_name, method, **kwargs):
    routing_message = zones_common.form_routing_message(zone_name,
            'down', method, kwargs, need_response=True)
    return rpc.call(context, FLAGS.zones_topic, routing_message)


def zone_cast(context, zone_name, method, **kwargs):
    routing_message = zones_common.form_routing_message(zone_name,
            'down', method, kwargs)
    rpc.cast(context, FLAGS.zones_topic, routing_message)


def zone_broadcast_up(context, method, **kwargs):
    bcast_message = zones_common.form_broadcast_message('up', method,
            kwargs)
    rpc.cast(context, FLAGS.zones_topic, bcast_message)


def cast_service_api_method(context, zone_name, service_name, method,
        *args, **kwargs):
    """Encapsulate a call to a service API within a routing call"""

    method_info = {'method': method,
                   'method_args': args,
                   'method_kwargs': kwargs}
    zone_cast(context, zone_name, 'run_service_api_method',
            service_name=service_name, method_info=method_info)


def call_service_api_method(context, zone_name, service_name, method,
        *args, **kwargs):
    """Encapsulate a call to a service API within a routing call"""

    method_info = {'method': method,
                   'method_args': args,
                   'method_kwargs': kwargs}
    return zone_call(context, zone_name, 'run_service_api_method',
            service_name=service_name, method_info=method_info)


def schedule_run_instance(context, **kwargs):
    message = {'method': 'schedule_run_instance',
               'args': kwargs}
    rpc.cast(context, FLAGS.zones_topic, message)


def instance_update(context, instance):
    bcast_message = zones_common.form_instance_update_broadcast_message(
            instance)
    rpc.cast(context, FLAGS.zones_topic, bcast_message)


def instance_destroy(context, instance):
    bcast_message = zones_common.form_instance_destroy_broadcast_message(
            instance)
    rpc.cast(context, FLAGS.zones_topic, bcast_message)
