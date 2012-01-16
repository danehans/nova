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


def route_call_to_zone(context, zone_name, method, **kwargs):
    message = {'method': 'route_call_by_zone_name',
               'args': {'zone_name': zone_name,
                        'method': method,
                        'method_kwargs': kwargs,
                        # not used atm
                        'source_zone': FLAGS.zone_name}}
    rpc.cast(context, FLAGS.zones_topic, message)


def cast_service_api_method(context, zone_name, service_name, method,
        *args, **kwargs):
    """Encapsulate a call to a service API within a routing call"""
    method_info = {'service_name': service_name,
                   'method': method,
                   'method_args': args,
                   'method_kwargs': kwargs}
    route_call_to_zone(context, zone_name, 'call_service_api_method',
            method_info=method_info)


# FIXME(comstud): Make calls work, I guess, for things not cached.
def call_service_api_method(context, zone_name, service_name, method,
        *args, **kwargs):
    """Encapsulate a call to a service API within a routing call"""
    method_info = {'service_name': service_name,
                   'method': method,
                   'method_args': args,
                   'method_kwargs': kwargs}
    route_call_to_zone(context, zone_name, 'call_service_api_method',
            method_info=method_info)


def schedule_run_instance(context, request_spec, admin_password,
        injected_files, requested_networks):
    message = {'method': 'schedule_run_instance',
               'args': {'request_spec': request_spec,
                        'admin_password': admin_password,
                        'injected_files': injected_files,
                        'requested_networks': requested_neworks}}
    rpc.cast(context, FLAGS.zones_topic, message)


def instance_update(context, instance):
    # extra things in case the instance disappears from cache at the top
    update_fields = ['vm_state', 'task_state', 'host', 'project_id',
            'user_id', 'progress', 'image_ref']

    update_info = {}
    for key in update_fields:
        # FIXME(comstud): Only checking this way because of tests for now
        if key in instance:
            update_info[key] = instance[key]

    # FIXME: encode created_at/updated_at
    message = {'method': 'instance_update',
               'args': {'instance_uuid': instance['uuid'],
                        'update_info': update_info,
                        'source_zone': FLAGS.zone_name}}
    rpc.cast(context, FLAGS.zones_topic, message)


def instance_destroy(context, instance):
    # FIXME: encode deleted_at
    message = {'method': 'instance_destroy',
               'args': {'instance_uuid': instance['uuid'],
                        'source_zone': FLAGS.zone_name}}
    rpc.cast(context, FLAGS.zones_topic, message)
