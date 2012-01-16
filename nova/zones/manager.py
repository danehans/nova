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

    def route_call_to_zone(self, context, zone_name, method, method_kwargs,
            source_zone=None, **kwargs):
        """Route a call to a specific zone name.  If the destination
        is our zone, we'll end up getting a call back to the appropriate
        method.
        """
        self.driver.route_call_to_zone(context, zone_name, method,
                method_kwargs, source_zone=source_zone, **kwargs)

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

    def _create_instance_here(self, context, request_spec):
        instance = self.db.create_db_entry_for_new_instance(context,
                request_spec['instance_type'],
                request_spec['image'],
                request_spec['instance_properties'],
                request_spec['security_group'],
                request_spec['block_device_mapping'])
        uuid = request_spec['instance_properties']['uuid']
        # FIXME(comstud): The instance_create() db call generates its
        # own uuid...so we update it here.  Prob should make the db
        # call not generate a uuid if one was passed
        self.db.instance_update(context, {'uuid': uuid})

    def schedule_run_instance(self, request_spec, admin_password,
            injected_files, requested_networks, **kwargs):

        args = {'request_spec': request_spec,
                'admin_password': admin_password,
                'injected_files': injected_files,
                'requested_networks': requested_networks}
        args.update(kwargs)
        msg = {'method': 'schedule_run_instance',
               'args': args}

        zone_info = self.driver.pick_a_zone(request_spec)
        if zone_info.is_me:
            # Need to create instance DB entry as scheduler thinks it's
            # already created... at least how things currently work.
            self._create_instance_here(context, request_spec)
            args['topic'] = FLAGS.compute_topic
            rpc.cast(context, FLAGS.scheduler_topic, msg)
        else:
            args['instance_type'] = instance_type
            args['image'] = image
            args['security_group'] = security_group
            args['block_device_mapping'] = block_device_mapping
            rpc.send_message_to_zone(context, zone_info, msg)
