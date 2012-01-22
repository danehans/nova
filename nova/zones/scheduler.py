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
Zones Scheduler
"""

from nova.db import base
from nova import flags
from nova import log as logging
from nova import rpc

LOG = logging.getLogger('nova.zones.driver')
FLAGS = flags.FLAGS


class ZonesScheduler(base.Base):
    """The zones scheduler."""

    def __init__(self, manager):
        super(ZonesScheduler, self).__init__()
        self.manager = manager

    def _create_instance_here(self, context, request_spec):
        instance_values = request_spec['instance_properties']
        instance_values['zone_name'] = FLAGS.zone_name
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
        rv = self.db.instance_update(context, instance['id'], {'uuid': uuid})
        self.manager.instance_update(uuid, rv, FLAGS.zone_name)

    def _get_weighted_zones_for_instance(self, context, request_spec,
            filter_properties):
        """Returns a random selection, or self if no child zones"""
        children = self.manager.get_child_zones()
        if not children:
            # No more children... I must be the only choice.
            return [self.my_zone_info]
        return children[int(random.random() * len(children))]

    def schedule_run_instance(self, context, request_spec,
            filter_properties=None, **kwargs):

        if filter_properties is None:
            filter_properties = {}
        args = {'request_spec': request_spec,
                'filter_properties': filter_properties}
        args.update(kwargs)

        zone_infos = self.scheduler.schedule_run_instance(request_spec,
                filter_properties)
        for zone_info in zone_infos:
            try:
                if zone_info.is_me:
                    # Need to create instance DB entry as scheduler
                    # thinks it's already created... At least how things
                    # currently work.
                    self._create_instance_here(context, request_spec)
                    args['topic'] = FLAGS.compute_topic
                    rpc.cast(context, FLAGS.scheduler_topic, msg)
                else:
                    self.manager.cast_to_method_in_zone(context,
                            zone_info, zone_info.name,
                            'schedule_run_instance', args)
                return
            except Exception:
                LOG.error(_("Couldn't communicate with zone '%s'") %
                        zone_info.name)
                pass
        # FIXME(comstud)
        msg = _("Couldn't communicate with any zones")
        LOG.error(msg)
        raise Exception(msg)
