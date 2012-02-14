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

"""Compute API that proxies via Zones Service"""

import re

from nova.compute import api as compute_api
from nova.compute import vm_states
from nova.compute import task_states
from nova import exception
from nova import flags
from nova import log as logging
from nova import utils
from nova.zones import api as zones_api

FLAGS = flags.FLAGS
LOG = logging.getLogger('nova.compute.zones_api')


check_instance_state = compute_api.check_instance_state
wrap_check_policy = compute_api.wrap_check_policy


class ComputeZonesAPI(compute_api.API):

    def cast_to_zones(self, context, instance, method, *args, **kwargs):
        instance_uuid = instance['uuid']
        zone_name = instance['zone_name']
        if not zone_name:
            raise exception.InstanceUnknownZone(instance_id=instance_uuid)
        zones_api.cast_service_api_method(context, zone_name, 'compute',
                method, instance_uuid, *args, **kwargs)

    def call_to_zones(self, context, instance, method, *args, **kwargs):
        instance_uuid = instance['uuid']
        zone_name = instance['zone_name']
        if not zone_name:
            raise exception.InstanceUnknownZone(instance_id=instance_uuid)
        return zones_api.call_service_api_method(context, zone_name,
                'compute', method, instance_uuid, *args, **kwargs)

    def _cast_or_call_compute_message(self, *args, **kwargs):
        """In some cases we might super() and we don't want the parent
        class to try to send any messages to compute directly.
        """
        return

    def _check_requested_networks(self, context, requested_networks):
        """Override compute API's checking of this.  It'll happen in
        child zone
        """
        return

    def _validate_image_href(self, context, image_href):
        """Override compute API's checking of this.  It'll happen in
        child zone
        """
        return

    def _create_image(self, context, instance, name, image_type,
            backup_type=None, rotation=None, extra_properties=None):
        if backup_type:
            return self.call_to_zones(context, instance, 'backup',
                    name, backup_type, rotation,
                    extra_properties=extra_properties)
        else:
            return self.call_to_zones(context, instance, 'snapshot',
                    name, extra_properties=extra_properties)

    def _run_instance_rpc_method(self, context, topic, message):
        """Proxy run_instance rpc call to zones instead of scheduler"""
        args = message['args']
        zones_api.schedule_run_instance(context, **args)

    def _schedule_run_instance(self, rpc_method, *args, **kwargs):
        """Override default behavior and send this to zones service by
        passing in our own rpc method
        """
        super(ComputeZonesAPI, self)._schedule_run_instance(
                self._run_instance_rpc_method, *args, **kwargs)

    def create(self, context, instance_type,
               image_href, kernel_id=None, ramdisk_id=None,
               min_count=None, max_count=None,
               display_name=None, display_description=None,
               key_name=None, key_data=None, security_group=None,
               availability_zone=None, user_data=None, metadata=None,
               injected_files=None, admin_password=None,
               block_device_mapping=None, access_ip_v4=None,
               access_ip_v6=None, requested_networks=None, config_drive=None,
               auto_disk_config=None, scheduler_hints=None):
        """
        Provision instances, sending instance information to the
        scheduler.  The scheduler will determine where the instance(s)
        go and will handle creating the DB entries.

        Returns a tuple of (instances, reservation_id) where instances
        could be 'None' or a list of instance dicts depending on if
        we waited for information from the scheduler or not.
        """

        self._check_create_policies(context, availability_zone,
                                requested_networks, block_device_mapping)

        refs_ret = []

        if not min_count:
            min_count = 1
        if not max_count:
            max_count = 1

        reservation_id = utils.generate_uid('r')

        for x in xrange(min_count):
            (refs, resv_id) = self._create_instance(
                context, instance_type,
                image_href, kernel_id, ramdisk_id,
                1, 1,
                display_name, display_description,
                key_name, key_data, security_group,
                availability_zone, user_data, metadata,
                injected_files, admin_password,
                access_ip_v4, access_ip_v6,
                requested_networks, config_drive,
                block_device_mapping, auto_disk_config,
                reservation_id=reservation_id,
                create_instance_here=True, scheduler_hints=scheduler_hints)
            refs_ret.extend(refs)
        return (refs_ret, reservation_id)

    def update(self, context, instance, **kwargs):
        """Update an instance."""
        rv = super(ComputeZonesAPI, self).update(context,
                instance, **kwargs)
        # We need to skip vm_state/task_state updates... those will
        # happen when via a a cast_to_zones for running a different
        # compute api method
        kwargs_copy = kwargs.copy()
        kwargs_copy.pop('vm_state', None)
        kwargs_copy.pop('task_state', None)
        if kwargs_copy:
            try:
                self.cast_to_zones(context, instance, 'update',
                        **kwargs_copy)
            except exception.InstanceUnknownZone:
                pass
        return rv

    def soft_delete(self, context, instance):
        """Terminate an instance."""
        super(ComputeZonesAPI, self).soft_delete(context, instance)
        self.cast_to_zones(context, instance, 'soft_delete')

    def delete(self, context, instance):
        """Terminate an instance."""
        super(ComputeZonesAPI, self).delete(context, instance)
        self.cast_to_zones(context, instance, 'delete')

    def restore(self, context, instance):
        """Restore a previously deleted (but not reclaimed) instance."""
        super(ComputeZonesAPI, self).restore(context, instance)
        self.cast_to_zones(context, instance, 'restore')

    def force_delete(self, context, instance):
        """Force delete a previously deleted (but not reclaimed) instance."""
        super(ComputeZonesAPI, self).force_delete(context, instance)
        self.cast_to_zones(context, instance, 'force_delete')

    def stop(self, context, instance, do_cast=True):
        """Stop an instance."""
        super(ComputeZonesAPI, self).stop(context, instance)
        if do_cast:
            self.cast_to_zones(context, instance, 'stop', do_cast=True)
        else:
            return self.call_to_zones(context, instance, 'stop',
                    do_cast=False)

    def start(self, context, instance):
        """Start an instance."""
        super(ComputeZonesAPI, self).start(context, instance)
        self.cast_to_zones(context, instance, 'start')

    def reboot(self, context, instance, *args, **kwargs):
        """Reboot the given instance."""
        super(ComputeZonesAPI, self).reboot(context, instance,
                *args, **kwargs)
        self.cast_to_zones(context, instance, 'reboot', *args,
                **kwargs)

    def rebuild(self, context, instance, *args, **kwargs):
        """Rebuild the given instance with the provided attributes."""
        super(ComputeZonesAPI, self).rebuild(context, instance, *args,
                **kwargs)
        self.cast_to_zones(context, instance, 'rebuild', *args, **kwargs)

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF],
                          task_state=[task_states.RESIZE_VERIFY])
    def revert_resize(self, context, instance):
        """Reverts a resize, deleting the 'new' instance in the process."""
        self.cast_to_zones(context, instance, 'revert_resize')

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF],
                          task_state=[task_states.RESIZE_VERIFY])
    def confirm_resize(self, context, instance):
        """Confirms a migration/resize and deletes the 'old' instance."""
        self.cast_to_zones(context, instance, 'confirm_resize')

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF],
                          task_state=[None])
    def resize(self, context, instance, flavor_id=None):
        """Resize (ie, migrate) a running instance.

        If flavor_id is None, the process is considered a migration, keeping
        the original flavor_id. If flavor_id is not None, the instance should
        be migrated to a new host and resized to the new flavor_id.
        """
        # FIXME(comstud): pass new instance_type object down to a method
        # that'll unfold it
        self.cast_to_zones(context, instance, 'resize', flavor_id=flavor_id)

    def add_fixed_ip(self, context, instance, *args, **kwargs):
        """Add fixed_ip from specified network to given instance."""
        super(ComputeZonesAPI, self).add_fixed_ip(context, instance,
                *args, **kwargs)
        self.cast_to_zones(context, instance, 'add_fixed_ip',
                *args, **kwargs)

    def remove_fixed_ip(self, context, instance, *args, **kwargs):
        """Remove fixed_ip from specified network to given instance."""
        super(ComputeZonesAPI, self).remove_fixed_ip(context, instance,
                *args, **kwargs)
        self.cast_to_zones(context, instance, 'remove_fixed_ip',
                *args, **kwargs)

    def pause(self, context, instance):
        """Pause the given instance."""
        super(ComputeZonesAPI, self).pause(context, instance)
        self.cast_to_zones(context, instance, 'pause')

    def unpause(self, context, instance):
        """Unpause the given instance."""
        super(ComputeZonesAPI, self).unpause(context, instance)
        self.cast_to_zones(context, instance, 'unpause')

    def set_host_enabled(self, context, host, enabled):
        """Sets the specified host's ability to accept new instances."""
        # FIXME(comstud): Need to know zone from host!
        pass

    def host_power_action(self, context, host, action):
        """Reboots, shuts down or powers up the host."""
        # FIXME(comstud): Need to know zone from host!
        pass

    def get_diagnostics(self, context, instance):
        """Retrieve diagnostics for the given instance."""
        # FIXME(comstud): Cache this?
        # Also: only calling super() to get state/policy checking
        super(ComputeZonesAPI, self).get_diagnostics(context, instance)
        return self.call_to_zones(context, instance, 'get_diagnostics')

    def get_actions(self, context, instance):
        """Retrieve actions for the given instance."""
        # FIXME(comstud): Cache this?
        # Also: only calling super() to get state/policy checking
        super(ComputeZonesAPI, self).get_actions(context, instance)
        return self.call_to_zones(context, instance, 'get_actions')

    def suspend(self, context, instance):
        """Suspend the given instance."""
        super(ComputeZonesAPI, self).suspend(context, instance)
        self.cast_to_zones(context, instance, 'suspend')

    def resume(self, context, instance):
        """Resume the given instance."""
        super(ComputeZonesAPI, self).resume(context, instance)
        self.cast_to_zones(context, instance, 'resume')

    def rescue(self, context, instance, rescue_password=None):
        """Rescue the given instance."""
        super(ComputeZonesAPI, self).rescue(context, instance,
                rescue_password=rescue_password)
        self.cast_to_zones(context, instance, 'rescue',
                rescue_password=rescue_password)

    def unrescue(self, context, instance):
        """Unrescue the given instance."""
        super(ComputeZonesAPI, self).unrescue(context, instance)
        self.cast_to_zones(context, instance, 'unrescue')

    def set_admin_password(self, context, instance, password=None):
        """Set the root/admin password for the given instance."""
        super(ComputeZonesAPI, self).set_admin_password(context, instance,
                password=password)
        self.cast_to_zones(context, instance, 'set_admin_password',
                password=password)

    def inject_file(self, context, instance, *args, **kwargs):
        """Write a file to the given instance."""
        super(ComputeZonesAPI, self).inject_file(context, instance, *args,
                **kwargs)
        self.cast_to_zones(context, instance, 'inject_file', *args, **kwargs)

    @wrap_check_policy
    def get_vnc_console(self, context, instance, *args, **kwargs):
        """Get a url to a VNC Console."""
        # NOTE(comstud): This might not need to go through zones?
        return self.call_to_zones(context, instance, 'get_vnc_console',
                *args, **kwargs)

    def get_console_output(self, context, instance, *args, **kwargs):
        """Get console output for an an instance."""
        # NOTE(comstud): Calling super() just to get policy check
        super(ComputeZonesAPI, self).get_console_output(context, instance,
                *args, **kwargs)
        return self.call_to_zones(context, instance, 'get_console_output',
                *args, **kwargs)

    def lock(self, context, instance):
        """Lock the given instance."""
        super(ComputeZonesAPI, self).lock(context, instance)
        self.cast_to_zones(context, instance, 'lock')

    def unlock(self, context, instance):
        """Unlock the given instance."""
        super(ComputeZonesAPI, self).lock(context, instance)
        self.cast_to_zones(context, instance, 'unlock')

    def reset_network(self, context, instance):
        """Reset networking on the instance."""
        super(ComputeZonesAPI, self).reset_network(context, instance)
        self.cast_to_zones(context, instance, 'reset_network')

    def inject_network_info(self, context, instance):
        """Inject network info for the instance."""
        super(ComputeZonesAPI, self).inject_network_info(context, instance)
        self.cast_to_zones(context, instance, 'inject_network_info')

    @wrap_check_policy
    def attach_volume(self, context, instance, volume_id, device):
        """Attach an existing volume to an existing instance."""
        if not re.match("^/dev/x{0,1}[a-z]d[a-z]+$", device):
            raise exception.InvalidDevicePath(path=device)
        self.cast_to_zones(context, instance, 'attach_volume',
                volume_id, device)

    @wrap_check_policy
    def detach_volume(self, context, volume_id):
        """Detach a volume from an instance."""
        # FIXME(comstud): this call should be in volume i think?
        return

    @wrap_check_policy
    def associate_floating_ip(self, context, instance, address):
        """Makes calls to network_api to associate_floating_ip.

        :param address: is a string floating ip address
        """
        self.cast_to_zones(context, instance, 'associate_floating_ip',
                address)

    def delete_instance_metadata(self, context, instance, key):
        """Delete the given metadata item from an instance."""
        super(ComputeZonesAPI, self).delete_instance_metadata(context,
                instance, key)
        self.cast_to_zones(context, instance, 'delete_instance_metadata',
                key)

    @wrap_check_policy
    def update_instance_metadata(self, context, instance,
                                 metadata, delete=False):
        rv = super(ComputeZonesAPI, self).update_instance_metadata(context,
                instance, metadata, delete=delete)
        try:
            self.cast_to_zones(context, instance,
                    'update_instance_metadata',
                    metadata, delete=delete)
        except exception.InstanceUnknownZone:
            pass
        return rv

    def get_instance_faults(self, context, instances):
        """Get all faults for a list of instance uuids."""
        # FIXME(comstud): We'll need to cache these
        return super(ComputeZonesAPI, self).get_instance_faults(context,
                instances)
