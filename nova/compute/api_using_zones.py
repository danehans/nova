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

from nova.compute import api as compute_api
from nova import flags
from nova import log as logging
from nova.zones import api as zones_api

FLAGS = flags.FLAGS
LOG = logging.getLogger('nova.compute.api_using_zones')


class APIUsingZones(compute_api.API):

    def cast_to_zones(self, context, instance, method, *args, **kwargs):
        instance_uuid = instance['uuid']
        zone_name = instance['zone']
        if not zone_name:
            # FIXME(comstud)
            raise SystemError
        zones_api.cast_service_api_method(context, zone_name, 'compute',
                method, context, instance_uuid, *args, **kwargs)

    def call_to_zones(self, context, instance, method, *args, **kwargs):
        instance_uuid = instance['uuid']
        zone_name = instance['zone']
        if not zone_name:
            # FIXME(comstud)
            raise SystemError
        zones_api.call_service_api_method(context, zone_name, 'compute',
                method, context, instance_uuid, *args, **kwargs)

    def create(self, context, instance_type,
               image_href, kernel_id=None, ramdisk_id=None,
               min_count=None, max_count=None,
               display_name=None, display_description=None,
               key_name=None, key_data=None, security_group=None,
               availability_zone=None, user_data=None, metadata=None,
               injected_files=None, admin_password=None, zone_blob=None,
               reservation_id=None, block_device_mapping=None,
               access_ip_v4=None, access_ip_v6=None,
               requested_networks=None, config_drive=None,
               auto_disk_config=None):
        """
        Provision instances, sending instance information to the
        scheduler.  The scheduler will determine where the instance(s)
        go and will handle creating the DB entries.

        Returns a tuple of (instances, reservation_id) where instances
        could be 'None' or a list of instance dicts depending on if
        we waited for information from the scheduler or not.
        """
        pass

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF,
                                    vm_states.ERROR])
    def soft_delete(self, context, instance):
        """Terminate an instance."""

        pass
        instance_uuid = instance["uuid"]
        LOG.debug(_("Going to try to soft delete %s"), instance_uuid)


    # NOTE(jerdfelt): The API implies that only ACTIVE and ERROR are
    # allowed but the EC2 API appears to allow from RESCUED and STOPPED
    # too
    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF,
                                    vm_states.ERROR, vm_states.RESCUED,
                                    vm_states.STOPPED])
    def delete(self, context, instance):
        """Terminate an instance."""
        LOG.debug(_("Going to try to terminate %s"), instance["uuid"])
        self.cast_to_zones(context, instance, 'delete')

    @check_instance_state(vm_state=[vm_states.SOFT_DELETE])
    def restore(self, context, instance):
        """Restore a previously deleted (but not reclaimed) instance."""
        self.cast_to_zones(context, instance, 'restore')

    @check_instance_state(vm_state=[vm_states.SOFT_DELETE])
    def force_delete(self, context, instance):
        """Force delete a previously deleted (but not reclaimed) instance."""
        self.cast_to_zones(context, instance, 'force_delte')

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF,
                                    vm_states.RESCUED],
                          task_state=[None, task_states.RESIZE_VERIFY])
    def stop(self, context, instance, do_cast=True):
        """Stop an instance."""
        instance_uuid = instance["uuid"]
        pass

    @check_instance_state(vm_state=[vm_states.STOPPED, vm_states.SHUTOFF])
    def start(self, context, instance):
        """Start an instance."""
        self.cast_to_zones(context, instance, 'start')

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF],
                          task_state=[None, task_states.RESIZE_VERIFY])
    def backup(self, context, instance, name, backup_type, rotation,
               extra_properties=None):
        """Backup the given instance

        :param instance: nova.db.sqlalchemy.models.Instance
        :param name: name of the backup or snapshot
            name = backup_type  # daily backups are called 'daily'
        :param rotation: int representing how many backups to keep around;
            None if rotation shouldn't be used (as in the case of snapshots)
        :param extra_properties: dict of extra image properties to include
        """
        recv_meta = self._create_image(context, instance, name, 'backup',
                            backup_type=backup_type, rotation=rotation,
                            extra_properties=extra_properties)
        return recv_meta

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF],
                          task_state=[None, task_states.RESIZE_VERIFY])
    def snapshot(self, context, instance, name, extra_properties=None):
        """Snapshot the given instance.

        :param instance: nova.db.sqlalchemy.models.Instance
        :param name: name of the backup or snapshot
        :param extra_properties: dict of extra image properties to include

        :returns: A dict containing image metadata
        """
        return self._create_image(context, instance, name, 'snapshot',
                                  extra_properties=extra_properties)

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF,
                                    vm_states.RESCUED],
                          task_state=[None, task_states.RESIZE_VERIFY])
    def reboot(self, context, instance, reboot_type):
        """Reboot the given instance."""
        self.cast_to_zones(context, instance, 'reboot', reboot_type)

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF],
                          task_state=[None, task_states.RESIZE_VERIFY])
    def rebuild(self, context, instance, image_href, admin_password, **kwargs):
        """Rebuild the given instance with the provided attributes."""

        files_to_inject = kwargs.pop('files_to_inject', [])
        self._check_injected_file_quota(context, files_to_inject)
        self.cast_to_zones(context, instance, 'rebuild', image_href,
                admin_password, **kwargs)

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

    def add_fixed_ip(self, context, instance, network_id):
        """Add fixed_ip from specified network to given instance."""
        self.cast_to_zones(context, instance, 'add_fixed_ip',
                network_id=network_id)

    def remove_fixed_ip(self, context, instance, address):
        """Remove fixed_ip from specified network to given instance."""
        self.cast_to_zones(context, instance, 'remove_fixed_ip',
                address)

    def add_network_to_project(self, context, project_id):
        """Force adds a network to the project."""
        self.cast_to_zones(context, instance, 'add_network_to_project',
                project_id)

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF,
                                    vm_states.RESCUED],
                          task_state=[None, task_states.RESIZE_VERIFY])
    def pause(self, context, instance):
        """Pause the given instance."""
        self.cast_to_zones(context, instance, 'pause')

    @check_instance_state(vm_state=[vm_states.PAUSED])
    def unpause(self, context, instance):
        """Unpause the given instance."""
        self.cast_to_zones(context, instance, 'unpause')

    def set_host_enabled(self, context, host, enabled):
        """Sets the specified host's ability to accept new instances."""
        self.cast_to_zones(context, instance, 'set_host_enabled',
                host, enabled)

    def host_power_action(self, context, host, action):
        """Reboots, shuts down or powers up the host."""
        # FIXME(comstud): Need to know zone from host!
        pass

    def get_diagnostics(self, context, instance):
        """Retrieve diagnostics for the given instance."""
        # FIXME(comstud): Cache this?
        self.call_to_zones(context, instance, 'get_diagnostics')

    def get_actions(self, context, instance):
        """Retrieve actions for the given instance."""
        # return self.db.instance_get_actions(context, instance['uuid'])
        # FIXME(comstud): Cache this?
        self.call_to_zones(context, instance, 'get_actions')

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF,
                                    vm_states.RESCUED],
                          task_state=[None, task_states.RESIZE_VERIFY])
    def suspend(self, context, instance):
        """Suspend the given instance."""
        self.cast_to_zones(context, instance, 'suspend')

    @check_instance_state(vm_state=[vm_states.SUSPENDED])
    def resume(self, context, instance):
        """Resume the given instance."""
        self.cast_to_zones(context, instance, 'resume')

    @check_instance_state(vm_state=[vm_states.ACTIVE, vm_states.SHUTOFF,
                                    vm_states.STOPPED],
                          task_state=[None, task_states.RESIZE_VERIFY])
    def rescue(self, context, instance, rescue_password=None):
        """Rescue the given instance."""
        self.cast_to_zones(context, instance, 'rescue')

    @check_instance_state(vm_state=[vm_states.RESCUED])
    def unrescue(self, context, instance):
        """Unrescue the given instance."""
        self.cast_to_zones(context, instance, 'unrescue')

    def set_admin_password(self, context, instance, password=None):
        """Set the root/admin password for the given instance."""
        self.cast_to_zones(context, instance, 'set_admin_password',
                password=password)

    def inject_file(self, context, instance, path, file_contents):
        """Write a file to the given instance."""
        self.cast_to_zones(context, instance, 'inject_file',
                path, file_contents)

    def get_ajax_console(self, context, instance):
        """Get a url to an AJAX Console."""
        self.call_to_zones(context, instance, 'get_ajax_console')

    def get_vnc_console(self, context, instance):
        """Get a url to a VNC Console."""
        self.call_to_zones(context, instance, 'get_vnc_console')

    def get_console_output(self, context, instance, tail_length=None):
        """Get console output for an an instance."""
        self.call_to_zones(context, instance, 'get_console_output',
                tail_length=tail_length)

    def reset_network(self, context, instance):
        """Reset networking on the instance."""
        self.cast_to_zones(context, instance, 'reset_network')

    def inject_network_info(self, context, instance):
        """Inject network info for the instance."""
        self.cast_to_zones(context, instance, 'inject_network_info')

    def attach_volume(self, context, instance, volume_id, device):
        """Attach an existing volume to an existing instance."""
        if not re.match("^/dev/x{0,1}[a-z]d[a-z]+$", device):
            raise exception.ApiError(_("Invalid device specified: %s. "
                                     "Example device: /dev/vdb") % device)
        self.cast_to_zones(context, instance, 'attach_volume',
                volume_id, device)

    def detach_volume(self, context, volume_id):
        """Detach a volume from an instance."""
        # FIXME(comstud): this call should be in volume i think?
        pass

    def associate_floating_ip(self, context, instance, address):
        """Makes calls to network_api to associate_floating_ip.

        :param address: is a string floating ip address
        """
        self.cast_to_zones(context, instance, 'associate_floating_ip',
                address)

    def delete_instance_metadata(self, context, instance, key):
        """Delete the given metadata item from an instance."""
        self.db.instance_metadata_delete(context, instance['id'], key)
        self.cast_to_zones(context, instance, 'delete_instance_metadata',
                key)

    def get_instance_faults(self, context, instances):
        """Get all faults for a list of instance uuids."""
        # FIXME(comstud): Cache this?
        self.call_to_zones(context, instance, 'get_instance_faults')
