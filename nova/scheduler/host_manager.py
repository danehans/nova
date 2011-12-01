# Copyright (c) 2011 Openstack, LLC.
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
Keep state on hosts in the current zone
"""

import datetime
import UserDict

from nova import db
from nova import flags
from nova import log as logging
from nova import utils

FLAGS = flags.FLAGS
flags.DEFINE_integer('reserved_host_disk_mb', 0,
        'Amount of disk in MB to reserve for host/dom0')
flags.DEFINE_integer('reserved_host_memory_mb', 512,
        'Amount of memory in MB to reserve for host/dom0')

LOG = logging.getLogger('nova.scheduler.host_state')


class ReadOnlyDict(UserDict.IterableUserDict):
    """A read-only dict."""
    def __init__(self, source=None):
        self.update(source)

    def __setitem__(self, key, item):
        raise TypeError

    def __delitem__(self, key):
        raise TypeError

    def clear(self):
        raise TypeError

    def pop(self, key, *args):
        raise TypeError

    def popitem(self):
        raise TypeError

    def update(self, source=None):
        if source is None:
            return
        elif isinstance(source, UserDict.UserDict):
            self.data = source.data
        elif isinstance(source, type({})):
            self.data = source
        else:
            raise TypeError


class HostState(object):
    """Mutable and immutable information tracked for a host.
    This is an attempt to remove the ad-hoc data structures
    previously used and lock down access.
    """

    def __init__(self, host, caps=None):
        self.host = host

        # Read-only capability dicts
        self.compute = None
        self.volume = None
        self.network = None
        if caps:
            self.compute = ReadOnlyDict(caps.get('compute', None))
            self.volume = ReadOnlyDict(caps.get('volume', None))
            self.network = ReadOnlyDict(caps.get('network', None))

        # Mutable available resources.
        # These will change as resources are virtually "consumed".
        self.free_ram_mb = 0
        self.free_disk_mb = 0

    def update_from_compute_node(self, compute):
        """Update information about a host from its compute_node info."""
        all_disk_mb = compute['local_gb'] * 1024
        all_ram_mb = compute['memory_mb']
        if FLAGS.reserved_host_disk_mb > 0:
            all_disk_mb -= FLAGS.reserved_host_disk_mb
        if FLAGS.reserved_host_memory_mb > 0:
            all_ram_mb -= FLAGS.reserved_host_memory_mb
        self.free_ram_mb = all_ram_mb
        self.free_disk_mb = all_disk_mb

    def update_from_instance(self, instance):
        """Update information about a host from instance info."""
        disk_mb = instance['local_gb'] * 1024
        ram_mb = instance['memory_mb']
        self.free_ram_mb -= ram_mb
        self.free_disk_mb -= disk_mb

    def __repr__(self):
        return "host '%s': free_ram_mb:%s free_disk_mb:%s" % \
                    (self.host, self.free_ram_mb, self.free_disk_mb)


class HostManager(object):
    """Keeps the host states updated."""

    # Can be overriden in a subclass
    host_state_cls = HostState

    def __init__(self):
        self.service_states = {}  # { <host> : { <service> : { cap k : v }}}

    def get_host_list(self):
        """Returns a list of dicts for each host that the Zone Manager
        knows about. Each dict contains the host_name and the service
        for that host.
        """
        all_hosts = self.service_states.keys()
        ret = []
        for host in self.service_states:
            for svc in self.service_states[host]:
                ret.append({"service": svc, "host_name": host})
        return ret

    def get_service_capabilities(self, context):
        """Roll up all the individual host info to generic 'service'
           capabilities. Each capability is aggregated into
           <cap>_min and <cap>_max values."""
        hosts_dict = self.service_states

        # TODO(sandy) - be smarter about fabricating this structure.
        # But it's likely to change once we understand what the Best-Match
        # code will need better.
        combined = {}  # { <service>_<cap> : (min, max), ... }
        stale_host_services = {}  # { host1 : [svc1, svc2], host2 :[svc1]}
        for host, host_dict in hosts_dict.iteritems():
            for service_name, service_dict in host_dict.iteritems():
                if not service_dict.get("enabled", True):
                    # Service is disabled; do no include it
                    continue

                #Check if the service capabilities became stale
                if self.host_service_caps_stale(host, service_name):
                    if host not in stale_host_services:
                        stale_host_services[host] = []  # Adding host key once
                    stale_host_services[host].append(service_name)
                    continue
                for cap, value in service_dict.iteritems():
                    if cap == "timestamp":  # Timestamp is not needed
                        continue
                    key = "%s_%s" % (service_name, cap)
                    min_value, max_value = combined.get(key, (value, value))
                    min_value = min(min_value, value)
                    max_value = max(max_value, value)
                    combined[key] = (min_value, max_value)

        # Delete the expired host services
        self.delete_expired_host_services(stale_host_services)
        return combined

    def _compute_node_get_all(self, context):
        """Broken out for testing."""
        return db.compute_node_get_all(context)

    def _instance_get_all(self, context):
        """Broken out for testing."""
        return db.instance_get_all(context)

    def get_all_host_states(self, context):
        """Returns a dict of all the hosts the HostManager
        knows about. Also, each of the consumable resources in HostState
        are pre-populated and adjusted based on data in the db.

        For example:
        {'192.168.1.100': HostState(), ...}

        Note: this can be very slow with a lot of instances.
        InstanceType table isn't required since a copy is stored
        with the instance (in case the InstanceType changed since the
        instance was created)."""

        # Make a compute node dict with the bare essential metrics.
        compute_nodes = self._compute_node_get_all(context)
        host_state_map = {}
        for compute in compute_nodes:
            service = compute['service']
            if not service:
                logging.warn(_("No service for compute ID %s") % compute['id'])
                continue
            host = service['host']
            caps = self.service_states.get(host, None)
            host_state = self.host_state_cls(host, caps=caps)
            host_state.update_from_compute_node(compute)
            host_state_map[host] = host_state

        # "Consume" resources from the host the instance resides on.
        instances = self._instance_get_all(context)
        for instance in instances:
            host = instance['host']
            if not host:
                continue
            host_state = host_state_map.get(host, None)
            if not host_state:
                continue
            host_state.update_from_instance(instance)
        return host_state_map

    def update_service_capabilities(self, service_name, host, capabilities):
        """Update the per-service capabilities based on this notification."""
        logging.debug(_("Received %(service_name)s service update from "
                "%(host)s.") % locals())
        service_caps = self.service_states.get(host, {})
        # Copy the capabilities, so we don't modify the original dict
        capab_copy = dict(capabilities)
        capab_copy["timestamp"] = utils.utcnow()  # Reported time
        service_caps[service_name] = capab_copy
        self.service_states[host] = service_caps

    def host_service_caps_stale(self, host, service):
        """Check if host service capabilites are not recent enough."""
        allowed_time_diff = FLAGS.periodic_interval * 3
        caps = self.service_states[host][service]
        if (utils.utcnow() - caps["timestamp"]) <= \
            datetime.timedelta(seconds=allowed_time_diff):
            return False
        return True

    def delete_expired_host_services(self, host_services_dict):
        """Delete all the inactive host services information."""
        for host, services in host_services_dict.iteritems():
            service_caps = self.service_states[host]
            for service in services:
                del service_caps[service]
                if len(service_caps) == 0:  # Delete host if no services
                    del self.service_states[host]
