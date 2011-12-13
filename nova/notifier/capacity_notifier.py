# Copyright 2011 OpenStack LLC.
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

import json

from nova import context
from nova.db import api


def notify(message):
    """Look for specific compute manager events and interprete them
    so as to keep the Capacity table up to date.

    NOTE: the True/False return codes are only for testing.
    """

    # The event_type must start with 'compute.instance.'
    event_type = message.get('event_type', None)
    preamble = 'compute.instance.'
    if not event_type or not event_type.startswith(preamble):
        return False

    # Events we're interested in end with .start and .end
    event = event_type[len(preamble):]
    parts = event.split('.')
    suffix = parts[-1].lower()
    event = event[:(-len(suffix) - 1)]

    if suffix not in ['start', 'end']:
        return False
    started = suffix == 'start'
    ended = suffix == 'end'

    work = 1 if started else -1

    # Extract the host name from the publisher id ...
    publisher_preamble = 'compute.'
    publisher = message.get('publisher_id', None)
    if not publisher or not publisher.startswith(publisher_preamble):
        return False
    host = publisher[len(publisher_preamble):]

    # If we deleted an instance, make sure we reclaim the resources.
    # We may need to do something explicit for rebuild/migrate.
    free_ram_mb = 0
    vms = 0
    if ended and event == 'delete':
        vms = -1
        free_ram_mb = message.get('payload', {}).get('memory_mb', 0)
        free_disk_gb = message.get('payload', {}).get('disk_gb', 0)

    if free_ram_mb == 0 and free_disk_gb == 0 and work == 0 and vms == 0:
        return

    api.capacity_update(host, context.get_admin_context(),
        free_ram_mb=free_ram_mb, free_disk_gb=free_disk_gb, work=work,
        vms=vms)

    return True
