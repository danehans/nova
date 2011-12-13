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
"""
Fakes For Distributed Scheduler tests.
"""

from nova.scheduler import distributed_scheduler
from nova.scheduler import zone_manager


class FakeDistributedScheduler(distributed_scheduler.DistributedScheduler):
    # No need to stub anything at the moment
    pass


class FakeZoneManager(zone_manager.ZoneManager):
    """host1: free_ram_mb=1024 free_disk_gb=1024
       host2: free_ram_mb=2048 free_disk_gb=2048
       host3: free_ram_mb=4096 free_disk_gb=4096
       host4: free_ram_mb=8192 free_disk_gb=8192"""

    def __init__(self):
        self.service_states = {
            'host1': {
                'compute': {'host_memory_free': 1073741824},
            },
            'host2': {
                'compute': {'host_memory_free': 2147483648},
            },
            'host3': {
                'compute': {'host_memory_free': 3221225472},
            },
            'host4': {
                'compute': {'host_memory_free': 999999999},
            },
        }

    def get_host_list_from_db(self, context):
        return [
            ('host1', dict(free_disk_gb=1024, free_ram_mb=1024)),
            ('host2', dict(free_disk_gb=2048, free_ram_mb=2048)),
            ('host3', dict(free_disk_gb=4096, free_ram_mb=4096)),
            ('host4', dict(free_disk_gb=8192, free_ram_mb=8192)),
        ]

    def _get_suitable_hosts(self, context, minimum_ram_mb, minimum_disk_gb):
        return [
            dict(free_disk_gb=0, free_ram_mb=0, host='host1',
                 running_vms=0, current_workload=0),
            dict(free_disk_gb=2048, free_ram_mb=2048, host='host2',
                 running_vms=1, current_workload=1),
            dict(free_disk_gb=4096, free_ram_mb=4096, host='host3',
                 running_vms=2, current_workload=2),
            dict(free_disk_gb=8192, free_ram_mb=8192, host='host4',
                 running_vms=3, current_workload=4),
        ]
