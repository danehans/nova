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
flags.DEFINE_bool('enable_rpc_zone_routing',
    False,
    'When True, zone communication occurs via RPC.')

LOG = logging.getLogger('nova.zones.api')



