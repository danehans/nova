# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# Copyright 2011 Piston Cloud Computing, Inc.
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
Tests For Compute w/ Zones
"""
from nova.compute import zones_api as compute_zones_api
from nova import flags
from nova import log as logging
from nova.tests import test_compute


LOG = logging.getLogger('nova.tests.test_compute_zones')
FLAGS = flags.FLAGS

ORIG_COMPUTE_API = None


def stub_call_to_zones(context, instance, method, *args, **kwargs):
    fn = getattr(ORIG_COMPUTE_API, method)
    return fn(context, instance, *args, **kwargs)


def stub_cast_to_zones(context, instance, method, *args, **kwargs):
    fn = getattr(ORIG_COMPUTE_API, method)
    fn(context, instance, *args, **kwargs)


def deploy_stubs(stubs, api):
    stubs.Set(api, 'call_to_zones', stub_call_to_zones)
    stubs.Set(api, 'cast_to_zones', stub_cast_to_zones)


class ZonesComputeAPITestCase(test_compute.ComputeAPITestCase):
    def setUp(self):
        super(ZonesComputeAPITestCase, self).setUp()
        global ORIG_COMPUTE_API
        ORIG_COMPUTE_API = self.compute_api
        self.compute_api = compute_zones_api.ComputeZonesAPI()
        deploy_stubs(self.stubs, self.compute_api)


class ZonesComputePolicyTestCase(test_compute.ComputePolicyTestCase):
    def setUp(self):
        super(ZonesComputePolicyTestCase, self).setUp()
        global ORIG_COMPUTE_API
        ORIG_COMPUTE_API = self.compute_api
        self.compute_api = compute_zones_api.ComputeZonesAPI()
        deploy_stubs(self.stubs, self.compute_api)
