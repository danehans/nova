# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# Copyright 2011 OpenStack LLC
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
Tests For Multi Scheduler
"""

from nova.scheduler import driver
from nova.scheduler import multi
from nova.tests.scheduler import test_scheduler


class FakeComputeScheduler(driver.Scheduler):
    is_fake_compute = True

    def __init__(self):
        super(FakeComputeScheduler, self).__init__()
        self.is_update_caps_called = False

    def schedule_theoretical(self, *args, **kwargs):
        pass

    def schedule(self, *args, **kwargs):
        pass


class FakeVolumeScheduler(driver.Scheduler):
    is_fake_volume = True

    def __init__(self):
        super(FakeVolumeScheduler, self).__init__()
        self.is_update_caps_called = False

    def schedule_create_volume(self, *args, **kwargs):
        pass

    def schedule_create_volumes(self, *args, **kwargs):
        pass

    def schedule(self, *args, **kwargs):
        pass


class FakeDefaultScheduler(driver.Scheduler):
    is_fake_default = True

    def __init__(self):
        super(FakeDefaultScheduler, self).__init__()
        self.is_update_caps_called = False

    def schedule(self, *args, **kwargs):
        pass


class MultiDriverTestCase(test_scheduler.SchedulerTestCase):
    """Test case for multi driver"""

    driver_cls = multi.MultiScheduler

    def setUp(self):
        super(MultiDriverTestCase, self).setUp()
        base_name = 'nova.tests.scheduler.test_multi_scheduler.%s'
        compute_cls_name = base_name % 'FakeComputeScheduler'
        volume_cls_name = base_name % 'FakeVolumeScheduler'
        default_cls_name = base_name % 'FakeDefaultScheduler'
        self.flags(compute_scheduler_driver=compute_cls_name,
                volume_scheduler_driver=volume_cls_name,
                default_scheduler_driver=default_cls_name)
        self._manager = multi.MultiScheduler()

    def test_drivers_inited(self):
        mgr = self._manager
        self.assertEqual(len(mgr.drivers), 3)
        self.assertTrue(mgr.drivers['compute'].is_fake_compute)
        self.assertTrue(mgr.drivers['volume'].is_fake_volume)
        self.assertTrue(mgr.drivers['default'].is_fake_default)

    def test_proxy_calls(self):
        mgr = self._manager
        compute_driver = mgr.drivers['compute']
        volume_driver = mgr.drivers['volume']

        #no compute methods are proxied at this time
        test_methods = {compute_driver: [],
                        volume_driver: ['create_volume', 'create_volumes']}

        for driver, methods in test_methods.iteritems():
            for method in methods:
                mgr_func = getattr(mgr, 'schedule_' + method)
                driver_func = getattr(driver, 'schedule_' + method)
                self.assertEqual(mgr_func, driver_func)

    def test_schedule_fallback_proxy(self):
        mgr = self._manager

        self.mox.StubOutWithMock(mgr.drivers['compute'], 'schedule')
        self.mox.StubOutWithMock(mgr.drivers['volume'], 'schedule')
        self.mox.StubOutWithMock(mgr.drivers['default'], 'schedule')

        ctxt = 'fake_context'
        method = 'fake_method'
        fake_args = (1, 2, 3)
        fake_kwargs = {'fake_kwarg1': 'fake_value1',
                       'fake_kwarg2': 'fake_value2'}

        mgr.drivers['compute'].schedule(ctxt, 'compute', method,
                *fake_args, **fake_kwargs)
        mgr.drivers['volume'].schedule(ctxt, 'volume', method,
                *fake_args, **fake_kwargs)
        mgr.drivers['default'].schedule(ctxt, 'random_topic', method,
                *fake_args, **fake_kwargs)

        self.mox.ReplayAll()
        mgr.schedule(ctxt, 'compute', method, *fake_args, **fake_kwargs)
        mgr.schedule(ctxt, 'volume', method, *fake_args, **fake_kwargs)
        mgr.schedule(ctxt, 'random_topic', method, *fake_args, **fake_kwargs)

    def test_update_service_capabilities(self):
        def fake_update_service_capabilities(self, service, host, caps):
            self.is_update_caps_called = True

        mgr = self._manager
        self.stubs.Set(driver.Scheduler,
                       'update_service_capabilities',
                       fake_update_service_capabilities)
        self.assertFalse(mgr.drivers['compute'].is_update_caps_called)
        self.assertFalse(mgr.drivers['volume'].is_update_caps_called)
        mgr.update_service_capabilities('foo_svc', 'foo_host', 'foo_caps')
        self.assertTrue(mgr.drivers['compute'].is_update_caps_called)
        self.assertTrue(mgr.drivers['volume'].is_update_caps_called)


class SimpleSchedulerTestCase(MultiDriverTestCase):
    """Test case for simple driver."""

    driver_cls = multi.MultiScheduler

    def setUp(self):
        super(SimpleSchedulerTestCase, self).setUp()
        base_name = 'nova.tests.scheduler.test_multi_scheduler.%s'
        compute_cls_name = base_name % 'FakeComputeScheduler'
        volume_cls_name = 'nova.scheduler.simple.SimpleScheduler'
        default_cls_name = base_name % 'FakeDefaultScheduler'
        self.flags(compute_scheduler_driver=compute_cls_name,
                volume_scheduler_driver=volume_cls_name,
                default_scheduler_driver=default_cls_name)
        self._manager = multi.MultiScheduler()

    def test_update_service_capabilities(self):
        def fake_update_service_capabilities(self, service, host, caps):
            self.is_update_caps_called = True

        mgr = self._manager
        self.stubs.Set(driver.Scheduler,
                       'update_service_capabilities',
                       fake_update_service_capabilities)
        self.assertFalse(mgr.drivers['compute'].is_update_caps_called)
        mgr.update_service_capabilities('foo_svc', 'foo_host', 'foo_caps')
        self.assertTrue(mgr.drivers['compute'].is_update_caps_called)
        self.assertTrue(mgr.drivers['volume'].is_update_caps_called)

    def test_drivers_inited(self):
        mgr = self._manager
        self.assertEqual(len(mgr.drivers), 3)
        self.assertTrue(mgr.drivers['compute'].is_fake_compute)
        self.assertTrue(mgr.drivers['volume'] is not None)
        self.assertTrue(mgr.drivers['default'].is_fake_default)

    def test_proxy_calls(self):
        pass
