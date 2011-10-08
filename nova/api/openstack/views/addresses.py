# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010-2011 OpenStack LLC.
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

import itertools

from nova import flags
from nova import log as logging
from nova import utils


FLAGS = flags.FLAGS
LOG = logging.getLogger('nova.api.openstack.views.addresses')


def _extract_ipv4_addresses(ip_info):
    for fixed_ip in ip_info['fixed_ips']:
        yield _build_ip_entity(fixed_ip['address'], 4)
        for floating_ip in fixed_ip.get('floating_ips', []):
            yield _build_ip_entity(floating_ip['address'], 4)


def _extract_ipv6_addresses(ip_info):
    for fixed_ip6 in ip_info.get('fixed_ip6s', []):
        yield _build_ip_entity(fixed_ip6['address'], 6)


def _build_ip_entity(address, version):
    return {'addr': address, 'version': version}


class ViewBuilder(object):
    """Models a server addresses response as a python dictionary."""

    def build(self, ip_info):
        raise NotImplementedError()


class ViewBuilderV10(ViewBuilder):

    def build(self, ip_info):
        if not ip_info:
            return dict(public=[], private=[])

        return dict(public=self.build_public_parts(ip_info),
                    private=self.build_private_parts(ip_info))

    def build_public_parts(self, ip_info):
        return utils.get_from_path(ip_info,
                'fixed_ips/floating_ips/address')

    def build_private_parts(self, ip_info):
        return utils.get_from_path(ip_info, 'fixed_ips/address')


class ViewBuilderV11(ViewBuilder):

    def build(self, ip_info):
        result = {}
        for entry in ip_info:
            network = entry['network']
            ips = list(_extract_ipv4_addresses(entry))
            if FLAGS.use_ipv6:
                ip6s = list(_extract_ipv6_addresses(entry))
                ips.extend(ip6s)
            result[network] = ips
        return result

    def build_network(self, ip_info, requested_network):
        networks = self.build(ip_info)
        if requested_network in networks:
            return {requested_network: networks[requested_network]}
        else:
            return None
