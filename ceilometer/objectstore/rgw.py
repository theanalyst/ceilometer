#
# Copyright 2015 eNovance
#
# Author: M R Swami Reddy <swamireddy@gmail.com>
# Author: Abhishek L <swamireddy@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""Common code for working with ceph object stores
"""


from oslo.utils import timeutils
from oslo.config import cfg
import six.moves.urllib.parse as urlparse

from keystoneclient import exceptions
from ceilometer.i18n import _
from ceilometer import sample
from ceilometer.agent import plugin_base
from ceilometer.openstack.common import log
from ceilometer.objectstore.rgw_client import RGWAdminClient as rgwclient

LOG = log.getLogger(__name__)

SERVICE_OPTS = [
    cfg.StrOpt('radosgw',
               default='object-store',
               help='Radosgw service type.'),
]

CREDENTIAL_OPTS = [
    cfg.StrOpt('rgw_access_key',
               default='12345',
               help='access_key for RGW Admin'),
    cfg.StrOpt('rgw_secret_key',
               default='12345',
               help='secret key for RGW Admin')
]

cfg.CONF.register_opts(SERVICE_OPTS, group='service_types')
cfg.CONF.register_opts(CREDENTIAL_OPTS, group='service_credentials')
cfg.CONF.import_group('service_credentials', 'ceilometer.service')


class _Base(plugin_base.PollsterBase):
    METHOD = 'bucket'
    _ENDPOINT = None

    
    def __init__(self):
        self.access_key = cfg.CONF.service_credentials.rgw_access_key
        self.secret = cfg.CONF.service_credentials.rgw_secret_key
        
    @property
    def default_discovery(self):
        return 'tenant'

    @property
    def CACHE_KEY_METHOD(self):
        return 'rgw.get_%s' % self.METHOD

    @staticmethod
    def _get_endpoint(ksclient):
        # we store the endpoint as a base class attribute, so keystone is
        # only ever called once, also we assume that in a single deployment
        # we may be only deploying `radosgw` or `swift` as the object-store
        if _Base._ENDPOINT is None:
            try:
                conf = cfg.CONF.service_credentials
                rgw_url = ksclient.service_catalog.url_for(
                    service_type=cfg.CONF.service_types.radosgw,
                    endpoint_type=conf.os_endpoint_type)
                _Base._ENDPOINT = urlparse.urljoin(rgw_url,'/admin')
            except exceptions.EndpointNotFound:
                LOG.debug(_("Radosgw endpoint not found"))
        return _Base._ENDPOINT


    def _iter_accounts(self, ksclient, cache, tenants):
        if self.CACHE_KEY_METHOD not in cache:
            cache[self.CACHE_KEY_METHOD] = list(self._get_account_info(
                ksclient, tenants))
        return iter(cache[self.CACHE_KEY_METHOD])

    def _get_account_info(self, ksclient, tenants):
        endpoint = self._get_endpoint(ksclient)
        rgw_client = rgwclient(endpoint, self.access_key, self.secret)
        for t in tenants:
            api_method = 'get_%s' % self.METHOD
            yield (t.id, getattr(rgw_client, api_method) (t.id))

class ContainersObjectsPollster(_Base):
    """Get info about object counts in a container using RGW Admin APIs"""

    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                       cache, tenants):
            for it in bucket_info['buckets']:
                yield sample.Sample(
                    name='radosgw.containers.objects',
                    type=sample.TYPE_GAUGE,
                    volume=int(it.num_objects),
                    unit='object',
                    user_id=None,
                    project_id=tenant,
                    resource_id=tenant + '/' + it.name,
                    timestamp=timeutils.isotime(),
                    resource_metadata=None,
                )

class ContainersSizePollster(_Base):
    """Get info about object sizes in a container using RGW Admin APIs"""

    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                       cache, tenants):
            for it in bucket_info['buckets']:
                    yield sample.Sample(
                        name='radosgw.containers.objects.size',
                        type=sample.TYPE_GAUGE,
                        volume=int(it.size),
                        unit='kB',
                        user_id=None,
                        project_id=tenant,
                        resource_id=tenant + '/' + it.name,
                        timestamp=timeutils.isotime(),
                        resource_metadata=None,
                    )


class ObjectsSizePollster(_Base):
    """Iterate over all accounts, using keystone."""

    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                   cache, tenants):
            yield sample.Sample(
                name='radosgw.objects.size',
                type=sample.TYPE_GAUGE,
                volume=int(bucket_info['size']),
                unit='kB',
                user_id=None,
                project_id=tenant,
                resource_id=tenant,
                timestamp=timeutils.isotime(),
                resource_metadata=None,
                )


class ObjectsPollster(_Base):
    """Iterate over all accounts, using keystone."""

    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                   cache, tenants):
            yield sample.Sample(
                name='radosgw.objects',
                type=sample.TYPE_GAUGE,
                volume=int(bucket_info['num_objects']),
                unit='object',
                user_id=None,
                project_id=tenant,
                resource_id=tenant,
                timestamp=timeutils.isotime(),
                resource_metadata=None,
                )


class ObjectsContainersPollster(_Base):
    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                   cache, tenants):
            yield sample.Sample(
                name='radosgw.objects.containers',
                type=sample.TYPE_GAUGE,
                volume=int(bucket_info['num_buckets']),
                unit='object',
                user_id=None,
                project_id=tenant,
                resource_id=tenant,
                timestamp=timeutils.isotime(),
                resource_metadata=None,
                )

class UsagePollster(_Base):

    METHOD = 'usage'

    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, usage in self._iter_accounts(manager.keystone,
                                                 cache, tenants):
            yield sample.Sample(
                name='radosgw.api.request',
                type=sample.TYPE_DELTA,
                volume=int(usage),
                unit='request',
                user_id=None,
                project_id=tenant,
                resource_id=tenant,
                timestamp=timeutils.isotime(),
                resource_metadata=None,
                )

