from oslo.utils import timeutils
from oslo.config import cfg

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
    
    def __init__(self):
        self.access_key = cfg.CONF.service_credentials.rgw_access_key
        self.secret = cfg.CONF.service_credentials.rgw_secret_key
        self.endpoint = 'http://127.0.0.1:8080/admin'
        self.rgw_client = rgwclient(self.endpoint,self.access_key,self.secret)
        
    @property
    def default_discovery(self):
        return 'tenant'

    @property
    def CACHE_KEY_METHOD(self):
        return 'rgw.get_%s' % self.METHOD

    def _iter_accounts(self, ksclient, cache, tenants):
        if self.CACHE_KEY_METHOD not in cache:
            cache[self.CACHE_KEY_METHOD] = list(self._get_account_info(
                ksclient, tenants))
        return iter(cache[self.CACHE_KEY_METHOD])

    def _get_account_info(self, ksclient, tenants):
        for t in tenants:
            api_method = 'get_%s' % self.METHOD
            yield (t.id, getattr(self.rgw_client, api_method) (t.id))

class ContainerObjectsPollster(_Base):
    """Get info about object counts in a container using RGW Admin APIs"""
    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                       cache, tenants):
            for it in bucket_info.buckets:
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
    """Get info about object counts in a container using RGW Admin APIs"""
    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                       cache, tenants):
            for it in bucket_info.buckets:
                    yield sample.Sample(
                        name='radosgw.containers.objects.size',
                        type=sample.TYPE_GAUGE,
                         volume=int(it.size)*1024,
                        unit='object',
                        user_id=None,
                        project_id=tenant,
                        resource_id=tenant + '/' + it.name,
                        timestamp=timeutils.isotime(),
                        resource_metadata=None,
                    )


class ObjectsSizePollster(_Base):
    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                   cache, tenants):
            yield sample.Sample(
                name='radosgw.objects.size',
                type=sample.TYPE_GAUGE,
                volume=int(bucket_info._size)*1024,
                unit='B',
                user_id=None,
                project_id=tenant,
                resource_id=tenant,
                timestamp=timeutils.isotime(),
                resource_metadata=None,
                )


class ObjectsPollster(_Base):
    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                   cache, tenants):
            yield sample.Sample(
                name='radosgw.objects',
                type=sample.TYPE_GAUGE,
                volume=int(bucket_info._num_objects),
                unit='object',
                user_id=None,
                project_id=tenant,
                resource_id=tenant,
                timestamp=timeutils.isotime(),
                resource_metadata=None,
                )


class ContainersPollster(_Base):
    def get_samples(self, manager, cache, resources):
        tenants = resources
        for tenant, bucket_info in self._iter_accounts(manager.keystone,
                                                   cache, tenants):
            yield sample.Sample(
                name='radosgw.containers',
                type=sample.TYPE_GAUGE,
                volume=int(bucket_info._num_buckets),
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
                name='radosgw.usage',
                type=sample.TYPE_GAUGE,
                volume=int(usage),
                unit='requests',
                user_id=None,
                project_id=tenant,
                resource_id=tenant,
                timestamp=timeutils.isotime(),
                resource_metadata=None,
                )

