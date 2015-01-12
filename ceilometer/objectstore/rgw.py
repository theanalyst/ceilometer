import requests
from awsauth import S3Auth

from oslo.utils import timeutils
from oslo.config import cfg

from ceilometer.i18n import _
from ceilometer import sample
from ceilometer.agent import plugin_base
from ceilometer.openstack.common import log

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

    def __init__(self):
        self.access_key = cfg.CONF.service_credentials.rgw_access_key
        self.secret = cfg.CONF.service_credentials.rgw_secret_key
        self.endpoint = 'http://127.0.0.1:8080/admin'
        self.host = '127.0.0.1:8080'
        LOG.debug(_("RGW Poller initiaged with %1".format(self.access_key)))
        
    @property
    def default_discovery(self):
        return 'tenant'


class ContainerObjectsPollster(_Base):
    """Get info about object counts in a container using RGW Admin APIs"""
    def get_samples(self, manager, cache, resources):
        tenants = resources
        tenant = "admin"
        METHOD = "bucket"
        r = requests.get("{0}/{1}".format(self.endpoint, METHOD),
                         params={"uid": tenant, "stats": True},
                         auth=S3Auth(self.access_key, self.secret, self.host)
                         )
        bucket_data = r.json()

        for it in bucket_data:
            for k, v in it["usage"].items():
                yield sample.Sample(
                    name='radosgw.containers.objects',
                    type=sample.TYPE_GAUGE,
                    volume=v["num_objects"],
                    unit='object',
                    user_id=None,
                    project_id=tenant,
                    resource_id=tenant + '/' + it['bucket'],
                    timestamp=timeutils.isotime(),
                    resource_metadata=None,
                )

class ContainersSizePollster(_Base):
    """Get info about object counts in a container using RGW Admin APIs"""
    def get_samples(self, manager, cache, resources):
        tenants = resources
        tenant = "admin"
        METHOD = "bucket"
        r = requests.get("{0}/{1}".format(self.endpoint, METHOD),
                         params={"uid": tenant, "stats": True},
                         auth=S3Auth(self.access_key, self.secret, self.host)
                         )
        LOG.debug(_("RGW Container Size Poller initiaged with %1".format(r.json())))
        bucket_data = r.json()

        for it in bucket_data:
            for k, v in it["usage"].items():
                yield sample.Sample(
                    name='radosgw.containers.objects.size',
                    type=sample.TYPE_GAUGE,
                    volume=v["size_kb"]*1024,
                    unit='object',
                    user_id=None,
                    project_id=tenant,
                    resource_id=tenant + '/' + it['bucket'],
                    timestamp=timeutils.isotime(),
                    resource_metadata=None,
                )
