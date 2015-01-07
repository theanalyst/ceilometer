from oslo.utils import timeutils
from oslo.config import cfg
import requests
from awsauth import S3Auth

from ceilometer.agent import plugin_base
from ceilometer.openstack.common import log
from ceilometer import sample

LOG = log.getLogger(__name__)

SERVICE_OPTS = [
    cfg.StrOpt('radosgw',
               default='object-store',
               help='Radosgw service type.'),
]

cfg.CONF.register_opts(SERVICE_OPTS, group='service_types')
cfg.CONF.import_group('service_credentials', 'ceilometer.service')


class _Base(plugin_base.PollsterBase):

    def __init__(self):
        self.access_key = '100a440500b64c5eac30976bf8c65d96'
        self.secret = '5451764aaf8d4ca28d30d423c7a3f337'
        self.endpoint = 'http://127.0.0.1:8080/admin'
        self.host = '127.0.0.1:8080'


class ContainerObjectsPollster(_Base):
    """Get info about object counts in a container using RGW Admin APIs"""
    def get_samples(self):
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
                    name='storage.containers.objects',
                    type=sample.TYPE_GAUGE,
                    volume=v["num_objects"],
                    unit='object',
                    user_id=None,
                    project_id=tenant,
                    resource_id=tenant + '/' + it['bucket'],
                    timestamp=timeutils.isotime(),
                    resource_metadata=None,
                )
