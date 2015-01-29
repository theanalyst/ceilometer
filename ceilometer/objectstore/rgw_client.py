import requests
from collections import namedtuple
from awsauth import S3Auth
import six.moves.urllib.parse as urlparse

from ceilometer.i18n import _

class RGWAdminAPIFailed(Exception):
    pass

class RGWAdminClient(object):
    def __init__(self, endpoint, access_key, secret_key):
	self.access_key = access_key
        self.secret = secret_key
        self.endpoint = endpoint
        self.hostname = urlparse.urlparse(endpoint).netloc


    def _make_request(self, path, req_params):
        uri = "{0}/{1}".format(self.endpoint,path)
        r = requests.get(uri, params=req_params,
                         auth=S3Auth(self.access_key, self.secret, self.hostname)
                         )

        if r.status_code != 200:
            raise RGWAdminAPIFailed(
                _('RGW AdminOps API returned %(status)s %(reason)s') %
                {'status': r.status_code, 'reason': r.reason})

        return r.json()

    def get_bucket(self, tenant_id):
        path = "bucket"
        req_params = {"uid":tenant_id, "stats": "true"}
        resp =self._make_request(path, req_params)
        stats = self._process_bucket_stats(resp)
        return stats

    def get_usage(self, tenant_id):
        path = "usage"
        req_params = {"uid": tenant_id}
        resp = self._make_request(path, req_params)
        return self._process_usage_stats(resp)

    @staticmethod
    def _process_bucket_stats(json_data):
        stats = {'num_buckets': 0, 'buckets': [], 'size': 0, 'num_objects': 0}
        Bucket = namedtuple('Bucket', 'name, num_objects, size')
        stats['num_buckets'] = len(json_data)
        for it in json_data:
            for k, v in it["usage"].items():
                stats['num_objects'] += v["num_objects"]
                stats['size'] += v["size_kb"]
                stats['buckets'].append(Bucket(it["bucket"], v["num_objects"],
                                               v["size_kb"]))

        return stats

    @staticmethod
    def _process_usage_stats(json_data):
        usage_data = json_data["summary"]
        return sum((it["total"]["successful_ops"] for it in usage_data))
