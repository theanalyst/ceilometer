import requests
from awsauth import S3Auth
import six.moves.urllib.parse as urlparse

class RGWStats(object):
    def __init__(self, user_id):
        """
        Container holding some stats per user
        """
        self.uid = user_id
        self._api_requests = 0
        self._num_buckets = 0
        self._num_objects = 0
        self.buckets = []
        self._size = 0

    def __repr__(self):
        return "<Stats uid={0} api_reqs={1} n_buckets={2}" \
            "n_obj={3} size={4}>".format(self.uid, self._api_requests,
                                         self._num_buckets, self._num_objects,
                                         self._size)


class RGWAdminClient(object):
    def __init__(self, endpoint, access_key, secret_key):
        import pdb 
	pdb.set_trace()
	self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint = endpoint
        self.hostname = urlparse.urlparse(endpoint).netloc


    def get_bucket(tenant_id):
        METHOD = "/bucket"
        uri = urlparse.urljoin(self.endpoint, METHOD)
        r = requests.get(uri, params={"uid": tenant_id, "stats": True},
                         auth=S3Auth(self.access_key,self.secret, self.hostname)
                         )
        stats = self._process_bucket_stats(r.json(), tenant_id)
        return stats

    def get_usage(self, tenant_id):
        METHOD = "usage"
        r = requests.get("{0}/{1}".format(self.endpoint, METHOD),
                         params={"uid": tenant_id},
                         auth=S3Auth(self.access_key, self.secret, self.host)
                         )
        return list(self.iter_usage_stats(r.json()))

    @staticmethod
    def _process_bucket_stats(json_data, tenant_id):
        stats = RGWStats(tenant_id)
        Bucket = namedtuple('Bucket', 'name, num_objects, size')
        for it in json_data:
            for k, v in it["usage"].items():
                stats._num_buckets += 1
                stats._num_objects += v["num_objects"]
                stats._size += v["size_kb"]
                stats.buckets.append(Bucket(it["bucket"], v["num_objects"],
                                            v["size_kb"]))
            else:
                stats.buckets.append(Bucket(it["bucket"], 0, 0))
                stats._num_buckets += 1

        return stats

    @staticmethod
    def iter_usage_stats(user_data):
        Usage = namedtuple('Usage', 'api, s_bytes, r_bytes, ops, success_ops')
        usage_data = user_data["summary"][0]["categories"]
        for it in usage_data:
            yield Usage(it["category"], it["bytes_sent"], it["bytes_received"],
                        it["ops"], it["successful_ops"])
