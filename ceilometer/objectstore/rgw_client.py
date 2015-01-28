#
# Copyright 2015 eNovance
#
# Author: M R Swami Reddy <swamireddy@gmail.com>
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

import requests
from collections import namedtuple
from awsauth import S3Auth
import six.moves.urllib.parse as urlparse


class RGWAdminClient(object):
    def __init__(self, endpoint, access_key, secret_key):
	self.access_key = access_key
        self.secret = secret_key
        self.endpoint = endpoint
        self.hostname = urlparse.urlparse(endpoint).netloc

    def get_bucket(self, tenant_id):
        METHOD="bucket"
        uri = "{0}/{1}".format(self.endpoint, METHOD)
        r = requests.get(uri, params={"uid": tenant_id, "stats": True},
                         auth=S3Auth(self.access_key,self.secret, self.hostname)
                         )
        stats = self._process_bucket_stats(r.json(), tenant_id)
        return stats

    def get_usage(self, tenant_id):
        METHOD="usage"
        r = requests.get("{0}/{1}".format(self.endpoint, METHOD),
                         params={"uid": tenant_id},
                         auth=S3Auth(self.access_key, self.secret, self.hostname)
                         )
        return self._process_usage_stats(r.json())

    @staticmethod
    def _process_bucket_stats(json_data, tenant_id):
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
