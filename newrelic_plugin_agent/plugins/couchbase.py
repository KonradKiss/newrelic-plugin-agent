"""
Couchbase
"""
import os, requests

from requests.auth import HTTPBasicAuth
from newrelic_plugin_agent.plugins import base
from statsmetrics import couchbase as couchbasemetrics

class Couchbase(base.JSONStatsPlugin):

    GUID = 'com.meetme.Couchbase'

    # Get metrics from statsmetrics plugin and transform to a list of dicts
    def _get_metrics(self):
        self.metrics = []
        for key,values in couchbasemetrics.get_metrics().items():
          for metric in values['metrics']:
            obj = {
              'type': key,
              'label': metric['id'],
              'suffix': metric['suffix']
            }
            self.metrics.append(obj)
            if 'bucket_stats' in values:
              for metric in values['bucket_stats']:
                obj = {
                  'type': 'bucket_stats',
                  'label': metric['id'],
                  'suffix': metric['suffix']
                }
                self.metrics.append(obj)

    def add_datapoints(self, data):
        """Add data points for all couchbase nodes.
        :param dict stats: stats for all nodes
        """
        self._get_metrics()
        # fetch metrics for each metric type (cluster, nodes, buckets)
        for typ, stats in data.iteritems():
            # set items to fetch stats from,
            # and set item key name, where the item's name will be fetched from
            if typ == 'nodes':
                items = stats['nodes']
                name_key = 'hostname'
            else:
                items = stats
                name_key = 'name'

            for metric in [m for m in self.metrics if m['type'] == typ]:
                # add gauge value for current metric.
                # cluster metrics are not repeated
                # nodes and bucket metrics are repeated,
                # bucket_stats metrics are a list of metrics
                # in the last minute, so we calculate average value
                if typ == 'cluster':
                    self._add_gauge_value(
                        metric, typ, items[name_key], items)
                elif typ == 'bucket_stats':
                    for item in items:
                        self._add_gauge_value(
                            metric, typ, item, items[item]["op"]["samples"])
                else:
                    for item in items:
                        self._add_gauge_value(
                                metric, typ, item[name_key], item)


    def _add_gauge_value(self, metric, typ, name, items):
        """Adds a gauge value for a nested metric.
        Some stats are missing from memcached bucket types,
        thus we use dict.get()
        :param dict m: metric as defined at the top of this class.
        :param str typ: metric type: cluster, nodes, buckets, bucket_stats.
        :param str name: cluster, node, or bucket name.
        :param dict items: stats to lookup the metric in.
        """
        label = metric['label']
        value = items
        for key in label.split('.'):
            if typ == 'bucket_stats':
                valueList = value[key]
                value = sum(valueList) / float(len(valueList))
            else:
                value = value.get(key, 0)
        self.add_gauge_value('%s/%s/%s' % (typ, name, label),
                             metric['suffix'], value)

    def fetch_data(self):
        """Fetch data from multiple couchbase stats URLs.
        Returns a dictionary with three keys: cluster, nodes and buckets.
        Each key holds the JSON response from the API request.
        If checking bucket, there is another API request to get bucket stats.
        :rtype: dict
        """
        data = {}
        for path, typ in [('pools/default', 'cluster'),
                          ('pools/nodes', 'nodes'),
                          ('pools/default/buckets', 'buckets')]:
            res = self.api_request(path)
            res = res and res.json() or {}
            data[typ] = res
            # Check if there are bucket stats
            if typ == 'buckets':
                data_bucket = {}
                for bucket in res:
                    if 'stats' in bucket:
                        res_bucket = self.api_request(bucket['stats']['uri'])
                        res_bucket = res_bucket and res_bucket.json() or {}
                        data_bucket[bucket['name']] = res_bucket
                data["bucket_stats"] = data_bucket
        return data

    def api_request(self, path):
        """Request data using Couchbase API
        Allows basic authentication with ENV variables
        Returns a Response object
        """
        if set(["COUCHBASE_USERNAME","COUCHBASE_PASSWORD"]).issubset(os.environ):
            res = requests.get(self.stats_url + path, auth=HTTPBasicAuth(os.environ["COUCHBASE_USERNAME"], os.environ["COUCHBASE_PASSWORD"]))
        else:
            res = requests.get(self.stats_url + path)
        return res
