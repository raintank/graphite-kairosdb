import re
import time
from elasticsearch import Elasticsearch
import requests
from graphite_api.intervals import Interval, IntervalSet
from graphite_api.node import LeafNode, BranchNode
from flask import g
import json
from structlog import get_logger
logger = get_logger()


def debug(*args, **kwargs):
    import pprint
    pprint.pprint((args, kwargs))
logger.debug = debug

class RaintankMetric(object):
    __slots__ = ('id', 'org_id', 'name', 'metric', 'interval', 'tags', 'thresholds',
        'target_type', 'state', 'keepAlives', 'unit', 'lastUpdate', 'public', 'leaf')

    def __init__(self, source, leaf):
        seenSlots = set()
        self.tags = {}
        self.leaf = leaf
        for slot in RaintankMetric.__slots__:
            if slot in source:
                setattr(self, slot, source[slot])
                seenSlots.add(slot)

        for k,v in source.iteritems():
            if k in seenSlots:
                continue
            self.tags[k] = v

    def is_leaf(self):
        return self.leaf

class KairosdbFinder(object):
    __fetch_multi__ = "kairosdb"

    def __init__(self, config):
        cfg = config.get('kairosdb', {})
        es = cfg.get('es', {})
        self.config = {
            "uri": cfg.get('uri', "http://localhost:8080"),
            "es": {
                "url": es.get('url', 'http://localhost:9200')
            }
        }
        self.es = Elasticsearch([self.config['es']['url']])

    def find_nodes(self, query):
        logger.debug("find_nodes")

        seen_branches = set()
        leaf_regex = self.compile_regex(query, False)
        #query Elasticsearch for paths
        matches = self.search_series(leaf_regex, query)
        for metric in matches:
            if metric.is_leaf():
                yield KairosdbLeafNode(metric.name, KairosdbReader(self.config, metric))
            else:
                name = metric.name
                while '.' in name:
                    name = name.rsplit('.', 1)[0]
                    if name not in seen_branches:
                        seen_branches.add(name)
                        if leaf_regex.match(name) is not None:
                            logger.debug("found branch", name=name)
                            yield BranchNode(name)


    def fetch_multi(self, nodes, start_time, end_time):
        step = None

        query = {
           "start_absolute": start_time * 1000,
           "end_absolute": end_time * 1000,
           "metrics": []
        }

        for node in nodes:
            match = {
                "tags": node.reader.metric.tags,
                "name": node.reader.metric.metric,
            }
            match['tags']['org_id'] = node.reader.metric.org_id
            query['metrics'].append(match)
            if step is None or node.reader.metric.interval < step:
                step = node.reader.metric.interval

        max_points = (end_time - start_time) / step
        for q in query['metrics']:
            q['limit'] = max_points
        resp = requests.post("%s/api/v1/datapoints/query" % self.config['uri'], json.dumps(query))
        data = resp.json()
        series = {}
        delta = None
        for i in range(0, len(data['queries'])):
            datapoints = []
            next_time = start_time
            pos = 0
            max_pos = len(data['queries'][i]['results'][0]['values'])
            if max_pos == 0:
                continue
            if delta is None:
                delta = (data['queries'][i]['results'][0]['values'][0][0]/1000) % start_time

            logger.debug(
                caller="fetch_multi()",
                num_points=max_pos,
                start_time=start_time,
                end_time=end_time,
                first_point=data['queries'][i]['results'][0]['values'][0][0],
                last_point=data['queries'][i]['results'][0]['values'][-1][0]
            )
            while next_time <= end_time:
                # check if there are missing values from the end of the time window
                if pos >= max_pos:
                    datapoints.append(None)
                    next_time += step
                    continue

                ts = data['queries'][i]['results'][0]['values'][pos][0]/1000

                # check if the first point is from before the start_time
                if ts <= start_time:
                    pos += 1
                    continue

                # read in the metric value.
                v = data['queries'][i]['results'][0]['values'][pos][1]

                # pad missing points with null.
                while ts > (next_time + step):
                    datapoints.append(None)
                    next_time += step
                datapoints.append(v)
                next_time += step
                pos += 1

            series[nodes[i].path] = datapoints
        if delta is None:
            delta = 0
        time_info = (start_time + delta, end_time, step)
        return time_info, series

    def compile_regex(self, query, branch=False):
        # we turn graphite's custom glob-like thing into a regex, like so:
        # * becomes [^\.]*
        # . becomes \.
        if branch:
            regex = '{0}.*'
        else:
            regex = '^{0}$'

        regex = regex.format(
            query.pattern.replace('.', '\.').replace('*', '[^\.]*').replace('{', '(').replace(',', '|').replace('}', ')')
        )
        logger.debug("compile_regex", pattern=query.pattern, regex=regex)
        return re.compile(regex)

    def search_series(self, leaf_regex, query):
        branch_regex = self.compile_regex(query, True)

        search_body = {
          "query": {
            "filtered": {
              "filter": {
                "or": [
                    {
                        "term": {
                            "org_id": g.org
                        }
                    },
                    {
                        "term": {
                           "public": True
                        }
                    }
                ]
              },
              "query": {
                "regexp": {
                "name": branch_regex.pattern
                }
              }
            }
          }
        }
        ret = self.es.search(index="definitions", doc_type="metric", body=search_body, size=1000 )
        matches = []
        if len(ret["hits"]["hits"]) > 0:
            for hit in ret["hits"]["hits"]:
                leaf = False
                source = hit['_source']
                if leaf_regex.match(source['name']) is not None:
                    leaf = True
                matches.append(RaintankMetric(source, leaf))
        logger.debug('search_series', matches=len(matches))
        return matches


class KairosdbLeafNode(LeafNode):
    __fetch_multi__ = "kairosdb"


class KairosdbReader(object):
    __slots__ = ('config', 'metric')

    def __init__(self, config, metric):
        self.config = config
        self.metric = metric

    def get_intervals(self):
        return IntervalSet([Interval(0, time.time())])

    def fetch(self, startTime, endTime):
        pass

