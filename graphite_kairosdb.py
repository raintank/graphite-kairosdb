import re
import time
import struct
import collections
from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
from graphite_api.intervals import Interval, IntervalSet
from graphite_api.node import LeafNode, BranchNode
from flask import g
import json
import structlog
logger = structlog.get_logger('graphite_api')

def parse_row_ts(ts, row_timestamp):
    timestamp = struct.unpack(">L", ts)[0]
    timestamp = timestamp >> 1
    return (timestamp + row_timestamp) / 1000

def parse_row_key(key):
    #read to first null byte to get measurement name
    measurement, key = key.split('\0', 1)
    # the next 8 bytes are the row_timestamp
    row_timestamp = struct.unpack(">q", key[:8])[0]
    #byte 8 is a null byte,
    #byte 9 is the length of the data_type label.
    data_type_size = struct.unpack(">b", key[9:10])[0]
    #From byte 10 for data_type_size bytes is the data_type label
    data_type = key[10:10+data_type_size]
    # the rest of the bytes are the tags string.
    tags = key[10+data_type_size:]

    return {"measurement": measurement, "row_timestamp": row_timestamp, "data_type": data_type, "tags": tags}

# https://github.com/kairosdb/kairosdb/blob/master/src/main/java/org/kairosdb/util/Util.java#L140
def unpack_kairos_unsignedlong(buffer):
    result = 0
    pos = 0
    shift = 0
    while shift < 64:
        b = struct.unpack(">b", buffer[pos])[0]
        result |= (b & 0x7f) << shift
        if (b & 0x80) == 0:
            return result
        shift += 7
        pos += 1
    raise Exception("Variable length quantity is too long")

def unpack_kairos_long(buffer):
    value = unpack_kairos_unsignedlong(buffer)
    return ((value >> 1) ^ -(value & 1))


class NullStatsd():
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def timer(self, key, val=None):
        return self

    def timing(self, key, val):
        pass

try:
    from graphite_api.app import app
    statsd = app.statsd
    assert statsd is not None
except:
    statsd = NullStatsd()

class RaintankMetric(object):
    __slots__ = ('id', 'org_id', 'name', 'metric', 'interval', 'tags',
        'target_type', 'unit', 'lastUpdate', 'public', 'leaf')

    def __init__(self, source, leaf):
        self.leaf = leaf
        for slot in RaintankMetric.__slots__:
            if slot in source:
                setattr(self, slot, source[slot])

    def is_leaf(self):
        #logger.debug("is_leaf", leaf=self.leaf, name=self.name)
        return self.leaf


class KairosdbFinder(object):
    __fetch_multi__ = "kairosdb"

    def __init__(self, config):
        cfg = config.get('kairosdb', {})
        es = cfg.get('es', {})
        cas = cfg.get('cassandra', {})
        self.config = {
            "cassandra": {
               "hosts": cas.get('hosts', ["localhost"]),
               "port": cas.get('port', 9042),
            },
            "es": {
                "url": es.get('url', 'http://localhost:9200')
            }
        }
        logger.info("initialize kairosdbFinder", config=self.config)
        self.es = Elasticsearch([self.config['es']['url']])
        self.cassandra = Cluster(self.config['cassandra']['hosts'], self.config['cassandra']['port']).connect('kairosdb')
        self.metric_lookup_stmt = self.cassandra.prepare('SELECT * FROM data_points WHERE key=? AND column1 > ? AND column1 <= ?')

    def find_nodes(self, query):
        seen_branches = set()
        leaf_regex = self.compile_regex(query, False)
        #query Elasticsearch for paths
        matches = self.search_series(leaf_regex, query)
        leafs = {}
        branches = {}
        for metric in matches:
            if metric.is_leaf():
                if metric.name in leafs:
                    leafs[metric.name].append(metric)
                else:
                    leafs[metric.name] = [metric]
            else:
                if metric.name in branches:
                    branches[metric.name].append(metric)
                else:
                    branches[metric.name] = [metric]

        for name, metrics in leafs.iteritems():    
            yield KairosdbLeafNode(name, KairosdbReader(self.config, metrics))
        for branchName, metrics in branches.iteritems():
            name = branchName
            while '.' in name:
                name = name.rsplit('.', 1)[0]
                if name not in seen_branches:
                    seen_branches.add(name)
                    if leaf_regex.match(name) is not None:
                        yield BranchNode(name)

    def fetch_from_cassandra(self, nodes, start_time, end_time):
        # datapoints are stored in rows that spane a 3week period.
        # so we need to determine the 1 or more periods we need to query.
        periods = []
        start_period = start_time - (start_time % 1814400)
        periods.append({ 'key': start_period, 'start': start_time, 'end': end_time })
        end_period = end_time - (end_time % 1814400)
        if start_period != end_period:
            pos = start_period + 1814400
            count = 0
            while pos <= end_period:
                periods.append({'key': pos, 'start': pos, 'end': end_time})
                # set the end_time range boundry of the last period to the end of that period.
                periods[count]['end'] = pos - 1
                count += 1
                pos += 1814400

        # we now need to generate all of the row_keys that we need.
        # we store an array of tuples, where each tuple is the (row_key, start_offset, end_offset)
        query_args = []
        node_index = {}
        datapoints = {}
        for node in nodes:
            for metric in node.reader.metrics:
                measurement = metric.metric
                tags = ""
                tag_list = metric.tags
                tag_list.append('org_id:%d' % g.org)
                for tag in sorted(tag_list):
                    parts = tag.split(":", 2)
                    tags += "%s=%s:" % (parts[0], parts[1])

                #keep a map between the measurement+tags to the node.path
                node_index["%s\0%s" % (measurement, tags)] = node.path

                #initialize where we will store the data.
                datapoints[node.path] = {}

                # now build or query_args
                for data_type in ["kairos_double", "kairos_long"]: #request both double and long values as kairos makes it impossible to know which in advance.
                    data_type_size = len(data_type)
                    for p in periods:
                        row_timestamp = p['key'] * 1000
                        row_key = "%s00%s00%s%s%s" % (
                            measurement.encode('hex'), "%016x" % row_timestamp, "%02x" % data_type_size, data_type.encode('hex'), tags.encode('hex')
                        )
                        logger.debug("cassandra query", row_key=row_key)
                        start = (p['start'] - p['key']) * 1000
                        end = (p['end'] - p['key']) * 1000

                        #The timestamps are shifted to support legacy datapoints that
                        #used the extra bit to determine if the value was long or double
                        row_key_bytes = bytearray(row_key.decode('hex'))
                        try:
                            start_bytes = bytearray(struct.pack(">L", start << 1))
                        except Exception as e:
                            logger.error("failed to pack %d" % start)
                            raise e
                        try:
                            end_bytes = bytearray(struct.pack(">L", end << 1 ))
                        except Exception as e:
                            logger.error("failed to pack %d" % end)
                            raise e

                        query_args.append((row_key_bytes, start_bytes , end_bytes))

        #perform cassandra queries in parrallel using async requests.
        futures = []
        for args in query_args:
            futures.append(self.cassandra.execute_async(self.metric_lookup_stmt, args))

        # wait for them to complete and use the results
        for future in futures:
            rows = future.result()
            first = True
            for row in rows:
                if first:
                    row_key = parse_row_key(row.key)
                    path = node_index["%s\0%s" % (row_key['measurement'], row_key['tags'])]
                    
                    if path not in datapoints:
                        datapoints[path] = {}
                    first = False

                ts = parse_row_ts(row.column1, row_key['row_timestamp'])
                try:
                    if row_key['data_type'] == "kairos_double":
                        value = struct.unpack(">d", row.value)[0]
                    else:
                        value = unpack_kairos_long(row.value)
                except Exception as e:
                    logger.error("failed to parse value", exception=e, data_type=row_key['data_type'])
                    value = None
                datapoints[path][ts] = value

        return datapoints

    def fetch_multi(self, nodes, start_time, end_time):
        step = None
        for node in nodes:
            for metric in node.reader.metrics:
                if step is None or metric.interval < step:
                    step = metric.interval

        with statsd.timer("graphite-api.fetch.kairosdb_query.query_duration"):
            data = self.fetch_from_cassandra(nodes, start_time, end_time)
        series = {}
        delta = None
        with statsd.timer("graphite-api.fetch.unmarshal_kairosdb_resp.duration"):
            for path, points in data.items():
                datapoints = []
                next_time = start_time;
                timestamps = points.keys()
                timestamps.sort()
                max_pos = len(timestamps)

                if max_pos == 0:
                    for i in range(int((end_time - start_time) / step)):
                        datapoints.append(None)
                    series[path] = datapoints
                    continue

                pos = 0

                if delta is None:
                    delta = (timestamps[0] % start_time) % step
                    # ts[0] is always greater then start_time.
                    if delta == 0:
                        delta = step

                while next_time <= end_time:
                    # check if there are missing values from the end of the time window
                    if pos >= max_pos:
                        datapoints.append(None)
                        next_time += step
                        continue

                    ts = timestamps[pos]
                    # read in the metric value.
                    v = points[ts]

                    # pad missing points with null.
                    while ts > (next_time + step):
                        datapoints.append(None)
                        next_time += step

                    datapoints.append(v)
                    next_time += step
                    pos += 1
                    if (ts + step) > end_time:
                        break

                series[path] = datapoints

        if delta is None:
            delta = 1
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
                           "org_id": -1
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

        with statsd.timer("graphite-api.search_series.es_search.query_duration"):
            ret = self.es.search(index="metric", doc_type="metric_index", body=search_body, size=10000 )
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
    __slots__ = ('config', 'metrics')

    def __init__(self, config, metrics):
        self.config = config
        self.metrics = metrics

    def get_intervals(self):
        return IntervalSet([Interval(0, time.time())])

    def fetch(self, startTime, endTime):
        pass

