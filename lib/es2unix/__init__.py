import urllib2, json, time, sys, re

def out(s):
    print s.encode('utf-8')

def err(s):
    print >> sys.stderr, s.encode('utf-8')

def timestamp():
    return "%d %s" % (int(time.time()), time.strftime('%H:%M:%S'))

def memoize(function):
    cache = {}
    def decorated_function(*args):
        if args in cache:
            return cache[args]
        else:
            val = function(*args)
            cache[args] = val
            return val
    return decorated_function

def get(url):
    req = urllib2.urlopen(url)
    encoding = req.headers['content-type'].split('charset=')[-1]
    return req.read().decode(encoding)

@memoize
def get_json(url):
    try:
        return json.loads(get(url))
    except StandardError, e:
        print >> sys.stderr, url, e
        return {}

def ip(s):
    return re.findall('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', s)[0]

def state(url, idx=None, node=None):
    _state_ = get_json('%s/_cluster/state' % url)
    if idx:
        return _state_['routing_table']['indices'][idx]
    if node:
        return _state_['nodes'][node]
    return _state_

def cluster_name(url):
    return state(url)['cluster_name']

def node(url, id):
    return get_json('%s/_cluster/nodes' % url)['nodes'][id]

def master(url):
    return node(url, state(url, 'master_node'))

def status(url, idx=None):
    _status_ = get_json('%s/_status' % url)
    if idx:
        return _status_['indices'][idx]
    return _status_

def idxhealth(url, idx=None):
    _ihealth_ = get_json('%s/_cluster/health?level=shards' % url)['indices']
    if idx:
        return _ihealth_[idx]
    return _ihealth_

def shardhealth(url, idx, shidx):
    return idxhealth(url, idx)['shards'][shidx]['status']

def settings(url, idx=None):
    _settings_ = get_json('%s/_settings' % url)
    if idx:
        return _settings_[idx]['settings']
    return _settings_

def num_docs(url, idx):
    return status(url, idx)['docs']['num_docs']

def indices(url):
    return idxhealth(url).keys()

def primary_p(sh):
    if sh['primary']:
        return 'p'
    return 'r'

def relo_info(url, routing):
    _to = routing['relocating_node']
    if _to:
        _to = node(url, _to)
        return '-> %s %s' % (ip(_to['transport_address']), _to['name'])
    return None

def shards(url):
    s = []
    for idx in indices(url):
        for shidx, replicas in status(url, idx)['shards'].items():
            for rep in replicas:
                node = state(url, node=rep['routing']['node'])
                rep.update({'_index': idx,
                            '_shidx': shidx,
                            '_primary': primary_p(rep['routing']),
                            '_bytes': rep['index']['size_in_bytes'],
                            '_bytes_hum': rep['index']['size'],
                            '_ip': ip(node['transport_address']),
                            '_node': node,
                            '_nodename': node['name'],
                            '_nodeid': rep['routing']['node'],
                            '_relo': relo_info(url, rep['routing']),
                            '_status': shardhealth(url, idx, shidx)})
                s.append(rep)
    for sh in state(url)['routing_nodes']['unassigned']:
        sh.update({'_index': sh['index'],
                   '_shidx': sh['shard'],
                   '_primary': primary_p(sh),
                   '_bytes': 0,
                   '_bytes_hum': '0b',
                   '_ip': 'x.x.x.x',
                   '_node': None,
                   '_nodename': None,
                   '_relo': None,
                   '_status': 'x'})
        s.append(sh)
    return s

