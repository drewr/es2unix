import urllib2, json, time, sys, re

def out(s):
    print s.encode('utf-8')

def err(s):
    print >> sys.stderr, s.encode('utf-8')

def timestamp():
    return "%d %s" % (int(time.time()), time.strftime('%H:%M:%S'))

def get(url):
    req = urllib2.urlopen(url)
    encoding = req.headers['content-type'].split('charset=')[-1]
    return req.read().decode(encoding)

_json_responses_ = {}
def get_json(url):
    if not _json_responses_.has_key(url):
        try:
            _json_responses_[url] = json.loads(get(url))
        except StandardError, e:
            print >> sys.stderr, url, e
            _json_responses_[url] = {'_error': e}
    return _json_responses_[url]

def ip(s):
    return re.findall('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', s)

_state_ = None
def state(url, idx=None, node=None):
    global _state_
    if not _state_:
        _state_ = get_json('%s/_cluster/state' % url)
    if idx:
        return _state_['routing_table']['indices'][idx]
    if node:
        return _state_['nodes'][node]
    return _state_

_status_ = None
def status(url, idx=None):
    global _status_
    if not _status_:
        _status_ = get_json('%s/_status' % url)
    if idx:
        return _status_['indices'][idx]
    return _status_

_ihealth_ = None
def idxhealth(url, idx=None):
    global _ihealth_
    if not _ihealth_:
        _ihealth_ = get_json('%s/_cluster/health?level=shards' % url)['indices']
    if idx:
        return _ihealth_[idx]
    return _ihealth_

_settings_ = None
def settings(url, idx=None):
    global _settings_
    if not _settings_:
        _settings_ = get_json('%s/_settings' % url)
    if idx:
        return _settings_[idx]['settings']
    return _settings_

def num_docs(url, idx):
    return status(url, idx)['docs']['num_docs']

def indices(url):
    return idxhealth(url).keys()

def shards(url):
    s = []
    for idx in indices(url):
        for shidx, replicas in status(url, idx)['shards'].items():
            for rep in replicas:
                p = 'r'
                if rep['routing']['primary']:
                    p = 'p'
                node = state(url, node=rep['routing']['node'])
                rep.update({'_index': idx,
                            '_shidx': shidx,
                            '_primary': p,
                            '_bytes': rep['index']['size_in_bytes'],
                            '_ip': ip(node['transport_address'])[0],
                            '_node': node,
                            '_nodeid': rep['routing']['node']})
                s.append(rep)
    return s

