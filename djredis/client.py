# coding: utf-8

import functools
import itertools
import re

from collections import defaultdict
from redis import StrictRedis
from redis.exceptions import RedisError
from redis.sentinel import Sentinel

from django.core.exceptions import ImproperlyConfigured

from djredis import errors
from djredis.utils import get_node_name
from djredis.utils.hashring import HashRing

TAG_RE = re.compile('.*\{(.*)\}.*', re.I)

def _combine_into_list(keys, args):
  # returns a single list combining keys and args
  try:
    iter(keys)
    # A string or bytes instance can be iterated, but indicates keys wasn't
    # passed as a list
    if isinstance(keys, (basestring, bytes)):
      keys = [keys]
  except TypeError:
    keys = [keys]
  keys = list(keys)
  if args:
    keys.extend(args)
  return keys


class RingClient(object):
  # TODO(usmanm): Add support for other redis commands.
  # TOOD(usmanm): Add support for removing dead nodes and re-adding them
  # when they return.
  BROADCAST_METHODS = {'dbsize', 'flushdb', 'info', 'ping'}
  ROUTE_METHODSS = {'exists', 'get', 'getset', 'incr', 'lock', 'set'}

  def __init__(self, hosts, options):
    if all(isinstance(host, StrictRedis) for host in hosts):
      nodes = list(hosts)
    else:
      assert all(isinstance(host, tuple) and len(host) == 2 for host in hosts)
      kwargs = self._get_node_kwargs(options)
      nodes = []
      for host, port in hosts:
        kwargs['host'] = host
        kwargs['port'] = port
        nodes.append(StrictRedis(**kwargs))
    self.name_to_node = {get_node_name(node): node for node in nodes}
    self.ring = HashRing(self.name_to_node.keys())

  def _get_node_kwargs(self, options):
    try:
      db = int(options.get('DATABASE', 0))
    except ValueError:
      raise ImproperlyConfigured('`DATABASE` must be a valid integer.')
    password = options.get('PASSWORD')
    try:
      socket_timeout = float(options.get('SOCKET_TIMEOUT', 0.2))
    except ValueError:
      raise ImproperlyConfigured('`SOCKET_TIMEOUT` must be a valid number type.')
    return {
      'db': db,
      'password': password,
      'socket_timeout': socket_timeout
      }

  def get_node(self, key):
    match = TAG_RE.match(key)
    if match:
      key = match.groups()[0]
    return self.name_to_node[self.ring(key)]

  def _broadcast(self, attr, *args, **kwargs):
    response = {}
    # TODO(usmanm): Parallelize this?
    for name, node in self.name_to_node.iteritems():
      response[name] = getattr(node, attr)(*args, **kwargs)
    return response

  def _route(self, attr, *args, **kwargs):
    assert len(args) > 0 or len(kwargs) > 0
    key = kwargs['key'] if 'key' in kwargs else args[0]
    return getattr(self.get_node(key), attr)(*args, **kwargs)

  def __getattr__(self, attr):
    if attr in RingClient.BROADCAST_METHODS:
      return functools.partial(self._broadcast, attr)
    if attr in RingClient.ROUTE_METHODSS:
      return functools.partial(self._route, attr)
    raise AttributeError("'%s' object has no attribute '%s'" %
                         (self.__class__.__name__, attr))

  def _get_node_to_key_map(self, keys):
    node_to_keys = defaultdict(list)
    for key in keys:
      node_to_keys[self.get_node(key)].append(key)
    return node_to_keys

  def delete(self, *keys):
    node_to_keys = self._get_node_to_key_map(keys)
    count = 0
    for node, keys in node_to_keys.iteritems():
      count += node.delete(*keys)
    return count

  def delete_tag(self, tag):
    # TODO(usmanm): Use evalsha instead to save sending the script to redis
    # everytime.
    node = self.get_node(tag)
    script = ('local keys = redis.call("keys", ARGV[1])\n'
              'local n = 0\n'
              'for k, v in ipairs(keys) do\n'
              '  n = n + redis.call("del", v)\n'
              'end\n'
              'return n')
    return node.eval(script, 0, '*{%s}*' % tag)

  def mget(self, keys, *args):
    keys = _combine_into_list(keys, args)
    node_to_keys = self._get_node_to_key_map(keys)
    key_to_value = {}
    for node, keys_to_fetch in node_to_keys.iteritems():
      key_to_value.update(dict(zip(keys_to_fetch, node.mget(keys_to_fetch))))
    return [key_to_value[key] for key in keys]

  def mset(self, mapping):
    node_to_mapping = defaultdict(dict)
    for key, value in mapping.iteritems():
      node_to_mapping[self.get_node(key)][key] = value
    return all(node.mset(mapping)
               for node, mapping in node_to_mapping.iteritems())

  def keys(self, pattern='*'):
    return list(itertools.chain(*(node.keys(pattern) for node in
                                  self.name_to_node.itervalues())))
          

  def disconnect(self):
    for node in self.name_to_node.itervalues():
      try:
        node.connection_pool.disconnect()
      except AttributeError:
        # TODO(usmanm): Figure this shit out.
        pass

class SentinelBackedRingClient(RingClient):
  def __init__(self, hosts, options):
    sentinel_kwargs = self._get_sentinel_kwargs(options)
    node_kwargs = self._get_node_kwargs(options)

    masters = None
    # Try to fetch a list of all masters from any sentinel.
    for host, port in hosts:
      client = StrictRedis(host=host, port=port, **sentinel_kwargs)
      try:
        masters = client.sentinel_masters().keys()
        break
      except RedisError:
        pass
    if masters is None:
      # No Sentinel responded successfully?
      raise errors.MastersListUnavailable
    if not len(masters):
      # The masters list was empty?
      raise errors.NoMastersConfigured

    sentinel_kwargs.update({
      # Sentinels connected to fewer sentinels than `MIN_SENTINELS` will
      # be ignored.
      'min_other_sentinels': options.get('MIN_SENTINELS',
                                         len(hosts) / 2),
      })
    self.sentinel = Sentinel(hosts, **sentinel_kwargs)
    masters = [self.sentinel.master_for(name, **node_kwargs)
               for name in masters]
    super(SentinelBackedRingClient, self).__init__(masters, options)

  def _get_sentinel_kwargs(self, options):
    password = options.get('SENTINEL_PASSWORD')
    try:
      socket_timeout = float(options.get('SOCKET_TIMEOUT', 0.2))
    except ValueError:
      raise ImproperlyConfigured('`SOCKET_TIMEOUT` must be a valid number type.')
    return {
      'password': password,
      'socket_timeout': socket_timeout
      }

  def disconnect(self):
    for node in self.sentinel.sentinels:
      try:
        node.connection_pool.disconnect()
      except AttributeError:
        # TODO(usmanm): Figure this shit out.
        pass
    super(SentinelBackedRingClient, self).disconnect()
