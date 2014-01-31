# coding: utf-8

import functools
import hashlib
import itertools

from collections import defaultdict
from random import shuffle
from redis import StrictRedis
from redis.exceptions import RedisError
from redis.sentinel import Sentinel

from django.core.exceptions import ImproperlyConfigured

from djredis import errors
from djredis.conf import settings
from djredis.utils import get_node_name
from djredis.utils.hashring import HashRing


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
  ROUTE_METHODS = {'getset', 'lock'}
  TAG_ROUTE_METHODS = {'exists', 'get', 'incrby', 'set', 'setnx'}

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
    self._script_cache = {}

  def _get_script_sha1(self, node, script):
    sha1, nodes = self._script_cache.setdefault(
      script, (hashlib.sha1(script).hexdigest(), set()))
    if node not in nodes:
      assert node.script_load(script) == sha1
      nodes.add(node)
    return sha1

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
    return self.name_to_node[self.ring(key)]

  def get_cache_key(self, key):
    if settings.DJREDIS_ENABLE_TAGGING:
      match = settings.DJREDIS_TAG_REGEX.match(key)
      if match:
        return '{%s}' % match.group(1)
    return key

  def _broadcast(self, attr, *args, **kwargs):
    response = {}
    # TODO(usmanm): Parallelize this?
    for name, node in self.name_to_node.iteritems():
      response[name] = getattr(node, attr)(*args, **kwargs)
    return response

  def _route(self, attr, *args, **kwargs):
    assert len(args) > 0 or len(kwargs) > 0
    return getattr(self.get_node(args[0]), attr)(*args, **kwargs)

  def _tag_route(self, attr, *args, **kwargs):
    assert len(args) > 0 or len(kwargs) > 0
    cache_key = self.get_cache_key(args[0])
    if cache_key != args[0]:
      attr = 'h%s' % attr # Call analagous hashes command.
      args = list(args)
      args.insert(0, cache_key)
    return getattr(self.get_node(cache_key), attr)(*args, **kwargs)

  def __getattr__(self, attr):
    if attr in RingClient.BROADCAST_METHODS:
      return functools.partial(self._broadcast, attr)
    if attr in RingClient.ROUTE_METHODS:
      return functools.partial(self._route, attr)
    if attr in RingClient.TAG_ROUTE_METHODS:
      return functools.partial(self._tag_route, attr)
    raise AttributeError("'%s' object has no attribute '%s'" %
                         (self.__class__.__name__, attr))

  def _get_node_to_key_map(self, keys):
    node_to_keys = defaultdict(lambda: defaultdict(list))
    for key in keys:
      cache_key = self.get_cache_key(key)
      if cache_key != key:
        node_to_keys[self.get_node(cache_key)][cache_key].append(key)
      else:
        node_to_keys[self.get_node(cache_key)][None].append(key)
    return node_to_keys

  def delete(self, *keys):
    node_to_keys = self._get_node_to_key_map(keys)
    count = 0
    for node, key_map in node_to_keys.iteritems():
      for bucket, keys in key_map.iteritems():
        if bucket is None:
          count += node.delete(*keys)
        else:
          count += node.hdel(bucket, *keys)
    return count

  def delete_tag(self, *tags):
    if not settings.DJREDIS_ENABLE_TAGGING:
      return
    keys_to_delete = []
    for tag in tags:
      if settings.DJREDIS_TAG_REGEX.match(tag):
        raise errors.InvalidKey('%s: a tag cannot contain a tag.' % tag)
      keys_to_delete.append('{%s}' % tag)
    node_to_keys = defaultdict(list)
    for key in keys_to_delete:
      node_to_keys[self.get_node(key)].append(key)
    num_deleted = 0
    for node, keys in node_to_keys.iteritems():
      num_deleted += node.delete(*keys)
    return num_deleted

  def mget(self, keys, *args):
    keys = _combine_into_list(keys, args)
    node_to_keys = self._get_node_to_key_map(keys)
    key_to_value = {}
    for node, key_map in node_to_keys.iteritems():
      for bucket, _keys in key_map.iteritems():
        if bucket is None:
          key_to_value.update(dict(zip(_keys, node.mget(_keys))))
        else:
          key_to_value.update(dict(zip(_keys, node.hmget(bucket, _keys))))
    return [key_to_value[key] for key in keys]

  def _set(self, key, value, nx=False, ex=False):
    cache_key = self.get_cache_key(key)
    node = self.get_node(cache_key)
    if cache_key == key:
      return node.set(key, value, nx=nx, ex=ex)
    if nx:
      value = node.hsetnx(cache_key, key, value)
    else:
      value = node.hset(cache_key, key, value)
    if ex:
      node.expire(cache_key, ex)
    return value

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
    hosts = list(hosts)
    shuffle(hosts) # Randomly sort sentinels before trying to bootstrap.
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
