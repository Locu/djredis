# coding: utf-8

import types

from django.core.cache.backends.base import BaseCache
from django.core.exceptions import ImproperlyConfigured
from django.utils.encoding import smart_str

from djredis.utils import pickle
from djredis.utils.imports import import_by_path

# Stub object to ensure not passing in a `timeout` argument results in
# the default timeout
DEFAULT_TIMEOUT = object()


class RedisCache(BaseCache):
  def __init__(self, locations, params):
    super(RedisCache, self).__init__(params)
    if isinstance(locations, types.StringTypes):
      locations = locations.split(';')
    hosts = []
    for host in locations:
      if isinstance(host, types.StringTypes):
        host = host.strip().split(':')
      hosts.append(tuple(host))
    if not hosts:
      raise ImproperlyConfigured('`LOCATION` must provide at least one host.')
    options = params.get('OPTIONS', {})
    self.compress = options.get('COMPRESS', False)
    client_cls = import_by_path(options.get('CLIENT_CLASS',
                                            'djredis.client.RingClient'))
    self.client = client_cls(tuple(hosts), options)

  def make_key(self, key, version=None):
    return smart_str(super(RedisCache, self).make_key(key, version=version))

  def get_backend_timeout(self, timeout=DEFAULT_TIMEOUT):
    """
    Returns the timeout value usable by this backend based upon the provided
    timeout.
    """
    if timeout == DEFAULT_TIMEOUT:
      timeout = self.default_timeout
    if isinstance(timeout, float):
      timeout = int(timeout)
    return timeout

  def _set(self, key, value, timeout, version, add_only=False):
    timeout = self.get_backend_timeout(timeout)
    value = pickle.dumps(value, compress=self.compress)
    if timeout != None and timeout <= 0:
      return False
    kwargs = {}
    if add_only:
      kwargs['nx'] = True
    if timeout:
      kwargs['ex'] = timeout
    return bool(self.client.set(self.make_key(key, version=version), value,
                                **kwargs))

  def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
    """
    Set a value in the cache if the key does not already exist. If
    timeout is given, that timeout will be used for the key; otherwise
    the default cache timeout will be used.
    
    Returns True if the value was stored, False otherwise.
    """
    return self._set(key, value, timeout, version, add_only=True)

  def get(self, key, default=None, version=None):
    """
    Fetch a given key from the cache. If the key does not exist, return
    default, which itself defaults to None.
    """
    value = self.client.get(self.make_key(key, version=version))
    if value is None: # Key missing?
      return default
    return pickle.loads(value, compress=self.compress)

  def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
    """
    Set a value in the cache. If timeout is given, that timeout will be
    used for the key; otherwise the default cache timeout will be used.
    """
    return self._set(key, value, timeout, version)

  def delete(self, key, version=None):
    """
    Delete a key from the cache, failing silently.
    """
    return self.client.delete(self.make_key(key, version=version))

  def get_many(self, keys, version=None):
    """
    Fetch a bunch of keys from the cache.
    
    Returns a dict mapping each key in keys to its value. If the given
    key is missing, it will be missing from the response dict.
    """
    if not keys:
      return {}
    values = self.client.mget(self.make_key(key, version=version)
                              for key in keys)
    return {keys[i]: pickle.loads(values[i], compress=self.compress)
            for i in xrange(len(keys)) if values[i] is not None}

  def has_key(self, key, version=None):
    """
    Returns True if the key is in the cache and has not expired.
    """
    return self.client.exists(self.make_key(key, version=version))

  def incr(self, key, delta=1, version=None):
    """
    Add delta to value in the cache. If the key does not exist, raise a
    ValueError exception.
    """
    key = self.make_key(key, version=version)
    exists = self.client.exists(key)
    if not exists:
      raise ValueError
    return self.client.incr(key, delta)

  def decr(self, key, delta=1, version=None):
    """
    Subtract delta from value in the cache. If the key does not exist, raise
    a ValueError exception.
    """
    return self.incr(key, delta=-delta, version=version)

  def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
    """
    Set a bunch of values in the cache at once from a dict of key/value
    pairs.
    
    If timeout is given, that timeout will be used for the key; otherwise
    the default cache timeout will be used.
    """
    # TODO(usmanm): Make this faster using per-node *pipelines*.
    for key, value in data.iteritems():
      self.set(key, value, timeout=timeout, version=version)
      
  def delete_many(self, keys, version=None):
    """
    Set a bunch of values in the cache at once.  For certain backends
    (memcached), this is much more efficient than calling delete() multiple
    times.
    """
    return self.client.delete(*(self.make_key(key, version=version)
                                for key in keys))

  def clear(self):
    """Remove *all* values from the cache at once."""
    self.client.flushdb()

  def close(self, **kwargs):
    """Close the cache connection"""
    # TODO(usmanm): StrictRedis does connection pooling internally so I believe
    # this should be a no-op.
    pass
