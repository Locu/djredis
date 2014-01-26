# coding: utf-8

import pickle
import time

from functools import wraps

from django.test import TestCase

from djredis.cache import RedisCache
from djredis.tests.runner import RedisRingRunner


# Helper datastructures for tests.
def f():
  return 42

class C:
  def m(n):
    return 24

class Unpickable(object):
  def __getstate__(self):
    raise pickle.PickleError()


class RedisCacheTestCase(TestCase):
  """
  Most of these tests are copied from Django's BaseCacheTests which
  can be found at:
  https://github.com/django/django/blob/master/tests/cache/tests.py
  """
  TEST_SETUPS = {
    'ring': '_setup_ring',
    'sentinel_ring': '_setup_sentinel_ring'
    }

  @staticmethod
  def _run_for_clients(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
      for client in RedisCacheTestCase.TEST_SETUPS:
        getattr(self, RedisCacheTestCase.TEST_SETUPS[client])()
        func(self, *args, **kwargs)
        self.runner.stop()
    return wrapper

  def _setup_ring(self):
    self.runner = RedisRingRunner()
    self.runner.start()
    self.cache = RedisCache(
      'localhost:9500; localhost:9501; localhost:9502',
      {'OPTIONS': {'CLIENT_CLASS': 'djredis.client.RingClient'}})

  def _setup_sentinel_ring(self):
    self.runner = RedisRingRunner(num_sentinels=3)
    self.runner.start()
    self.cache = RedisCache(
      'localhost:9700; localhost:9701; localhost:9702',
      {'OPTIONS': {'CLIENT_CLASS': 'djredis.client.SentinelBackedRingClient'}})

  @classmethod
  def setUpClass(cls):
    # Wrap all test functions so that we run them against all cache types.
    for attr_name in cls.__dict__:
      attr = getattr(cls, attr_name)
      if attr_name.startswith('test_') and callable(attr):
        setattr(cls, attr_name, RedisCacheTestCase._run_for_clients(attr))

  def test_simple(self):
    # Simple cache set/get works
    self.cache.set('key', 'value')
    self.assertEqual(self.cache.get('key'), 'value')

  def test_add(self):
    # A key can be added to a cache
    self.cache.add('addkey1', 'value')
    result = self.cache.add('addkey1', 'newvalue')
    self.assertEqual(result, False)
    self.assertEqual(self.cache.get('addkey1'), 'value')
    
  def test_non_existent(self):
    # Non-existent cache keys return as None/default
    # get with non-existent keys
    self.assertEqual(self.cache.get('does_not_exist'), None)
    self.assertEqual(self.cache.get('does_not_exist', 'bang!'), 'bang!')

  def test_get_many(self):
    # Multiple cache keys can be returned using get_many
    self.cache.set('a', 'a')
    self.cache.set('b', 'b')
    self.cache.set('c', 'c')
    self.cache.set('d', 'd')
    self.assertEqual(self.cache.get_many(['a', 'c', 'd']),
                     {'a': 'a', 'c': 'c', 'd': 'd'})
    self.assertEqual(self.cache.get_many(['a', 'b', 'e']),
                     {'a': 'a', 'b': 'b'})

  def test_delete(self):
    # Cache keys can be deleted
    self.cache.set('key1', 'spam')
    self.cache.set('key2', 'eggs')
    self.assertEqual(self.cache.get('key1'), 'spam')
    self.cache.delete('key1')
    self.assertEqual(self.cache.get('key1'), None)
    self.assertEqual(self.cache.get('key2'), 'eggs')

  def test_has_key(self):
    # The cache can be inspected for cache keys
    self.cache.set('hello1', 'goodbye1')
    self.assertEqual(self.cache.has_key('hello1'), True)
    self.assertEqual(self.cache.has_key('goodbye1'), False)

  def test_in(self):
    # The in operator can be used to inspect cache contents
    self.cache.set('hello2', 'goodbye2')
    self.assertEqual('hello2' in self.cache, True)
    self.assertEqual('goodbye2' in self.cache, False)

  def test_incr(self):
    # Cache values can be incremented
    self.cache.set('answer', 41)
    self.assertEqual(self.cache.incr('answer'), 42)
    self.assertEqual(self.cache.get('answer'), 42)
    self.assertEqual(self.cache.incr('answer', 10), 52)
    self.assertEqual(self.cache.get('answer'), 52)
    self.assertEqual(self.cache.incr('answer', -10), 42)
    self.assertRaises(ValueError, self.cache.incr, 'does_not_exist')

  def test_decr(self):
    # Cache values can be decremented
    self.cache.set('answer', 43)
    self.assertEqual(self.cache.decr('answer'), 42)
    self.assertEqual(self.cache.get('answer'), 42)
    self.assertEqual(self.cache.decr('answer', 10), 32)
    self.assertEqual(self.cache.get('answer'), 32)
    self.assertEqual(self.cache.decr('answer', -10), 42)
    self.assertRaises(ValueError, self.cache.decr, 'does_not_exist')

  def test_close(self):
    self.assertTrue(hasattr(self.cache, 'close'))
    self.cache.close()

  def test_data_types(self):
    # Many different data types can be cached
    stuff = {
      'string': 'this is a string',
      'int': 42,
      'list': [1, 2, 3, 4],
      'tuple': (1, 2, 3, 4),
      'dict': {'A': 1, 'B': 2},
      'function': f,
      'class': C,
      }
    self.cache.set('stuff', stuff)
    self.assertEqual(self.cache.get('stuff'), stuff)

  def test_expiration(self):
    # Cache values can be set to expire
    self.cache.set('expire1', 'very quickly', 1)
    self.cache.set('expire2', 'very quickly', 1)
    self.cache.set('expire3', 'very quickly', 1)
    
    time.sleep(2)
    self.assertEqual(self.cache.get('expire1'), None)
    
    self.cache.add('expire2', 'newvalue')
    self.assertEqual(self.cache.get('expire2'), 'newvalue')
    self.assertEqual(self.cache.has_key('expire3'), False)

  def test_unicode(self):
    # Unicode values can be cached
    stuff = {
      'ascii': 'ascii_value',
      'unicode_ascii': 'Iñtërnâtiônàlizætiøn1',
      'Iñtërnâtiônàlizætiøn': 'Iñtërnâtiônàlizætiøn2',
      'ascii2': {'x': 1}
      }
    # Test `set`
    for (key, value) in stuff.items():
      self.cache.set(key, value)
      self.assertEqual(self.cache.get(key), value)
      
    # Test `add`
    for (key, value) in stuff.items():
      self.cache.delete(key)
      self.cache.add(key, value)
      self.assertEqual(self.cache.get(key), value)

    # Test `set_many`
    for (key, value) in stuff.items():
      self.cache.delete(key)
      self.cache.set_many(stuff)
    for (key, value) in stuff.items():
      self.assertEqual(self.cache.get(key), value)

  def test_binary_string(self):
    # Binary strings should be cacheable
    from zlib import compress, decompress
    value = 'value_to_be_compressed'
    compressed_value = compress(value.encode())
    
    # Test set
    self.cache.set('binary1', compressed_value)
    compressed_result = self.cache.get('binary1')
    self.assertEqual(compressed_value, compressed_result)
    self.assertEqual(value, decompress(compressed_result).decode())

    # Test add
    self.cache.add('binary1-add', compressed_value)
    compressed_result = self.cache.get('binary1-add')
    self.assertEqual(compressed_value, compressed_result)
    self.assertEqual(value, decompress(compressed_result).decode())

    # Test set_many
    self.cache.set_many({'binary1-set_many': compressed_value})
    compressed_result = self.cache.get('binary1-set_many')
    self.assertEqual(compressed_value, compressed_result)
    self.assertEqual(value, decompress(compressed_result).decode())

  def test_set_many(self):
    # Multiple keys can be set using set_many
    self.cache.set_many({'key1': 'spam', 'key2': 'eggs'})
    self.assertEqual(self.cache.get('key1'), 'spam')
    self.assertEqual(self.cache.get('key2'), 'eggs')

  def test_set_many_expiration(self):
    # set_many takes a second ``timeout`` parameter
    self.cache.set_many({'key1': 'spam', 'key2': 'eggs'}, 1)
    time.sleep(2)
    self.assertEqual(self.cache.get('key1'), None)
    self.assertEqual(self.cache.get('key2'), None)
    
  def test_delete_many(self):
    # Multiple keys can be deleted using delete_many
    self.cache.set('key1', 'spam')
    self.cache.set('key2', 'eggs')
    self.cache.set('key3', 'ham')
    self.cache.delete_many(['key1', 'key2'])
    self.assertEqual(self.cache.get('key1'), None)
    self.assertEqual(self.cache.get('key2'), None)
    self.assertEqual(self.cache.get('key3'), 'ham')

  def test_clear(self):
    # The cache can be emptied using clear
    self.cache.set('key1', 'spam')
    self.cache.set('key2', 'eggs')
    self.cache.clear()
    self.assertEqual(self.cache.get('key1'), None)
    self.assertEqual(self.cache.get('key2'), None)

  def test_long_timeout(self):
    '''
    Using a timeout greater than 30 days makes memcached think
    it is an absolute expiration timestamp instead of a relative
    offset. Test that we honour this convention. Refs #12399.
    '''
    self.cache.set('key1', 'eggs', 60 * 60 * 24 * 30 + 1)  # 30 days + 1 second
    self.assertEqual(self.cache.get('key1'), 'eggs')
    
    self.cache.add('key2', 'ham', 60 * 60 * 24 * 30 + 1)
    self.assertEqual(self.cache.get('key2'), 'ham')
    
    self.cache.set_many({'key3': 'sausage', 'key4': 'lobster bisque'},
                        60 * 60 * 24 * 30 + 1)
    self.assertEqual(self.cache.get('key3'), 'sausage')
    self.assertEqual(self.cache.get('key4'), 'lobster bisque')
    
  def test_forever_timeout(self):
    '''
    Passing in None into timeout results in a value that is cached forever
    '''
    self.cache.set('key1', 'eggs', None)
    self.assertEqual(self.cache.get('key1'), 'eggs')
    
    self.cache.add('key2', 'ham', None)
    self.assertEqual(self.cache.get('key2'), 'ham')
    
    self.cache.set_many({'key3': 'sausage', 'key4': 'lobster bisque'}, None)
    self.assertEqual(self.cache.get('key3'), 'sausage')
    self.assertEqual(self.cache.get('key4'), 'lobster bisque')

  def test_zero_timeout(self):
    '''
    Passing in 0 into timeout results in a value that is never cached
    '''
    self.cache.set('key1', 'eggs', 0)
    self.assertEqual(self.cache.get('key1'), None)
    
    self.cache.add('key2', 'ham', 0)
    self.assertEqual(self.cache.get('key2'), None)
    
    self.cache.set_many({'key3': 'sausage', 'key4': 'lobster bisque'}, 0)
    self.assertEqual(self.cache.get('key3'), None)
    self.assertEqual(self.cache.get('key4'), None)
    
  def test_float_timeout(self):
    # Make sure a timeout given as a float doesn't crash anything.
    self.cache.set('key1', 'spam', 100.2)
    self.assertEqual(self.cache.get('key1'), 'spam')
    
  def test_cache_versioning_get_set(self):
    # set, using default version = 1
    self.cache.set('answer1', 42)
    self.assertEqual(self.cache.get('answer1'), 42)
    self.assertEqual(self.cache.get('answer1', version=1), 42)
    self.assertEqual(self.cache.get('answer1', version=2), None)
    
    # set, default version = 1, but manually override version = 2
    self.cache.set('answer2', 42, version=2)
    self.assertEqual(self.cache.get('answer2'), None)
    self.assertEqual(self.cache.get('answer2', version=1), None)
    self.assertEqual(self.cache.get('answer2', version=2), 42)

  def test_cache_versioning_add(self):
    # add, default version = 1, but manually override version = 2
    self.cache.add('answer1', 42, version=2)
    self.assertEqual(self.cache.get('answer1', version=1), None)
    self.assertEqual(self.cache.get('answer1', version=2), 42)
    
    self.cache.add('answer1', 37, version=2)
    self.assertEqual(self.cache.get('answer1', version=1), None)
    self.assertEqual(self.cache.get('answer1', version=2), 42)

    self.cache.add('answer1', 37, version=1)
    self.assertEqual(self.cache.get('answer1', version=1), 37)
    self.assertEqual(self.cache.get('answer1', version=2), 42)

  def test_cache_versioning_has_key(self):
    self.cache.set('answer1', 42)
    self.assertTrue(self.cache.has_key('answer1'))
    self.assertTrue(self.cache.has_key('answer1', version=1))
    self.assertFalse(self.cache.has_key('answer1', version=2))

  def test_cache_versioning_delete(self):
    self.cache.set('answer1', 37, version=1)
    self.cache.set('answer1', 42, version=2)
    self.cache.delete('answer1')
    self.assertEqual(self.cache.get('answer1', version=1), None)
    self.assertEqual(self.cache.get('answer1', version=2), 42)
    
    self.cache.set('answer2', 37, version=1)
    self.cache.set('answer2', 42, version=2)
    self.cache.delete('answer2', version=2)
    self.assertEqual(self.cache.get('answer2', version=1), 37)
    self.assertEqual(self.cache.get('answer2', version=2), None)

  def test_cache_versioning_incr_decr(self):
    self.cache.set('answer1', 37, version=1)
    self.cache.set('answer1', 42, version=2)
    self.cache.incr('answer1')
    self.assertEqual(self.cache.get('answer1', version=1), 38)
    self.assertEqual(self.cache.get('answer1', version=2), 42)
    self.cache.decr('answer1')
    self.assertEqual(self.cache.get('answer1', version=1), 37)
    self.assertEqual(self.cache.get('answer1', version=2), 42)

    self.cache.set('answer2', 37, version=1)
    self.cache.set('answer2', 42, version=2)
    self.cache.incr('answer2', version=2)
    self.assertEqual(self.cache.get('answer2', version=1), 37)
    self.assertEqual(self.cache.get('answer2', version=2), 43)
    self.cache.decr('answer2', version=2)
    self.assertEqual(self.cache.get('answer2', version=1), 37)
    self.assertEqual(self.cache.get('answer2', version=2), 42)

  def test_cache_versioning_get_set_many(self):
    # set, using default version = 1
    self.cache.set_many({'ford1': 37, 'arthur1': 42})
    self.assertEqual(self.cache.get_many(['ford1', 'arthur1']),
                     {'ford1': 37, 'arthur1': 42})
    self.assertEqual(self.cache.get_many(['ford1', 'arthur1'], version=1),
                     {'ford1': 37, 'arthur1': 42})
    self.assertEqual(self.cache.get_many(['ford1', 'arthur1'], version=2), {})

    # set, default version = 1, but manually override version = 2
    self.cache.set_many({'ford2': 37, 'arthur2': 42}, version=2)
    self.assertEqual(self.cache.get_many(['ford2', 'arthur2']), {})
    self.assertEqual(self.cache.get_many(['ford2', 'arthur2'], version=1), {})
    self.assertEqual(self.cache.get_many(['ford2', 'arthur2'], version=2),
                     {'ford2': 37, 'arthur2': 42})

  def test_incr_version(self):
    self.cache.set('answer', 42, version=2)
    self.assertEqual(self.cache.get('answer'), None)
    self.assertEqual(self.cache.get('answer', version=1), None)
    self.assertEqual(self.cache.get('answer', version=2), 42)
    self.assertEqual(self.cache.get('answer', version=3), None)
    
    self.assertEqual(self.cache.incr_version('answer', version=2), 3)
    self.assertEqual(self.cache.get('answer'), None)
    self.assertEqual(self.cache.get('answer', version=1), None)
    self.assertEqual(self.cache.get('answer', version=2), None)
    self.assertEqual(self.cache.get('answer', version=3), 42)

  def test_decr_version(self):
    self.cache.set('answer', 42, version=2)
    self.assertEqual(self.cache.get('answer'), None)
    self.assertEqual(self.cache.get('answer', version=1), None)
    self.assertEqual(self.cache.get('answer', version=2), 42)
    
    self.assertEqual(self.cache.decr_version('answer', version=2), 1)
    self.assertEqual(self.cache.get('answer'), 42)
    self.assertEqual(self.cache.get('answer', version=1), 42)
    self.assertEqual(self.cache.get('answer', version=2), None)
      
  def test_set_fail_on_pickleerror(self):
    # See https://code.djangoproject.com/ticket/21200
    with self.assertRaises(pickle.PickleError):
      self.cache.set('unpickable', Unpickable())
