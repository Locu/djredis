# coding: utf-8

import time

from django.test import TestCase

from djredis.cache import RedisCache
from djredis.conf import settings
from djredis.tests.runner import RedisRingRunner
from djredis.utils import pickle


class RingClientTestCase(TestCase):
  def setUp(self):
    self.runner = RedisRingRunner(num_nodes=3)
    self.runner.start()
    self.cache = RedisCache(
      'localhost:9500; localhost:9501; localhost:9502',
      {'OPTIONS': {'CLIENT_CLASS': 'djredis.client.RingClient'}})

  def tearDown(self):
    self.cache.close()
    self.runner.stop()


  def test_tags(self):
    settings.DJREDIS_ENABLE_TAGGING = True
    
    self.assertEqual(self.cache.client.get_cache_key('{mytag}-key1'),
                     '{mytag}')

    self.cache.set('{mytag}-key1', 'helloworld1')
    self.cache.set('{mytag}-key2', 'helloworld2')
    self.cache.set('{mytag}-key3', 'helloworld3')
    self.assertEqual(self.cache.get('{mytag}-key1'), 'helloworld1')
    self.assertEqual(self.cache.get('{mytag}-key2'), 'helloworld2')
    self.assertEqual(self.cache.get('{mytag}-key3'), 'helloworld3')

    node = self.cache.client.get_node('{mytag}')
    self.assertEqual(self.cache.client.keys(), ['{mytag}'])
    self.assertEqual(node.hlen('{mytag}'), 3)
    self.assertEqual(
      pickle.loads(node.hget('{mytag}', self.cache.make_key('{mytag}-key1'))),
      'helloworld1')
    self.assertEqual(
      pickle.loads(node.hget('{mytag}', self.cache.make_key('{mytag}-key2'))),
      'helloworld2')
    self.assertEqual(
      pickle.loads(node.hget('{mytag}', self.cache.make_key('{mytag}-key3'))),
      'helloworld3')

    self.assertEqual(self.cache.delete('{mytag}-key1'), 1)
    self.assertEqual(node.hlen('{mytag}'), 2)
    self.assertEqual(set(node.hkeys('{mytag}')),
                     set((self.cache.make_key('{mytag}-key2'),
                          self.cache.make_key('{mytag}-key3'))))
    self.cache.client.delete_tag('mytag') # Should just delete hash key.
    self.assertEqual(self.cache.client.keys(), [])

    self.cache.set('{mytag1}-key', 'helloworld1')
    self.cache.set('{mytag2}-key', 'helloworld2')
    self.assertEqual(self.cache.get('{mytag1}-key'), 'helloworld1')
    self.assertEqual(self.cache.get('{mytag2}-key'), 'helloworld2')
    self.assertEqual(set(self.cache.client.keys()),
                     set(['{mytag1}', '{mytag2}']))
    self.assertEqual(self.cache.client.delete_tag('mytag1', 'mytag2'), 2)
    self.assertEqual(self.cache.client.keys(), [])


class SentinelBackedRingClientTestCase(TestCase):
  def setUp(self):
    self.runner = RedisRingRunner(num_nodes=3, num_sentinels=3)
    self.runner.start()
    self.recreate_cache()
    self.wait()

  def tearDown(self):
    self.cache.close()
    self.runner.stop()

  def wait(self):
    """ Wait long enough for everything to come back to consistent state. """
    time.sleep(8)

  def recreate_cache(self):
    if hasattr(self, 'cache'):
      self.cache.close()
    self.cache = RedisCache(
      'localhost:9700; localhost:9701; localhost:9702',
      {'OPTIONS': {'CLIENT_CLASS': 'djredis.client.SentinelBackedRingClient'}})

  def test_dead_sentinel(self):
    ping = self.cache.client.ping()
    self.assertEqual(len(ping), 3)
    self.assertTrue(all(value for value in ping.itervalues()))
    self.cache.close()
    self.runner.stop_sentinel(0) # Kill first sentinel
    self.recreate_cache()
    ping = self.cache.client.ping()
    self.assertEqual(len(ping), 3)
    self.assertTrue(all(value for value in ping.itervalues()))
    self.runner.start_sentinel(0)

  def test_master_failure(self):
    self.cache.client.set('lol', 'cat')
    node = self.cache.client.ring('lol')
    master_index = int(node.lstrip('mymaster'))
    # Ensure that sentinel cluster sees the master and its slave.
    self.assertEqual(self.cache.client.sentinel.discover_master(node),
                     ('127.0.0.1', 9500 + master_index))
    self.assertEqual(self.cache.client.sentinel.discover_slaves(node),
                     [('127.0.0.1', 9600 + master_index)])
    # Ensure that we have the correct value on the master.
    self.assertTrue(self.cache.client.get('lol'), 'cat')
    # Kill the master.
    self.runner.stop_master(master_index)
    self.wait()
    # Ensure that master fail-overed to the slave. Since old master is down,
    # the new master does not have any slaves.
    self.assertEqual(self.cache.client.sentinel.discover_master(node),
                     ('127.0.0.1', 9600 + master_index))
    self.assertEqual(self.cache.client.sentinel.discover_slaves(node),
                     [])
    # The new master still has the right value?
    self.assertTrue(self.cache.client.get('lol'), 'cat')
    # Bring back old master.
    self.runner.start_master(master_index)
    self.wait()
    # The new master should have the old master as its slave.
    self.assertEqual(self.cache.client.sentinel.discover_master(node),
                     ('127.0.0.1', 9600 + master_index))
    self.assertEqual(self.cache.client.sentinel.discover_slaves(node),
                     [('127.0.0.1', 9500 + master_index)])
    # Just being pedantic, no reason this value should have changed.
    self.assertTrue(self.cache.client.get('lol'), 'cat')

  def test_master_and_sentinel_failure(self):
    self.cache.client.set('lol', 'cat')
    node = self.cache.client.ring('lol')
    master_index = int(node.lstrip('mymaster'))
    # Ensure that sentinel cluster sees the master and its slave.
    self.assertEqual(self.cache.client.sentinel.discover_master(node),
                     ('127.0.0.1', 9500 + master_index))
    self.assertEqual(self.cache.client.sentinel.discover_slaves(node),
                     [('127.0.0.1', 9600 + master_index)])
    # Ensure that we have the correct value on the master.
    self.assertTrue(self.cache.client.get('lol'), 'cat')
    # Kill the master and a sentinel.
    self.runner.stop_master(master_index)
    self.runner.stop_sentinel(0)
    self.recreate_cache()
    self.wait()
    # Ensure that master fail-overed to the slave. Since old master is down,
    # the new master does not have any slaves.
    self.assertEqual(self.cache.client.sentinel.discover_master(node),
                     ('127.0.0.1', 9600 + master_index))
    self.assertEqual(self.cache.client.sentinel.discover_slaves(node),
                     [])
    # The new master still has the right value?
    self.assertTrue(self.cache.client.get('lol'), 'cat')
    # Bring back old master and sentinel.
    self.runner.start_master(master_index)
    self.runner.start_sentinel(0)
    self.wait()
    # The new master should have the old master as its slave.
    self.assertEqual(self.cache.client.sentinel.discover_master(node),
                     ('127.0.0.1', 9600 + master_index))
    self.assertEqual(self.cache.client.sentinel.discover_slaves(node),
                     [('127.0.0.1', 9500 + master_index)])
    # Just being pedantic, no reason this value should have changed.
    self.assertTrue(self.cache.client.get('lol'), 'cat')
