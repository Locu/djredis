import time

from django.test import TestCase

from djredis.cache import RedisCache
from djredis.tests.runner import RedisRingRunner


class RingClientTestCase(TestCase):
  pass


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
