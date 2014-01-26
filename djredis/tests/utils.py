# coding: utf-8

from collections import defaultdict

from django.core.exceptions import ImproperlyConfigured
from django.test import TestCase

from djredis.utils import pickle
from djredis.utils.hashring import HashRing
from djredis.utils.imports import import_by_path


class HashRingTestCase(TestCase):
  def test_ring_maps_keys_evenly_to_nodes(self):
    num_nodes = 10
    num_virtual_nodes = 100
    ring = HashRing(range(num_nodes), num_virtual_nodes)
    bins = defaultdict(int)
    num_keys = 10000
    for x in xrange(num_keys):
      _bin = ring.get_node('lolcat-%s' % x)
      self.assertTrue(0 <= _bin < num_nodes)
      bins[_bin] += 1
  
    for count in bins.itervalues():
      fraction_in_bin = count / float(num_keys)
      self.assertTrue(
        (0.8 / num_nodes) <= fraction_in_bin <= (1.2 / num_nodes) )

  def test_adding_a_node_only_changes_a_few_mappings(self):
    num_nodes = 10
    num_virtual_nodes = 100
    ring = HashRing(range(num_nodes), num_virtual_nodes)
    num_keys = 10000
    keys = ['lolcat-%s' % x for x in range(num_keys)]
    original_mapping = {(k, ring(k)) for k in keys}
    # Add a new node.
    ring.add_node(num_nodes)
    for key in keys:
      self.assertTrue(0 <= ring.get_node(key) < (num_nodes + 1))
    new_mapping = {(k, ring(k)) for k in keys}
    # Most of the mappings should be the same.
    self.assertTrue(
      len(original_mapping & new_mapping) > (0.8 * len(original_mapping)))
 
  def test_removing_a_node_only_changes_a_few_mappings(self):
    num_nodes = 10
    num_virtual_nodes = 100
    ring = HashRing(range(num_nodes), num_virtual_nodes)
    num_keys = 10000
    keys = ['lolcat-%s' % x for x in range(num_keys)]
    original_mapping = {(k, ring(k)) for k in keys}
    # Remove a node.
    ring.remove_node(0)
    for key in keys:
      self.assertTrue(0 < ring.get_node(key) < num_nodes)
    new_mapping = {(k, ring(k)) for k in keys}
    # Most of the mapping should be the same.
    self.assertTrue(
      len(original_mapping & new_mapping) > (0.8 * len(original_mapping)))

  def test_add_node_keeps_list_sorted(self):
    num_nodes = 10
    ring = HashRing(range(num_nodes), 100)
    ring.add_node(num_nodes)
    self.assertTrue(
      sorted(ring._sorted_virtual_nodes) == ring._sorted_virtual_nodes)

  def test_remove_node_keeps_list_sorted(self):
    num_nodes = 10
    ring = HashRing(range(num_nodes), 100)
    ring.remove_node(num_nodes / 2)
    self.assertTrue(
      sorted(ring._sorted_virtual_nodes) == ring._sorted_virtual_nodes)


class ImportsTestCase(TestCase):
  def test_import_by_path(self):
    self.assertTrue(import_by_path('os.path'))
    self.assertTrue(import_by_path('djredis.client.RingClient'))
    self.assertRaises(ImproperlyConfigured, import_by_path, 'lolcat.djredis')


class PickleTestCase(TestCase):
  def test_integers(self):
    self.assertEqual(pickle.dumps(1), 1)
    self.assertEqual(pickle.dumps(1.0), 1)
    self.assertNotEqual(pickle.dumps('1'), 1)
    self.assertEqual(pickle.loads(1), 1)

  def test_simple(self):
    self.assertNotEqual(pickle.dumps('lolcat'), 'lolcat')
    self.assertEqual(pickle.loads(pickle.dumps('lolcat')), 'lolcat')
    self.assertEqual(pickle.loads(None), None)

  def test_compress(self):
    self.assertTrue(len(pickle.dumps('lolcat'*10)) >
                    len(pickle.dumps('lolcat'*10, compress=True)))
