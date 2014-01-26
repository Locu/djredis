# coding: utf-8

import hashlib
import bisect


class HashRing(object):
  """
  A simple consistent hashing implementation.
  
  See the original paper:
  http://thor.cs.ucsb.edu/~ravenben/papers/coreos/KLL+97.pdf
  """
  def __init__(self, nodes, num_virtual_nodes=100):
    assert len(nodes) > 0

    self.nodes = set()
    self.num_virtual_nodes = num_virtual_nodes
    self._hash_to_node = {}
    self._sorted_virtual_nodes = []

    for node in nodes:
      self.add_node(node)

  @staticmethod
  def _generate_hash(key):
    return hashlib.md5(str(key)).hexdigest()

  def add_node(self, node):
    if node in self.nodes:
      return
    self.nodes.add(node)
    for virtual_node in xrange(self.num_virtual_nodes):
      key = HashRing._generate_hash('%s:%s' % (str(node), virtual_node))
      self._hash_to_node[key] = node
      bisect.insort(self._sorted_virtual_nodes, key)

  def remove_node(self, node):
    if not node in self.nodes:
      return
    self.nodes.remove(node)
    for virtual_node in xrange(self.num_virtual_nodes):
      key = HashRing._generate_hash('%s:%s' % (str(node), virtual_node))
      del self._hash_to_node[key]
      idx = bisect.bisect_left(self._sorted_virtual_nodes, key)
      assert self._sorted_virtual_nodes[idx] == key
      del self._sorted_virtual_nodes[idx]

  def get_node(self, key):
    if not self.nodes:
      return None
    idx = bisect.bisect(self._sorted_virtual_nodes,
                        HashRing._generate_hash(key))
    node_key = self._sorted_virtual_nodes[idx %
                                          len(self._sorted_virtual_nodes)]
    return self._hash_to_node[node_key]

  def __call__(self, key):
    return self.get_node(key)
