from __future__ import absolute_import

try:
  import cPickle as pickle
except ImportError:
  import pickle
import zlib

from django.utils.encoding import smart_str


def loads(value, compress=False):
  # Integers are not pickled when storing in the cache because we allow
  # methods like incr/decr which would fail on pickled values.
  if value is None:
    return None
  try:
    return int(value)
  except ValueError:
    pass
  if compress:
    value = zlib.decompress(value)
  # TODO(usmanm): Is this needed?
  value = smart_str(value)
  return pickle.loads(value)

def dumps(value, compress=False):
  # Don't pickle integers (pickled integers will fail with incr/decr). Plus
  # pickling integers wastes memory. Typecast floats to ints and don't pickle
  # if you lose precision from the typecast.
  if isinstance(value, int):
    return value
  if isinstance(value, float) and int(value) == value:
    return int(value)
  value = pickle.dumps(value)
  if compress:
    value = zlib.compress(value)
  return value
