# coding: utf-8

import importlib
import inspect
import sys

from django.test import TestCase

TEST_MODULES = ['cache', 'client', 'utils']

def import_tests():
  """
  This function dynamically imports all the tests that need to be run for
  djredis.
  """
  current_module = sys.modules[__name__]
  for name in TEST_MODULES:
    module = importlib.import_module('djredis.tests.%s' % name)
    assert inspect.ismodule(module)
    for name, attribute in inspect.getmembers(module):
      if inspect.isclass(attribute) and issubclass(attribute, TestCase):
        setattr(current_module, name, attribute)
