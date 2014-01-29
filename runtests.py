#!/usr/bin/env python
# coding: utf-8
import logging
import sys

from argparse import ArgumentParser
from os.path import abspath
from os.path import dirname

# Modify the `PATH` so that our djredis app is in it.
parent_dir = dirname(abspath(__file__))
sys.path.insert(0, parent_dir)

# Load Django-related settings; necessary for tests to run and for Django
# imports to work.
import local_settings; local_settings

# Now, imports from Django will work properly without raising errors related to
# missing or badly-configured settings.
from django.test.simple import DjangoTestSuiteRunner
from django.conf import settings

def runtests(verbosity, failfast, interactive, test_labels):
  if 'south' in settings.INSTALLED_APPS:
    # This tells South to run the migrations after syncdb. See:
    # http://blogs.terrorware.com/geoff/2012/03/05/making-sure-south-migrations-get-run-when-using-djangos-create_test_db/
    from south.management.commands import patch_for_test_db_setup
    patch_for_test_db_setup()

  from djredis.tests import import_tests; import_tests()
  logging.getLogger('djredis').addHandler(logging.NullHandler())

  test_runner = DjangoTestSuiteRunner(
      verbosity=verbosity,
      interactive=interactive,
      failfast=failfast)

  sys.exit(test_runner.run_tests(test_labels))

if __name__ == '__main__':
  # Parse any command line arguments.
  parser = ArgumentParser()
  parser.add_argument('--failfast',
                      action='store_true',
                      default=False,
                      dest='failfast')
  parser.add_argument('--interactive',
                      action='store_true',
                      default=False,
                      dest='interactive')
  parser.add_argument('--verbosity', default=1, type=int)
  parser.add_argument('test_labels', nargs='*', default=('djredis',))

  args = parser.parse_args()

  # Run the tests.
  runtests(args.verbosity, args.failfast, args.interactive, args.test_labels)
