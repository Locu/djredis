# coding: utf-8

from importlib import import_module

from django.core.exceptions import ImproperlyConfigured


def import_by_path(dotted_path):
  """
  Import a dotted module path and return the attribute designated by the
  last name in the path.
  """
  try:
    module_path, attr_name = dotted_path.rsplit('.', 1)
  except ValueError:
    raise ImproperlyConfigured("`%s` doesn't look like a module path." % 
                               dotted_path)
  try:
    module = import_module(module_path)
  except ImportError:
    raise ImproperlyConfigured("Couldn't import module `%s`." % module_path)
  try:
    return getattr(module, attr_name)
  except AttributeError:
    raise ImproperlyConfigured("`%s` module doesn't have a `%s` attribute." % (
        module_path, attr_name))
