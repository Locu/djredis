# coding: utf-8

class DJRedisError(Exception):
  pass

class InvalidKey(DJRedisError):
  pass

class MastersListUnavailable(DJRedisError):
  pass

class NoMastersConfigured(DJRedisError):
  pass
