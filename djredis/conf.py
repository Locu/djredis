# coding: utf-8
import re

from appconf import AppConf
from django.conf import settings # nopa


class DJRedisConf(AppConf):
  class Meta:
    prefix = 'djredis'

  ENABLE_TAGGING = False
  TAG_REGEX = re.compile('.*\{(.*)\}.*', re.I)
