# coding: utf-8

import itertools
import math
import os
import redis
import subprocess
import sys
import tempfile
import time


class RunnerError(Exception):
  pass


class Runner(object):
  def start(self):
    raise NotImplemented

  def stop(self):
    raise NotImplemented

  def restart(self):
    try:
      self.stop()
    except RunnerError:
      pass
    finally:
      self.start()


class ProcessRunner(Runner):
  def __init__(self, args, verbose=False):
    """
    Runs a shell command in a separate process.
    `args` - list of args that represent the shell command.
    """
    self.args = args
    self.verbose = verbose
    self._sub_proc = None

  def wait(self):
    pass

  def start(self):
    if self._sub_proc:
      raise RunnerError('Process already running.')
    if self.verbose:
      stdout = sys.stdout
      stderr = sys.stderr
    else:
      stdout = stderr = open(os.devnull, 'w')
    self._sub_proc = subprocess.Popen(self.args,
                                      stdout=stdout,
                                      stderr=stderr)
    self.wait()
  
  def stop(self):
    if not self._sub_proc:
      raise RunnerError('Process not running.')
    self._sub_proc.terminate()
    self._sub_proc.wait()
    self._sub_proc = None


class RedisRunner(ProcessRunner):
  def __init__(self, port, redis_server_path='redis-server', **kwargs):
    self.port = port
    super(RedisRunner, self).__init__([redis_server_path,
                                       '--port', str(self.port)],
                                      **kwargs)

  @staticmethod
  def probe(port):
    for i in xrange(50):
      try:
        connection = redis.Connection('localhost', port)
        connection.connect()
        connection.disconnect()
        return True
      except redis.ConnectionError:
        time.sleep(0.05) # Wait 50ms between each *probe*.
    return False

  def wait(self):
    if not RedisRunner.probe(self.port):
      raise RunnerError('RedisRunner failed to start.')


class RedisSentinelRunner(ProcessRunner):
  def __init__(self, port, sentinel_conf, redis_server_path='redis-server',
               **kwargs):
    # Create temporary sentinel config file.
    self._tmp_file = tempfile.NamedTemporaryFile(delete=False)
    self._tmp_file.write('\n'.join(line for line in sentinel_conf))
    self._tmp_file.close()
    self.port = port
    super(RedisSentinelRunner, self).__init__([redis_server_path,
                                               self._tmp_file.name,
                                               '--sentinel',
                                               '--port', str(self.port)],
                                              **kwargs)

  def wait(self):
    # Wait for Sentinel to start.
    if not RedisRunner.probe(self.port):
      raise RunnerError('RedisSentinelRunner failed to start.')

  def __del__(self):
    # Clean up temp file when the runner is being GC'd.
    try:
      os.unlink(self._tmp_file.name)
    except OSError:
      pass


class RedisRingRunner(Runner):
  MASTER_PORT = 9500
  SLAVE_PORT = 9600
  SENTINEL_PORT = 9700

  def __init__(self, redis_server_path='redis-server', num_nodes=3,
               num_sentinels=0):
    num_nodes = min(num_nodes, 15) # Don't allow more than 15 nodes
    num_sentinels = min(num_sentinels, 5) # Don't allow more than 5 sentinels
    self._masters = []
    self._slaves = []
    self._sentinels = []

    # Set up master nodes.
    for i in xrange(num_nodes):
      self._masters.append(RedisRunner(RedisRingRunner.MASTER_PORT + i,
                                       redis_server_path=redis_server_path))

    if num_sentinels:
      sentinel_conf = []
      quorum = math.ceil(num_sentinels / 2.0)
      for i in xrange(num_nodes):
        sentinel_conf.append('sentinel monitor mymaster%s 127.0.0.1 %s %s' %
                             (i, RedisRingRunner.MASTER_PORT + i, quorum))
        sentinel_conf.append('sentinel parallel-syncs mymaster%s 1' % i)
        sentinel_conf.append('sentinel down-after-milliseconds mymaster%s 2000'
                             % i)
        self._slaves.append(RedisRunner(RedisRingRunner.SLAVE_PORT + i,
                                        redis_server_path=redis_server_path))
      for i in xrange(num_sentinels):
        self._sentinels.append(
          RedisSentinelRunner(RedisRingRunner.SENTINEL_PORT + i,
                              sentinel_conf,
                              redis_server_path=redis_server_path)
          )

  def start(self):
    for master in self._masters:
      master.start()
    if self._sentinels:
      for slave in self._slaves:
        slave.start()
      for i in xrange(len(self._slaves)):
        # Make slaves follow thier masters.
        client = redis.StrictRedis(port=RedisRingRunner.SLAVE_PORT + i)
        client.slaveof(host='localhost', port=RedisRingRunner.MASTER_PORT + i)
      for sentinel in self._sentinels:
        sentinel.start()
      # Wait for Sentinels to reach quorum for each master.
      time.sleep(0.75 * len(self._masters))

  def stop(self):
    for runner in itertools.chain(self._sentinels,
                                  self._masters,
                                  self._slaves):
      runner.stop()

  def start_master(self, i):
    assert 0 <= i < len(self._masters)
    self._masters[i].start()

  def stop_master(self, i):
    assert 0 <= i < len(self._masters)
    self._masters[i].stop()

  def start_slave(self, i):
    assert 0 <= i < len(self._slaves)
    self._slaves[i].start()

  def stop_slave(self, i):
    assert 0 <= i < len(self._slaves)
    self._slaves[i].stop()

  def start_sentinel(self, i):
    assert 0 <= i < len(self._sentinels)
    self._sentinels[i].start()

  def stop_sentinel(self, i):
    assert 0 <= i < len(self._sentinels)
    self._sentinels[i].stop()
